package com.joxette.it;

import com.joxette.compaction.CompactionService;
import com.joxette.compaction.RunStatus;
import com.joxette.compaction.TriggerSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the actual point of {@code expireSnapshotsAndCleanup}: after compaction, the
 * physical file count in object storage goes down, not just the catalog's logical view
 * of it. Before this feature, {@code ducklake_merge_adjacent_files} alone left every
 * superseded small file behind forever (see {@code docs/superpowers/specs/...} and
 * {@code CompactionService}'s class javadoc).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class CompactionSnapshotCleanupIT {

    private static final String BUCKET = "joxette-compact-test";
    private static final String ENTITY_TYPE = "cleanuptest";

    @Container
    static final MinIOContainer minio =
            new MinIOContainer(DockerImageName.parse("minio/minio:RELEASE.2024-01-16T16-07-38Z"));

    @DynamicPropertySource
    static void minioProperties(DynamicPropertyRegistry registry) {
        String s3Url = minio.getS3URL();
        String userName = minio.getUserName();
        String password = minio.getPassword();
        try (S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create(s3Url))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(userName, password)))
                .region(Region.US_EAST_1)
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build()) {
            s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        }
        registry.add("joxette.catalog.object-storage-path", () -> "s3://" + BUCKET + "/data/");
        registry.add("joxette.s3.endpoint",   () -> s3Url);
        registry.add("joxette.s3.access-key", () -> userName);
        registry.add("joxette.s3.secret-key", () -> password);
        // 0h retention so the snapshot(s) from this test's own writes are immediately
        // eligible for expiry — no need to wait out a real 24h default.
        registry.add("joxette.compaction.snapshot-retention-hours", () -> "0");
    }

    @Autowired private Connection duckDB;
    @Autowired private CompactionService compactionService;

    @Test
    void compaction_mergesFilesAndReclaimsDiskSpace() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            // Column list/types match SchemaManager.createEntityCassetteTable's real DDL
            // exactly (kafka_key is BLOB, not VARCHAR — easy to get wrong).
            st.execute("CREATE TABLE IF NOT EXISTS lake.main.entity_" + ENTITY_TYPE + " (" +
                    "recorded_at TIMESTAMPTZ, entity_id VARCHAR, bucket INTEGER, message_type VARCHAR, " +
                    "topic VARCHAR, kafka_offset BIGINT, kafka_partition INTEGER, kafka_timestamp TIMESTAMPTZ, " +
                    "kafka_key BLOB, kafka_value BLOB, metadata VARCHAR, " +
                    "headers STRUCT(key VARCHAR, value VARCHAR)[])");
            st.execute("INSERT INTO entity_type_configs (entity_type, bucket_count) VALUES ('"
                    + ENTITY_TYPE + "', 64) ON CONFLICT DO NOTHING");
        }

        // Four separate inserts, each flushed immediately, to force four distinct small
        // Parquet files — matching how real batches land as separate files over time.
        Instant ts = Instant.parse("2020-01-01T00:00:00Z");
        for (int i = 0; i < 4; i++) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "INSERT INTO lake.main.entity_" + ENTITY_TYPE +
                    " (recorded_at, entity_id, bucket, message_type, topic, kafka_offset, kafka_partition, " +
                    "  kafka_timestamp, kafka_key, kafka_value, metadata, headers) " +
                    "VALUES (?, ?, 0, 'order', 'orders.events', ?, 0, ?, ?, ?, NULL, [])")) {
                ps.setTimestamp(1, java.sql.Timestamp.from(ts));
                ps.setString(2, "ENT-" + i);
                ps.setLong(3, i); // kafka_offset
                ps.setTimestamp(4, java.sql.Timestamp.from(ts)); // kafka_timestamp
                ps.setBytes(5, ("k" + i).getBytes()); // kafka_key is BLOB, not VARCHAR
                ps.setBytes(6, ("v" + i).getBytes()); // kafka_value
                ps.executeUpdate();
            }
            try (Statement st = duckDB.createStatement()) {
                st.execute("CALL ducklake_flush_inlined_data('lake')");
            }
        }

        int filesBefore = countParquetFiles();
        assertThat(filesBefore).isGreaterThanOrEqualTo(4);

        var run = compactionService.beginRun(TriggerSource.MANUAL, java.util.List.of(ENTITY_TYPE));
        compactionService.executeRun(run.id(), java.util.List.of(ENTITY_TYPE));

        var completed = compactionService.getRunById(run.id());
        assertThat(completed.status()).isEqualTo(RunStatus.COMPLETED);

        int filesAfter = countParquetFiles();
        assertThat(filesAfter)
                .as("compaction should have merged files AND cleaned up the superseded originals")
                .isLessThan(filesBefore);
    }

    private int countParquetFiles() throws Exception {
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT COUNT(*) FROM glob('s3://" + BUCKET + "/data/main/entity_" + ENTITY_TYPE + "/**/*.parquet')")) {
            rs.next();
            return rs.getInt(1);
        }
    }
}
