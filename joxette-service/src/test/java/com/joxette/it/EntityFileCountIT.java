package com.joxette.it;

import com.joxette.replay.EntityReplayService;
import com.joxette.replay.EntityStats;
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
import java.sql.Statement;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the two-stage catalog-prune-then-verify file count is numerically
 * exact: entity A's count must include only files that actually contain A's
 * rows (an A-only file and a mixed A+B file), never the B-only file — a test
 * that only checked "count > 0" would pass even if the query counted every
 * file in the table.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class EntityFileCountIT {

    private static final String BUCKET = "joxette-filecount-test";
    private static final String ENTITY_TYPE = "filecounttest";

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
        registry.add("joxette.storage-console.url-template",
                () -> "http://localhost:9001/rustfs/console/browser/?bucket={bucket}&key={prefix}");
    }

    @Autowired private Connection duckDB;
    @Autowired private EntityReplayService entityReplayService;

    @Test
    void getEntityStats_fileCount_countsOnlyFilesContainingThatEntity() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS lake.main.entity_" + ENTITY_TYPE + " (" +
                    "recorded_at TIMESTAMPTZ, entity_id VARCHAR, bucket INTEGER, message_type VARCHAR, " +
                    "topic VARCHAR, kafka_offset BIGINT, kafka_partition INTEGER, kafka_timestamp TIMESTAMPTZ, " +
                    "kafka_key BLOB, kafka_value BLOB, metadata VARCHAR, " +
                    "headers STRUCT(key VARCHAR, value VARCHAR)[])");
        }

        insertAndFlush("A", 0);              // file 1: A-only
        insertAndFlush("B", 1);              // file 2: B-only
        insertBothAndFlush(2, 3);            // file 3: mixed A+B

        EntityStats statsA = entityReplayService.getEntityStats(ENTITY_TYPE, "A");
        EntityStats statsB = entityReplayService.getEntityStats(ENTITY_TYPE, "B");

        assertThat(statsA.fileCount())
                .as("entity A appears in the A-only file and the mixed file, not the B-only file")
                .isEqualTo(2);
        assertThat(statsB.fileCount())
                .as("entity B appears in the B-only file and the mixed file, not the A-only file")
                .isEqualTo(2);

        assertThat(statsA.objectStoreDirectory())
                .isEqualTo("s3://" + BUCKET + "/data/main/entity_" + ENTITY_TYPE + "/");
        assertThat(statsA.storageConsoleUrl())
                .isEqualTo("http://localhost:9001/rustfs/console/browser/?bucket=" + BUCKET
                        + "&key=data%2Fmain%2Fentity_" + ENTITY_TYPE + "%2F");
    }

    private void insertAndFlush(String entityId, long offset) throws Exception {
        Instant ts = Instant.parse("2020-01-01T00:00:00Z");
        try (PreparedStatement ps = duckDB.prepareStatement(
                "INSERT INTO lake.main.entity_" + ENTITY_TYPE +
                " (recorded_at, entity_id, bucket, message_type, topic, kafka_offset, kafka_partition, " +
                "  kafka_timestamp, kafka_key, kafka_value, metadata, headers) " +
                "VALUES (?, ?, 0, 'order', 'orders.events', ?, 0, ?, ?, ?, NULL, [])")) {
            ps.setObject(1, java.sql.Timestamp.from(ts));
            ps.setString(2, entityId);
            ps.setLong(3, offset);
            ps.setObject(4, java.sql.Timestamp.from(ts));
            ps.setBytes(5, ("k" + offset).getBytes());
            ps.setBytes(6, ("v" + offset).getBytes());
            ps.executeUpdate();
        }
        try (Statement st = duckDB.createStatement()) {
            st.execute("CALL ducklake_flush_inlined_data('lake')");
        }
    }

    private void insertBothAndFlush(long offsetA, long offsetB) throws Exception {
        Instant ts = Instant.parse("2020-01-01T00:00:00Z");
        try (PreparedStatement ps = duckDB.prepareStatement(
                "INSERT INTO lake.main.entity_" + ENTITY_TYPE +
                " (recorded_at, entity_id, bucket, message_type, topic, kafka_offset, kafka_partition, " +
                "  kafka_timestamp, kafka_key, kafka_value, metadata, headers) " +
                "VALUES (?, ?, 0, 'order', 'orders.events', ?, 0, ?, ?, ?, NULL, [])")) {
            ps.setObject(1, java.sql.Timestamp.from(ts));
            ps.setString(2, "A");
            ps.setLong(3, offsetA);
            ps.setObject(4, java.sql.Timestamp.from(ts));
            ps.setBytes(5, ("k" + offsetA).getBytes());
            ps.setBytes(6, ("v" + offsetA).getBytes());
            ps.executeUpdate();

            ps.setObject(1, java.sql.Timestamp.from(ts));
            ps.setString(2, "B");
            ps.setLong(3, offsetB);
            ps.setObject(4, java.sql.Timestamp.from(ts));
            ps.setBytes(5, ("k" + offsetB).getBytes());
            ps.setBytes(6, ("v" + offsetB).getBytes());
            ps.executeUpdate();
        }
        try (Statement st = duckDB.createStatement()) {
            st.execute("CALL ducklake_flush_inlined_data('lake')");
        }
    }
}
