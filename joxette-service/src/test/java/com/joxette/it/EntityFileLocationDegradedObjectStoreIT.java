package com.joxette.it;

import com.joxette.replay.EntityFileLocation;
import com.joxette.replay.EntityReplayService;
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
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves {@code getEntityFileLocation} degrades gracefully instead of failing
 * outright when some — but not all — of an entity's physical Parquet files are
 * unreachable: it must count what it *can* read and report the rest as
 * {@code filesUnavailable} rather than throwing {@code UpstreamUnavailableException}
 * for the whole request. Mirrors the real production failure mode from
 * {@code TopicReplayServiceObjectStoreRetryTest} ("IO Error: Server returned
 * nothing") but at the file-count layer, where per-file granularity makes a
 * partial result possible.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class EntityFileLocationDegradedObjectStoreIT {

    private static final String BUCKET = "joxette-degraded-filecount-test";
    private static final String ENTITY_TYPE = "degradedfilecounttest";

    @Container
    static final MinIOContainer minio =
            new MinIOContainer(DockerImageName.parse("minio/minio:RELEASE.2024-01-16T16-07-38Z"));

    private static String s3Url;
    private static String userName;
    private static String password;

    @DynamicPropertySource
    static void minioProperties(DynamicPropertyRegistry registry) {
        s3Url = minio.getS3URL();
        userName = minio.getUserName();
        password = minio.getPassword();
        try (S3Client s3 = s3Client()) {
            s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        }
        registry.add("joxette.catalog.object-storage-path", () -> "s3://" + BUCKET + "/data/");
        registry.add("joxette.s3.endpoint",   () -> s3Url);
        registry.add("joxette.s3.access-key", () -> userName);
        registry.add("joxette.s3.secret-key", () -> password);
    }

    private static S3Client s3Client() {
        return S3Client.builder()
                .endpointOverride(URI.create(s3Url))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(userName, password)))
                .region(Region.US_EAST_1)
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();
    }

    @Autowired private Connection duckDB;
    @Autowired private EntityReplayService entityReplayService;

    @Test
    void getEntityFileLocation_countsReachableFiles_andFlagsUnreachableOnesInsteadOfFailing() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS lake.main.entity_" + ENTITY_TYPE + " (" +
                    "recorded_at TIMESTAMPTZ, entity_id VARCHAR, bucket INTEGER, message_type VARCHAR, " +
                    "topic VARCHAR, kafka_offset BIGINT, kafka_partition INTEGER, kafka_timestamp TIMESTAMPTZ, " +
                    "kafka_key BLOB, kafka_value BLOB, metadata VARCHAR, " +
                    "headers STRUCT(key VARCHAR, value VARCHAR)[])");
        }

        insertAndFlush("A", 0); // file 1
        insertAndFlush("A", 1); // file 2 — two separate flushes -> two separate physical files

        List<String> files = listFiles();
        assertThat(files).as("two separate flushes should produce two physical files").hasSize(2);

        // Delete exactly one of the two underlying objects directly from S3 — the
        // other stays intact. DuckLake's catalog still believes both exist (we
        // didn't tell it otherwise); only the physical read of the deleted one fails.
        deleteFromS3(files.get(0));

        EntityFileLocation location = entityReplayService.getEntityFileLocation(ENTITY_TYPE, "A");

        assertThat(location.fileCount())
                .as("only the still-reachable file should be counted")
                .isEqualTo(1);
        assertThat(location.filesUnavailable())
                .as("the deleted file should be reported as unavailable, not silently dropped or fatal")
                .isEqualTo(1);
    }

    private List<String> listFiles() throws Exception {
        List<String> files = new ArrayList<>();
        try (PreparedStatement ps = duckDB.prepareStatement(
                "SELECT data_file FROM ducklake_list_files('lake', ?)")) {
            ps.setString(1, "entity_" + ENTITY_TYPE);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) files.add(rs.getString(1));
            }
        }
        return files;
    }

    private void deleteFromS3(String fullPath) {
        // fullPath looks like s3://bucket/data/main/entity_x/bucket=0/file.parquet
        String withoutScheme = fullPath.substring("s3://".length());
        int firstSlash = withoutScheme.indexOf('/');
        String key = withoutScheme.substring(firstSlash + 1);
        try (S3Client s3 = s3Client()) {
            s3.deleteObject(DeleteObjectRequest.builder().bucket(BUCKET).key(key).build());
        }
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
}
