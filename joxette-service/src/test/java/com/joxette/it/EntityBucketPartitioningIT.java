package com.joxette.it;

import com.joxette.compaction.CompactionService;
import com.joxette.compaction.RunStatus;
import com.joxette.compaction.TriggerSource;
import com.joxette.replay.EntityFileLocation;
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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the bucket-partitioning migration end-to-end: entities written
 * before partitioning was declared live in old flat files; after compaction
 * runs, those files are gone, every remaining file lives under a
 * {@code bucket=N/} path, all rows survived intact, and the file-count fast
 * path (built on top of the same migration signal) gives the same correct
 * per-entity count the slow scan-based path already proved correct for
 * (see {@code EntityFileCountIT}).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class EntityBucketPartitioningIT {

    private static final String BUCKET = "joxette-partition-test";
    private static final String ENTITY_TYPE = "partitiontest";

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
        // 0h retention so the snapshot created by this test's own migration is
        // immediately eligible for expiry -- otherwise expireSnapshotsAndCleanup()
        // (default 24h retention) would leave the old flat file physically present
        // on disk even though its rows are already soft-deleted, and the
        // "no flat files remain" assertion below would fail. Same pattern as
        // CompactionSnapshotCleanupIT.
        registry.add("joxette.compaction.snapshot-retention-hours", () -> "0");
    }

    @Autowired private Connection duckDB;
    @Autowired private CompactionService compactionService;
    @Autowired private EntityReplayService entityReplayService;

    @Test
    void compaction_migratesLegacyFilesIntoBucketLayout_andFastPathStaysCorrect() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS lake.main.entity_" + ENTITY_TYPE + " (" +
                    "recorded_at TIMESTAMPTZ, entity_id VARCHAR, bucket INTEGER, message_type VARCHAR, " +
                    "topic VARCHAR, kafka_offset BIGINT, kafka_partition INTEGER, kafka_timestamp TIMESTAMPTZ, " +
                    "kafka_key BLOB, kafka_value BLOB, metadata VARCHAR, " +
                    "headers STRUCT(key VARCHAR, value VARCHAR)[])");
            st.execute("INSERT INTO entity_type_configs (entity_type, bucket_count) VALUES ('"
                    + ENTITY_TYPE + "', 64) ON CONFLICT DO NOTHING");
        }

        // Write BEFORE partitioning is declared -- old flat files.
        insertAndFlush("A", 0);
        insertAndFlush("B", 1);

        int flatFilesBefore = countFilesOutsideBucketDirs();
        assertThat(flatFilesBefore).as("both pre-partition inserts landed as flat files").isEqualTo(2);

        // Declare partitioning (what SchemaManager does at every startup -- issued
        // directly here since the table itself was created after this test's Spring
        // context already booted, so SchemaManager's own startup pass never saw it).
        try (Statement st = duckDB.createStatement()) {
            st.execute("ALTER TABLE lake.main.entity_" + ENTITY_TYPE + " SET PARTITIONED BY (bucket)");
        }

        // Write AFTER partitioning -- new bucket=N files, same two entities.
        insertAndFlush("A", 2);
        insertAndFlush("B", 3);

        int bucketFilesBeforeCompaction = countFilesUnderBucketDirs();
        assertThat(bucketFilesBeforeCompaction)
                .as("post-partition inserts landed under bucket=N/ immediately, no migration needed for them")
                .isGreaterThanOrEqualTo(1);

        var run = compactionService.beginRun(TriggerSource.MANUAL, List.of(ENTITY_TYPE));
        compactionService.executeRun(run.id(), List.of(ENTITY_TYPE));

        var completed = compactionService.getRunById(run.id());
        assertThat(completed.status()).isEqualTo(RunStatus.COMPLETED);

        assertThat(countFilesOutsideBucketDirs())
                .as("old flat files were migrated away -- none should remain outside bucket=N/")
                .isZero();
        assertThat(countFilesUnderBucketDirs())
                .as("every remaining file lives under some bucket=N/ directory")
                .isGreaterThan(0);

        try (Statement st = duckDB.createStatement();
             var rs = st.executeQuery("SELECT COUNT(*) FROM lake.main.entity_" + ENTITY_TYPE)) {
            rs.next();
            assertThat(rs.getInt(1)).as("no rows lost or duplicated by the migration").isEqualTo(4);
        }

        EntityStats statsA = entityReplayService.getEntityStats(ENTITY_TYPE, "A");
        EntityStats statsB = entityReplayService.getEntityStats(ENTITY_TYPE, "B");
        EntityFileLocation locationA = entityReplayService.getEntityFileLocation(ENTITY_TYPE, "A");
        EntityFileLocation locationB = entityReplayService.getEntityFileLocation(ENTITY_TYPE, "B");
        assertThat(locationA.fileCount()).as("fast path gives a correct, nonzero count for A").isGreaterThan(0);
        assertThat(locationB.fileCount()).as("fast path gives a correct, nonzero count for B").isGreaterThan(0);
        assertThat(statsA.messageCount()).as("row data intact for A after migration").isEqualTo(2);
        assertThat(statsB.messageCount()).as("row data intact for B after migration").isEqualTo(2);
    }

    private void insertAndFlush(String entityId, long offset) throws Exception {
        Instant ts = Instant.parse("2020-01-01T00:00:00Z");
        // Must match MessageRouter.computeBucket exactly -- the fast-path glob looks
        // up files by this same hash, so a mismatched bucket column here (e.g. a
        // hardcoded 0, as sibling ITs use when bucket correctness doesn't matter to
        // them) would make the fast path search the wrong directory and find nothing.
        int bucket = computeBucket(ENTITY_TYPE, entityId, 64);
        try (PreparedStatement ps = duckDB.prepareStatement(
                "INSERT INTO lake.main.entity_" + ENTITY_TYPE +
                " (recorded_at, entity_id, bucket, message_type, topic, kafka_offset, kafka_partition, " +
                "  kafka_timestamp, kafka_key, kafka_value, metadata, headers) " +
                "VALUES (?, ?, ?, 'order', 'orders.events', ?, 0, ?, ?, ?, NULL, [])")) {
            ps.setObject(1, java.sql.Timestamp.from(ts));
            ps.setString(2, entityId);
            ps.setInt(3, bucket);
            ps.setLong(4, offset);
            ps.setObject(5, java.sql.Timestamp.from(ts));
            ps.setBytes(6, ("k" + offset).getBytes());
            ps.setBytes(7, ("v" + offset).getBytes());
            ps.executeUpdate();
        }
        try (Statement st = duckDB.createStatement()) {
            st.execute("CALL ducklake_flush_inlined_data('lake')");
        }
    }

    /** Mirrors MessageRouter.computeBucket (package-private, different package) exactly. */
    private static int computeBucket(String entityType, String entityId, int buckets) {
        int hash = 31 * entityType.hashCode() + entityId.hashCode();
        return Math.floorMod(hash, buckets);
    }

    private int countFilesOutsideBucketDirs() throws Exception {
        try (Statement st = duckDB.createStatement();
             var rs = st.executeQuery(
                     "SELECT COUNT(*) FROM glob('s3://" + BUCKET + "/data/main/entity_"
                     + ENTITY_TYPE + "/*.parquet')")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private int countFilesUnderBucketDirs() throws Exception {
        try (Statement st = duckDB.createStatement();
             var rs = st.executeQuery(
                     "SELECT COUNT(*) FROM glob('s3://" + BUCKET + "/data/main/entity_"
                     + ENTITY_TYPE + "/bucket=*/**/*.parquet')")) {
            rs.next();
            return rs.getInt(1);
        }
    }
}
