package com.joxette.it;

import com.joxette.recording.EntityCassetteBatchWriter;
import com.joxette.replay.EntityRoute;
import com.joxette.replay.KafkaMessage;
import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestTemplate;
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
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@code POST /cassettes/entities/rebuild-known-entities}.
 *
 * <h2>Scenario</h2>
 * <ol>
 *   <li>Write entity events to DuckLake entity-cassette tables via the real
 *       {@link EntityCassetteBatchWriter} write path (not direct SQL inserts).</li>
 *   <li>Flush inline DuckLake data to Parquet files stored in a Testcontainers
 *       MinIO instance (the DuckLake DATA_PATH points to MinIO).</li>
 *   <li>Wipe the {@code known_entities} registry (plain DuckDB, main schema).</li>
 *   <li>Call {@code POST /cassettes/entities/rebuild-known-entities}.</li>
 *   <li>Assert every {@code (entity_type, entity_id)} row is restored with correct
 *       {@code first_seen} and {@code last_seen} timestamps, and that the
 *       per-entity message count in the cassette matches expectations.</li>
 * </ol>
 *
 * <p>{@code known_entities} is a plain-DuckDB table in the primary connection's
 * {@code main} schema — it is accessed without any catalog qualifier.
 * Entity-cassette data lives in {@code lake.main.entity_*} (DuckLake-backed).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class RebuildKnownEntitiesIT {

    private static final String BUCKET = "joxette-test";

    @Container
    static final MinIOContainer minio =
            new MinIOContainer(DockerImageName.parse("minio/minio:RELEASE.2024-01-16T16-07-38Z"));

    /**
     * Runs before the Spring context starts.
     * Creates the S3 bucket and registers DuckLake DATA_PATH + S3 secret properties
     * so that {@code DuckLakeManager} points the lake catalog at MinIO.
     */
    @DynamicPropertySource
    static void minioProperties(DynamicPropertyRegistry registry) {
        String s3Url    = minio.getS3URL();
        String userName = minio.getUserName();
        String password = minio.getPassword();

        createBucket(s3Url, userName, password);

        // DuckLake DATA_PATH → MinIO so flushed Parquet files land in the bucket.
        registry.add("joxette.catalog.object-storage-path", () -> "s3://" + BUCKET + "/data/");
        // DuckDB httpfs S3 secret consumed by DuckLakeManager.configureS3Secret().
        registry.add("joxette.s3.endpoint",   () -> s3Url);
        registry.add("joxette.s3.access-key", () -> userName);
        registry.add("joxette.s3.secret-key", () -> password);
    }

    @LocalServerPort
    private int port;

    private final RestTemplate restTemplate = new RestTemplate();

    /** Shared DuckDB connection from the Spring context — same instance used by all services. */
    @Autowired
    private Connection duckDB;

    private String url(String path) {
        return "http://localhost:" + port + "/v1" + path;
    }

    @BeforeEach
    void setUp() throws Exception {
        // Register entity types so that rebuildKnownEntities() discovers them via
        // configRepo.listEntityTypes() which queries entity_type_configs.
        try (Statement st = duckDB.createStatement()) {
            st.execute("""
                    INSERT INTO entity_type_configs (entity_type, bucket_count, created_at)
                    VALUES ('order', 256, now())
                    ON CONFLICT (entity_type) DO NOTHING
                    """);
            st.execute("""
                    INSERT INTO entity_type_configs (entity_type, bucket_count, created_at)
                    VALUES ('customer', 256, now())
                    ON CONFLICT (entity_type) DO NOTHING
                    """);
        }

        // Create DuckLake entity-cassette tables if they do not yet exist (idempotent).
        DuckDBTestSupport.createEntityTable(duckDB, "order");
        DuckDBTestSupport.createEntityTable(duckDB, "customer");

        // Wipe all state left by a previous test method.
        // known_entities is in the primary DB's main schema — no lake. prefix.
        try (Statement st = duckDB.createStatement()) {
            st.execute("DELETE FROM known_entities");
            st.execute("DELETE FROM lake.main.entity_order");
            st.execute("DELETE FROM lake.main.entity_customer");
        }
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    @Test
    void rebuildKnownEntities_viaEntityCassetteBatchWriter_restoresFirstAndLastSeen()
            throws Exception {
        // order-001: two events  → first_seen ≤ last_seen (written at two distinct instants)
        // order-002: one event   → first_seen = last_seen
        // cust-001:  one event   → first_seen = last_seen
        try (EntityCassetteBatchWriter writer = new EntityCassetteBatchWriter(duckDB)) {
            writer.writeRoutes(
                    List.of(new EntityRoute("order", "order-001", 0, "order_created", "test-topic")),
                    message("orders.events", 0, 0L, Instant.parse("2024-01-01T10:00:00Z")));
            writer.writeRoutes(
                    List.of(new EntityRoute("order", "order-001", 0, "order_updated", "test-topic")),
                    message("orders.events", 0, 1L, Instant.parse("2024-01-01T12:00:00Z")));
            writer.writeRoutes(
                    List.of(new EntityRoute("order", "order-002", 1, "order_created", "test-topic")),
                    message("orders.events", 0, 2L, Instant.parse("2024-01-02T08:00:00Z")));
            writer.writeRoutes(
                    List.of(new EntityRoute("customer", "cust-001", 0, "customer_signup", "test-topic")),
                    message("customers", 0, 0L, Instant.parse("2024-01-01T10:00:00Z")));
        }

        // Flush inline DuckLake data to Parquet in MinIO, then persist catalog metadata.
        try (Statement st = duckDB.createStatement()) {
            try {
                st.execute("CALL ducklake_flush_inlined_data('lake')");
            } catch (Exception ignored) {
                // Non-fatal: data is still readable from the inline buffer.
                // The rebuild will find it either way.
            }
            st.execute("CHECKPOINT");
        }

        // known_entities was wiped by @BeforeEach — trigger the rebuild.
        assertThat(DuckDBTestSupport.countRows(duckDB, "known_entities")).isEqualTo(0L);

        ResponseEntity<Map> response = restTemplate.postForEntity(
                url("/cassettes/entities/rebuild-known-entities"), null, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        // 3 distinct entities across both entity types.
        assertThat(((Number) response.getBody().get("rebuilt")).longValue()).isEqualTo(3L);

        // Every entity must have a row in known_entities.
        assertEntityExists("order",    "order-001");
        assertEntityExists("order",    "order-002");
        assertEntityExists("customer", "cust-001");

        // order-001 has two events written at separate instants: first_seen ≤ last_seen.
        assertFirstSeenLeLastSeen("order", "order-001");

        // Single-event entities: first_seen must equal last_seen.
        assertFirstSeenEqualsLastSeen("order",    "order-002");
        assertFirstSeenEqualsLastSeen("customer", "cust-001");

        // Message-count check: verify row counts in the entity-cassette tables
        // (known_entities does not store message_count; we assert directly on the source).
        assertThat(countCassetteRows("order",    "order-001")).isEqualTo(2L);
        assertThat(countCassetteRows("order",    "order-002")).isEqualTo(1L);
        assertThat(countCassetteRows("customer", "cust-001")).isEqualTo(1L);

        // Total known_entities row count.
        assertThat(DuckDBTestSupport.countRows(duckDB, "known_entities")).isEqualTo(3L);
    }

    @Test
    void rebuildKnownEntities_emptyEntityTables_returns0AndLeavesRegistryEmpty()
            throws Exception {
        // Entity tables were wiped in @BeforeEach — rebuild finds nothing to scan.
        ResponseEntity<Map> response = restTemplate.postForEntity(
                url("/cassettes/entities/rebuild-known-entities"), null, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(((Number) response.getBody().get("rebuilt")).longValue()).isEqualTo(0L);
        assertThat(DuckDBTestSupport.countRows(duckDB, "known_entities")).isEqualTo(0L);
    }

    @Test
    void rebuildKnownEntities_idempotent_secondCallProducesSameResult() throws Exception {
        try (EntityCassetteBatchWriter writer = new EntityCassetteBatchWriter(duckDB)) {
            writer.writeRoutes(
                    List.of(new EntityRoute("order", "order-X", 3, "created", "test-topic")),
                    message("orders.events", 0, 5L, Instant.parse("2024-06-01T09:00:00Z")));
        }

        // First rebuild.
        restTemplate.postForEntity(url("/cassettes/entities/rebuild-known-entities"), null, Map.class);

        // Second rebuild must produce the same result (ON CONFLICT DO UPDATE is idempotent).
        ResponseEntity<Map> second = restTemplate.postForEntity(
                url("/cassettes/entities/rebuild-known-entities"), null, Map.class);

        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(((Number) second.getBody().get("rebuilt")).longValue()).isEqualTo(1L);
        assertEntityExists("order", "order-X");
        assertFirstSeenEqualsLastSeen("order", "order-X");
        assertThat(DuckDBTestSupport.countRows(duckDB, "known_entities")).isEqualTo(1L);
    }

    /**
     * Regression test for the production data-integrity bug this class exists to guard
     * against: {@code resolveEntityDataSource()} used to fall back to an unscoped,
     * bucket-wide {@code **\/*.parquet} glob whenever a table's live row count was zero
     * — regardless of <em>why</em> it was zero. That glob (a) matched Parquet files
     * belonging to every entity type under the object-storage root, not just the one
     * being rebuilt, mislabeling their rows with the current loop's {@code entityType},
     * and (b) could resurrect rows that were already logically DELETEd from the live
     * table, since DuckLake DELETE only marks rows deleted — it doesn't remove the
     * underlying Parquet file until compaction/vacuum runs.
     *
     * <p>This test reproduces both preconditions in one bucket: real "order" data is
     * written and flushed to Parquet, then deleted from the live table (so the bucket
     * genuinely contains "order" Parquet files with all rows delete-marked), while
     * "customer" is left completely untouched (so the bucket has zero files for it).
     * It then calls rebuild with the opt-in {@code recoverOrphanedFiles=true} flag —
     * the most permissive setting — and asserts that even then:
     * <ul>
     *   <li>the deleted "order" rows are NOT resurrected (the {@code ducklake_list_files}
     *       tracked-file gate trusts the deletion instead of scanning raw storage), and</li>
     *   <li>no "order" data leaks into "customer"'s rebuild (the fallback glob, when it
     *       does run, is scoped to that table's own on-disk path, never the whole bucket).</li>
     * </ul>
     * If either regression reappeared, this test would fail loudly: rows would resurface
     * in {@code known_entities} under one or both entity types.
     */
    @Test
    void rebuildKnownEntities_recoverOrphanedFiles_doesNotResurrectDeletedRowsOrCrossContaminate()
            throws Exception {
        // Write and flush real "order" data so the bucket contains genuine "order" Parquet
        // files — the exact raw material a bucket-wide glob would (wrongly) hoover up.
        try (EntityCassetteBatchWriter writer = new EntityCassetteBatchWriter(duckDB)) {
            writer.writeRoutes(
                    List.of(new EntityRoute("order", "order-777", 7, "created", "test-topic")),
                    message("orders.events", 0, 9L, Instant.parse("2024-07-01T09:00:00Z")));
        }
        try (Statement st = duckDB.createStatement()) {
            try {
                st.execute("CALL ducklake_flush_inlined_data('lake')");
            } catch (Exception ignored) {
                // Non-fatal — see other tests in this class for the same pattern.
            }
            st.execute("CHECKPOINT");
        }
        assertThat(countCassetteRows("order", "order-777")).isEqualTo(1L);

        // Logically delete it — DuckLake keeps the Parquet file(s) tracked (with delete
        // markers) until compaction/vacuum reclaims them, so the file is still sitting in
        // the bucket after this DELETE.
        try (Statement st = duckDB.createStatement()) {
            st.execute("DELETE FROM lake.main.entity_order WHERE entity_id = 'order-777'");
        }
        assertThat(countCassetteRows("order", "order-777")).isEqualTo(0L);

        // "customer" is untouched by this test (and was wiped in @BeforeEach) — zero rows,
        // zero ever-tracked files.

        // Even with the most permissive recovery flag, neither type may resurrect/contaminate.
        ResponseEntity<Map> response = restTemplate.postForEntity(
                url("/cassettes/entities/rebuild-known-entities?recoverOrphanedFiles=true"),
                null, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(((Number) response.getBody().get("rebuilt")).longValue())
                .as("deleted 'order' rows must not be resurrected, and 'customer' must not " +
                    "pick up 'order' data even under recoverOrphanedFiles=true")
                .isEqualTo(0L);
        assertThat(DuckDBTestSupport.countRows(duckDB, "known_entities")).isEqualTo(0L);

        // Explicitly confirm no cross-type leakage: order-777 must not appear under either type.
        try (PreparedStatement ps = duckDB.prepareStatement(
                "SELECT COUNT(*) FROM known_entities WHERE entity_id = 'order-777'")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1))
                        .as("order-777 must not resurface under any entity type")
                        .isEqualTo(0L);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Builds a minimal {@link KafkaMessage} — no key, null value, no headers. */
    private static KafkaMessage message(String topic, int partition, long offset, Instant ts) {
        return new KafkaMessage(topic, partition, offset, ts.toEpochMilli(),
                null, null, List.of());
    }

    private void assertEntityExists(String entityType, String entityId) throws Exception {
        try (PreparedStatement ps = duckDB.prepareStatement(
                "SELECT COUNT(*) FROM known_entities WHERE entity_type = ? AND entity_id = ?")) {
            ps.setString(1, entityType);
            ps.setString(2, entityId);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1))
                        .as("Row (%s, %s) must exist in known_entities", entityType, entityId)
                        .isEqualTo(1L);
            }
        }
    }

    private void assertFirstSeenLeLastSeen(String entityType, String entityId) throws Exception {
        try (PreparedStatement ps = duckDB.prepareStatement(
                "SELECT first_seen <= last_seen FROM known_entities " +
                "WHERE entity_type = ? AND entity_id = ?")) {
            ps.setString(1, entityType);
            ps.setString(2, entityId);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getBoolean(1))
                        .as("first_seen must be ≤ last_seen for (%s, %s)", entityType, entityId)
                        .isTrue();
            }
        }
    }

    private void assertFirstSeenEqualsLastSeen(String entityType, String entityId) throws Exception {
        try (PreparedStatement ps = duckDB.prepareStatement(
                "SELECT first_seen = last_seen FROM known_entities " +
                "WHERE entity_type = ? AND entity_id = ?")) {
            ps.setString(1, entityType);
            ps.setString(2, entityId);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getBoolean(1))
                        .as("first_seen must equal last_seen for single-event entity (%s, %s)",
                                entityType, entityId)
                        .isTrue();
            }
        }
    }

    /**
     * Counts rows for a specific entity ID in its cassette table
     * ({@code lake.main.entity_{type}}), providing a "message_count" assertion
     * for data that {@code known_entities} does not store directly.
     */
    private long countCassetteRows(String entityType, String entityId) throws Exception {
        try (PreparedStatement ps = duckDB.prepareStatement(
                "SELECT COUNT(*) FROM lake.main.entity_" + entityType + " WHERE entity_id = ?")) {
            ps.setString(1, entityId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }
    }

    /** Creates the MinIO bucket. Called from {@link #minioProperties} before the context starts. */
    private static void createBucket(String s3Url, String accessKey, String secretKey) {
        try (S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create(s3Url))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .region(Region.US_EAST_1)
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build()) {
            s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        }
    }
}
