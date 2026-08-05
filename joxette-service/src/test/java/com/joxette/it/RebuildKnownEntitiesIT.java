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

    /**
     * Entity type used exclusively by
     * {@link #rebuildKnownEntities_recoverOrphanedFiles_doesNotResurrectDeletedRowsOrCrossContaminate()}
     * for its "zero ever-tracked files" side of the assertion. Registered on demand (via
     * {@link #registerEntityType(String)}, not in {@code @BeforeEach}) so no other test method
     * in this class can accidentally write data for it and invalidate the
     * execution-order-independence this exists to guarantee.
     */
    private static final String ZERO_FILES_ENTITY_TYPE = "invoice";

    /**
     * Entity type used exclusively by
     * {@link #rebuildKnownEntities_recoverOrphanedFiles_recoversDataAfterCatalogTableReset()}.
     * Must not be shared with any other test method: that test {@code DROP}s and recreates
     * its cassette table, which resets the DuckLake catalog's file tracking for the table's
     * <em>entire</em> object-storage directory — not just the rows this test itself wrote. If
     * another test method had already flushed data for this same entity type into the same
     * bucket, that data's Parquet files would also become "orphaned" by the drop and get
     * swept up by the recovery scan, inflating the recovered-row count and making the
     * assertion depend on execution order (the exact class of bug this file's tests exist to
     * catch elsewhere — see {@link #ZERO_FILES_ENTITY_TYPE}).
     */
    private static final String RECOVERY_TEST_ENTITY_TYPE = "shipment";

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
     * {@link #ZERO_FILES_ENTITY_TYPE} is left completely untouched (so the bucket has
     * zero files for it). It then calls rebuild with the opt-in
     * {@code recoverOrphanedFiles=true} flag — the most permissive setting — and asserts
     * that even then:
     * <ul>
     *   <li>the deleted "order" rows are NOT resurrected (the {@code ducklake_list_files}
     *       tracked-file gate trusts the deletion instead of scanning raw storage), and</li>
     *   <li>no "order" data leaks into {@link #ZERO_FILES_ENTITY_TYPE}'s rebuild (the
     *       fallback glob, when it does run, is scoped to that table's own on-disk path,
     *       never the whole bucket).</li>
     * </ul>
     * If either regression reappeared, this test would fail loudly: rows would resurface
     * in {@code known_entities} under one or both entity types.
     *
     * <p><b>Why a dedicated entity type instead of reusing "order" or "customer":</b> this
     * test's second half depends on one entity type having genuinely <em>zero ever-tracked
     * files</em> so that {@code resolveEntityDataSource}'s scoped-glob branch (as opposed to
     * the {@code tableHasTrackedFiles} short-circuit) actually executes. {@code @BeforeEach}
     * only {@code DELETE}s rows — it never drops or resets file-level history — so if another
     * test method in this class writes-and-flushes data for "customer" before JUnit happens
     * to run this test, "customer" would still carry tracked file history from that earlier
     * flush and the scoped-glob branch would never run, making the assertion below pass for
     * the wrong reason regardless of test execution order. {@link #ZERO_FILES_ENTITY_TYPE} is
     * registered and used only by this test method, so it is guaranteed to have zero rows and
     * zero tracked files no matter what order the other tests in this class run in.
     */
    @Test
    void rebuildKnownEntities_recoverOrphanedFiles_doesNotResurrectDeletedRowsOrCrossContaminate()
            throws Exception {
        registerEntityType(ZERO_FILES_ENTITY_TYPE);

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

        // ZERO_FILES_ENTITY_TYPE is written by no other test in this class — guaranteed zero
        // rows and zero ever-tracked files regardless of execution order (see class javadoc
        // on this test method).

        // Even with the most permissive recovery flag, neither type may resurrect/contaminate.
        ResponseEntity<Map> response = restTemplate.postForEntity(
                url("/cassettes/entities/rebuild-known-entities?recoverOrphanedFiles=true"),
                null, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(((Number) response.getBody().get("rebuilt")).longValue())
                .as("deleted 'order' rows must not be resurrected, and '" + ZERO_FILES_ENTITY_TYPE +
                    "' must not pick up 'order' data even under recoverOrphanedFiles=true")
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

    /**
     * Positive-path proof that {@code recoverOrphanedFiles=true} genuinely recovers real
     * orphaned data, not just "correctly does nothing" (the only behaviour the other tests
     * in this class exercise).
     *
     * <p>Simulates a lost/reset {@code .ducklake} catalog file by writing and flushing real
     * {@link #RECOVERY_TEST_ENTITY_TYPE} data to Parquet in MinIO, then dropping and
     * recreating its cassette table. DuckLake's {@code DROP TABLE} only removes the catalog
     * entry — it does not delete the already-flushed Parquet file(s) from object storage (see
     * {@code SchemaManager.probeVariant()}'s vacuum comment for the same documented
     * behaviour) — and recreating a table with the same name resolves to the same default
     * on-disk path ({@code {data_path}/main/{table_name}/...}), so the pre-drop Parquet file
     * becomes exactly the kind of table-scoped "orphaned file" the recovery fallback is built
     * to find. This was verified directly against the DuckDB/DuckLake CLI before relying on it
     * here: {@code DROP TABLE} leaves the file on disk under the same
     * {@code main/entity_<type>/} directory, and the recreated table reports zero rows and
     * zero {@code ducklake_list_files} entries — the exact precondition
     * {@code resolveEntityDataSource} requires before it will attempt the fallback scan.
     *
     * <p>Uses a dedicated entity type not written by any other test method — see
     * {@link #RECOVERY_TEST_ENTITY_TYPE}'s javadoc for why: the {@code DROP}/recreate here
     * resets catalog file-tracking for that type's <em>entire</em> object-storage directory,
     * so reusing a type another test also writes to would sweep up that other data too and
     * make the recovered-row count depend on test execution order.
     *
     * <p>Asserts both directions: without {@code recoverOrphanedFiles} the orphaned data
     * stays lost, and with it the data is genuinely recovered — both into {@code known_entities}
     * and back into the catalog table itself (re-population), matching
     * {@code resolveEntityDataSource}'s documented behaviour.
     */
    @Test
    void rebuildKnownEntities_recoverOrphanedFiles_recoversDataAfterCatalogTableReset()
            throws Exception {
        registerEntityType(RECOVERY_TEST_ENTITY_TYPE);

        try (EntityCassetteBatchWriter writer = new EntityCassetteBatchWriter(duckDB)) {
            writer.writeRoutes(
                    List.of(new EntityRoute(RECOVERY_TEST_ENTITY_TYPE, "shipment-999", 4, "created", "test-topic")),
                    message("shipments.events", 0, 11L, Instant.parse("2024-08-01T09:00:00Z")));
        }
        try (Statement st = duckDB.createStatement()) {
            try {
                st.execute("CALL ducklake_flush_inlined_data('lake')");
            } catch (Exception ignored) {
                // Non-fatal — see other tests in this class for the same pattern.
            }
            st.execute("CHECKPOINT");
        }
        assertThat(countCassetteRows(RECOVERY_TEST_ENTITY_TYPE, "shipment-999")).isEqualTo(1L);

        // Simulate catalog loss: DROP TABLE removes only the catalog entry for this table.
        // The Parquet file already flushed to MinIO survives on object storage. Recreating
        // the table (as SchemaManager does at startup against a reset catalog) resolves to
        // the same default DuckLake path, making the surviving file an orphan of the
        // freshly-recreated, catalog-amnesiac table.
        try (Statement st = duckDB.createStatement()) {
            st.execute("DROP TABLE lake.main.entity_" + RECOVERY_TEST_ENTITY_TYPE);
        }
        DuckDBTestSupport.createEntityTable(duckDB, RECOVERY_TEST_ENTITY_TYPE);
        assertThat(countCassetteRows(RECOVERY_TEST_ENTITY_TYPE, "shipment-999")).isEqualTo(0L);

        // Without recoverOrphanedFiles, the orphaned data must NOT be recovered.
        ResponseEntity<Map> noRecover = restTemplate.postForEntity(
                url("/cassettes/entities/rebuild-known-entities"), null, Map.class);
        assertThat(noRecover.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(((Number) noRecover.getBody().get("rebuilt")).longValue())
                .as("orphaned data must stay lost when recoverOrphanedFiles is not requested")
                .isEqualTo(0L);
        try (PreparedStatement ps = duckDB.prepareStatement(
                "SELECT COUNT(*) FROM known_entities WHERE entity_type = ? AND entity_id = 'shipment-999'")) {
            ps.setString(1, RECOVERY_TEST_ENTITY_TYPE);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(0L);
            }
        }

        // With recoverOrphanedFiles=true, the orphaned Parquet file must be found and the
        // entity's data genuinely recovered — both into known_entities and back into the
        // catalog table itself.
        ResponseEntity<Map> recover = restTemplate.postForEntity(
                url("/cassettes/entities/rebuild-known-entities?recoverOrphanedFiles=true"),
                null, Map.class);
        assertThat(recover.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(((Number) recover.getBody().get("rebuilt")).longValue())
                .as("recoverOrphanedFiles=true must genuinely recover the orphaned 'shipment-999' row")
                .isEqualTo(1L);
        assertEntityExists(RECOVERY_TEST_ENTITY_TYPE, "shipment-999");
        assertFirstSeenEqualsLastSeen(RECOVERY_TEST_ENTITY_TYPE, "shipment-999");
        // The catalog table itself should have been re-populated from the orphaned file.
        assertThat(countCassetteRows(RECOVERY_TEST_ENTITY_TYPE, "shipment-999")).isEqualTo(1L);
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

    /**
     * Registers {@code entityType} in {@code entity_type_configs} and creates its DuckLake
     * cassette table, on demand from within the one test that needs it — used for entity
     * types (see {@link #ZERO_FILES_ENTITY_TYPE}, {@link #RECOVERY_TEST_ENTITY_TYPE}) that must
     * not be shared with any other test method in this class.
     */
    private void registerEntityType(String entityType) throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("""
                    INSERT INTO entity_type_configs (entity_type, bucket_count, created_at)
                    VALUES ('%s', 256, now())
                    ON CONFLICT (entity_type) DO NOTHING
                    """.formatted(entityType));
        }
        DuckDBTestSupport.createEntityTable(duckDB, entityType);
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
