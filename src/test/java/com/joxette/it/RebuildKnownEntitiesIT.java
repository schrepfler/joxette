package com.joxette.it;

import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@code POST /cassettes/entities/rebuild-known-entities}.
 *
 * <h2>Scenario</h2>
 * <ol>
 *   <li>Write entity events directly into per-type cassette tables backed by a
 *       DuckLake catalog whose DATA_PATH points to a Testcontainers MinIO instance.</li>
 *   <li>Force a CHECKPOINT so inline data is flushed to Parquet files in MinIO.</li>
 *   <li>Wipe {@code lake.known_entities}.</li>
 *   <li>Call {@code POST /cassettes/entities/rebuild-known-entities}.</li>
 *   <li>Assert every {@code (entity_type, entity_id)} row is restored with the
 *       correct {@code first_seen} and {@code last_seen} timestamps.</li>
 * </ol>
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
     * Runs before the Spring context starts:
     * <ol>
     *   <li>Creates the S3 bucket in MinIO.</li>
     *   <li>Registers DuckLake + S3 configuration properties so that
     *       {@code DuckLakeManager} attaches the lake catalog with
     *       {@code DATA_PATH 's3://joxette-test/data/'}.</li>
     * </ol>
     */
    @DynamicPropertySource
    static void minioProperties(DynamicPropertyRegistry registry) {
        String s3Url    = minio.getS3URL();
        String userName = minio.getUserName();
        String password = minio.getPassword();

        createBucket(s3Url, userName, password);

        registry.add("joxette.catalog.object-storage-path", () -> "s3://" + BUCKET + "/data/");
        // joxette.s3.* maps to JoxetteProperties.S3, consumed by
        // DuckLakeManager.configureS3Secret() (CREATE SECRET joxette_s3).
        // USE_SSL and URL_STYLE are hardcoded to false / path in configureS3Secret().
        registry.add("joxette.s3.endpoint",   () -> s3Url);
        registry.add("joxette.s3.access-key", () -> userName);
        registry.add("joxette.s3.secret-key", () -> password);
    }

    @Autowired private TestRestTemplate restTemplate;

    /** Shared DuckDB connection from the Spring context — same instance used by all services. */
    @Autowired private Connection duckDB;

    @BeforeEach
    void setUp() throws Exception {
        // Ensure entity tables exist (IF NOT EXISTS makes this idempotent).
        DuckDBTestSupport.createEntityTable(duckDB, "order");
        DuckDBTestSupport.createEntityTable(duckDB, "customer");

        // Wipe state left from a previous test method.
        try (Statement st = duckDB.createStatement()) {
            st.execute("DELETE FROM lake.known_entities");
            st.execute("DELETE FROM lake.entity_order");
            st.execute("DELETE FROM lake.entity_customer");
        }
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    @Test
    void rebuildKnownEntities_restoresAllEntitiesWithCorrectTimestamps() throws Exception {
        // order-001: two events → first_seen = t1, last_seen = t2
        Instant t1 = Instant.parse("2024-01-01T10:00:00Z");
        Instant t2 = Instant.parse("2024-01-01T12:00:00Z");
        // order-002: single event → first_seen = last_seen = t3
        Instant t3 = Instant.parse("2024-01-02T08:00:00Z");
        // cust-001: single event → first_seen = last_seen = t1
        DuckDBTestSupport.insertEntityRow(duckDB, "order",    "order-001", 0,
                "orders", 0, 0L, t1, Instant.now(), "order-001", null);
        DuckDBTestSupport.insertEntityRow(duckDB, "order",    "order-001", 0,
                "orders", 0, 1L, t2, Instant.now(), "order-001", null);
        DuckDBTestSupport.insertEntityRow(duckDB, "order",    "order-002", 1,
                "orders", 0, 2L, t3, Instant.now(), "order-002", null);
        DuckDBTestSupport.insertEntityRow(duckDB, "customer", "cust-001",  0,
                "customers", 0, 0L, t1, Instant.now(), "cust-001", null);

        // Flush inline data to Parquet in MinIO before wiping and rebuilding.
        try (Statement st = duckDB.createStatement()) {
            st.execute("CHECKPOINT");
        }

        // known_entities was already wiped by @BeforeEach — rebuild from cassettes.
        ResponseEntity<Map> response = restTemplate.postForEntity(
                "/cassettes/entities/rebuild-known-entities", null, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(((Number) response.getBody().get("rebuilt")).longValue()).isEqualTo(3L);

        assertKnownEntity("order",    "order-001", 0, t1, t2);
        assertKnownEntity("order",    "order-002", 1, t3, t3);
        assertKnownEntity("customer", "cust-001",  0, t1, t1);

        // Verify total row count.
        assertThat(DuckDBTestSupport.countRows(duckDB, "lake.known_entities")).isEqualTo(3L);
    }

    @Test
    void rebuildKnownEntities_emptyEntityTables_returns0AndLeavesRegistryEmpty() throws Exception {
        // Entity tables exist but contain no rows (wiped in @BeforeEach).
        ResponseEntity<Map> response = restTemplate.postForEntity(
                "/cassettes/entities/rebuild-known-entities", null, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(((Number) response.getBody().get("rebuilt")).longValue()).isEqualTo(0L);
        assertThat(DuckDBTestSupport.countRows(duckDB, "lake.known_entities")).isEqualTo(0L);
    }

    @Test
    void rebuildKnownEntities_idempotent_secondCallGivesSameResult() throws Exception {
        Instant ts = Instant.parse("2024-06-01T09:00:00Z");
        DuckDBTestSupport.insertEntityRow(duckDB, "order", "order-X", 3,
                "orders", 0, 0L, ts, Instant.now(), "order-X", null);

        // First rebuild
        restTemplate.postForEntity("/cassettes/entities/rebuild-known-entities", null, Map.class);
        // Second rebuild should produce identical state
        ResponseEntity<Map> second = restTemplate.postForEntity(
                "/cassettes/entities/rebuild-known-entities", null, Map.class);

        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(((Number) second.getBody().get("rebuilt")).longValue()).isEqualTo(1L);
        assertKnownEntity("order", "order-X", 3, ts, ts);
        assertThat(DuckDBTestSupport.countRows(duckDB, "lake.known_entities")).isEqualTo(1L);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void assertKnownEntity(String entityType, String entityId, int expectedBucket,
                                   Instant expectedFirstSeen, Instant expectedLastSeen)
            throws Exception {
        try (PreparedStatement ps = duckDB.prepareStatement("""
                SELECT entity_bucket, first_seen, last_seen
                FROM lake.known_entities
                WHERE entity_type = ? AND entity_id = ?
                """)) {
            ps.setString(1, entityType);
            ps.setString(2, entityId);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next())
                        .as("Row (%s, %s) must exist in known_entities", entityType, entityId)
                        .isTrue();
                assertThat(rs.getInt("entity_bucket"))
                        .as("entity_bucket for (%s, %s)", entityType, entityId)
                        .isEqualTo(expectedBucket);
                assertThat(rs.getTimestamp("first_seen").toInstant())
                        .as("first_seen for (%s, %s)", entityType, entityId)
                        .isEqualTo(expectedFirstSeen);
                assertThat(rs.getTimestamp("last_seen").toInstant())
                        .as("last_seen for (%s, %s)", entityType, entityId)
                        .isEqualTo(expectedLastSeen);
            }
        }
    }

    /**
     * Creates the MinIO bucket used as DuckLake DATA_PATH.
     * Called from {@link #minioProperties} before the Spring context starts.
     */
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
