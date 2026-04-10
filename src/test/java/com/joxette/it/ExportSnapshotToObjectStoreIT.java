package com.joxette.it;

import com.joxette.replay.ObjectStoreSnapshotInfo;
import com.joxette.replay.SnapshotInfo;
import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@code POST /cassettes/snapshots/export-to-object-store}.
 *
 * <h2>Strategy</h2>
 * <p>Starts both a Kafka container (required by the Spring Boot context) and a MinIO
 * container (S3-compatible object store).  Dynamic properties wire the running Spring
 * application to both containers.
 *
 * <p>The test:
 * <ol>
 *   <li>Seeds {@code lake.cassette} directly via the shared DuckDB connection.</li>
 *   <li>Calls {@code POST /cassettes/snapshots/export-to-object-store}.</li>
 *   <li>Verifies exported files are present in the MinIO bucket.</li>
 *   <li>Verifies a snapshot record was inserted into {@code lake.snapshots}.</li>
 * </ol>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class ExportSnapshotToObjectStoreIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @Container
    @SuppressWarnings("resource")
    static GenericContainer<?> minio = new GenericContainer<>(
            DockerImageName.parse("minio/minio:RELEASE.2024-01-29T03-56-32Z"))
            .withEnv("MINIO_ROOT_USER", "minioadmin")
            .withEnv("MINIO_ROOT_PASSWORD", "minioadmin")
            .withCommand("server", "/data", "--console-address", ":9001")
            .withExposedPorts(9000)
            .waitingFor(Wait.forHttp("/minio/health/live").forPort(9000));

    static final String TEST_BUCKET = "joxette-it";
    static final String SNAPSHOT_NAME = "it-export-snap";

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("joxette.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("joxette.object-store.endpoint-url",
                () -> "http://localhost:" + minio.getMappedPort(9000));
        registry.add("joxette.object-store.access-key", () -> "minioadmin");
        registry.add("joxette.object-store.secret-key", () -> "minioadmin");
        registry.add("joxette.object-store.bucket", () -> TEST_BUCKET);
        registry.add("joxette.object-store.force-path-style", () -> "true");
    }

    @LocalServerPort
    private int port;

    private final RestTemplate restTemplate = new RestTemplate();

    /** Shared DuckDB connection — same instance used by all services in the Spring context. */
    @Autowired
    private Connection duckDB;

    /** S3Client wired to MinIO by S3Config (created because the bucket property is set). */
    @Autowired
    private S3Client s3Client;

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    @BeforeEach
    void setUp() throws Exception {
        // Ensure the test bucket exists in MinIO.
        try {
            s3Client.headBucket(r -> r.bucket(TEST_BUCKET));
        } catch (NoSuchBucketException e) {
            s3Client.createBucket(r -> r.bucket(TEST_BUCKET));
        }

        // Create the general cassette table that the test will seed with rows.
        // Must be done before any insertCassetteRow() calls; idempotent (IF NOT EXISTS).
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, "export-test-topic");

        // Clear DuckDB state.
        // general cassette table: lake.main.general_export_test_topic
        // snapshots registry:     snapshots (plain DuckDB, primary-DB main schema — no lake. prefix)
        try (Statement st = duckDB.createStatement()) {
            st.execute("DELETE FROM lake.main.general_export_test_topic");
            st.execute("DELETE FROM snapshots");
        }

        // Remove any leftover local snapshot directory from a previous failed run.
        Path snapshotDir = Path.of("snapshots", SNAPSHOT_NAME);
        if (Files.exists(snapshotDir)) {
            deleteDirectory(snapshotDir);
        }

        // Remove any objects from the previous test run so list assertions start clean.
        var listed = s3Client.listObjectsV2(
                r -> r.bucket(TEST_BUCKET).prefix("snapshots/" + SNAPSHOT_NAME + "/"));
        for (var obj : listed.contents()) {
            s3Client.deleteObject(r -> r.bucket(TEST_BUCKET).key(obj.key()));
        }
    }

    // -------------------------------------------------------------------------
    // Test
    // -------------------------------------------------------------------------

    @Test
    void exportToObjectStore_uploadsFilesToMinioAndCreatesSnapshotRecord() throws Exception {
        // Arrange: seed two cassette messages.
        String topic = "export-test-topic";
        Instant ts = Instant.parse("2024-07-01T10:00:00Z");
        DuckDBTestSupport.insertCassetteRow(duckDB, topic, 0, 0L, ts, Instant.now(),
                "msg-key-1", "msg-value-1".getBytes(StandardCharsets.UTF_8));
        DuckDBTestSupport.insertCassetteRow(duckDB, topic, 0, 1L, ts.plusSeconds(1), Instant.now(),
                "msg-key-2", "msg-value-2".getBytes(StandardCharsets.UTF_8));

        // Act: call the export endpoint.
        ResponseEntity<ObjectStoreSnapshotInfo> response = restTemplate.postForEntity(
                url("/cassettes/snapshots/export-to-object-store"),
                Map.of("name", SNAPSHOT_NAME),
                ObjectStoreSnapshotInfo.class);

        // Assert: response is HTTP 201 with expected metadata.
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        ObjectStoreSnapshotInfo info = response.getBody();
        assertThat(info).isNotNull();
        assertThat(info.name()).isEqualTo(SNAPSHOT_NAME);
        assertThat(info.sizeBytes()).isPositive();
        assertThat(info.objectStoreUri())
                .startsWith("s3://" + TEST_BUCKET + "/snapshots/" + SNAPSHOT_NAME + "/");

        // Assert: exported files are present in MinIO under the expected prefix.
        ListObjectsV2Response objects = s3Client.listObjectsV2(
                r -> r.bucket(TEST_BUCKET).prefix("snapshots/" + SNAPSHOT_NAME + "/"));
        assertThat(objects.contents())
                .as("MinIO should contain at least one exported file for snapshot '%s'", SNAPSHOT_NAME)
                .isNotEmpty();
        assertThat(objects.contents())
                .as("exported files should include schema.sql")
                .anyMatch(obj -> obj.key().endsWith("schema.sql"));

        // Assert: a snapshot record exists in lake.snapshots (queried via the REST API).
        ResponseEntity<List<SnapshotInfo>> snapshots = restTemplate.exchange(
                url("/cassettes/snapshots"),
                HttpMethod.GET, null,
                new ParameterizedTypeReference<>() {});
        assertThat(snapshots.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(snapshots.getBody())
                .as("lake.snapshots should contain a record for snapshot '%s'", SNAPSHOT_NAME)
                .isNotNull()
                .anyMatch(s -> SNAPSHOT_NAME.equals(s.name()));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static void deleteDirectory(Path dir) throws IOException {
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }
}
