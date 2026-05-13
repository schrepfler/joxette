package com.joxette.it;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.PagedResponse;
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
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

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
 * Integration test for {@code POST /cassettes/snapshots/{name}/restore}.
 *
 * <h2>Strategy</h2>
 * <ol>
 *   <li>Seed pre-snapshot records directly into the general cassette table.</li>
 *   <li>Create a named snapshot via {@code POST /cassettes/snapshots}.</li>
 *   <li>Write additional post-snapshot records into the same table.</li>
 *   <li>Call {@code POST /cassettes/snapshots/{name}/restore}.</li>
 *   <li>Assert that the replay API returns only the pre-snapshot records
 *       (post-snapshot records are gone because the restore replaced the table).</li>
 * </ol>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class RestoreSnapshotIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:4.0.2"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("joxette.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @LocalServerPort
    private int port;

    private final RestTemplate restTemplate = new RestTemplate();

    /** Shared DuckDB connection — same instance used by all services in the Spring context. */
    @Autowired
    private Connection duckDB;

    static final String TEST_TOPIC = "restore-test-topic";
    static final String SNAPSHOT_NAME = "it-restore-snap";

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    @BeforeEach
    void setUp() throws Exception {
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TEST_TOPIC);
        try (Statement st = duckDB.createStatement()) {
            st.execute("DELETE FROM lake.main.general_restore_test_topic");
            st.execute("DELETE FROM snapshots");
        }
        // Remove any local snapshot directory left by a previous failed run.
        Path snapshotDir = Path.of("snapshots", SNAPSHOT_NAME);
        if (Files.exists(snapshotDir)) {
            deleteDirectory(snapshotDir);
        }
    }

    // -------------------------------------------------------------------------
    // Test
    // -------------------------------------------------------------------------

    @Test
    void restoreSnapshot_replacesTableContentsWithPreSnapshotState() throws Exception {
        Instant ts = Instant.parse("2024-07-01T10:00:00Z");

        // Step 1: write pre-snapshot records.
        DuckDBTestSupport.insertCassetteRow(duckDB, TEST_TOPIC, 0, 0L, ts, Instant.now(),
                "pre-key-1", "pre-value-1".getBytes(StandardCharsets.UTF_8));
        DuckDBTestSupport.insertCassetteRow(duckDB, TEST_TOPIC, 0, 1L, ts.plusSeconds(1), Instant.now(),
                "pre-key-2", "pre-value-2".getBytes(StandardCharsets.UTF_8));

        // Step 2: create a snapshot via the REST API.
        ResponseEntity<SnapshotInfo> createResp = restTemplate.postForEntity(
                url("/cassettes/snapshots"),
                Map.of("name", SNAPSHOT_NAME),
                SnapshotInfo.class);
        assertThat(createResp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(createResp.getBody()).isNotNull();
        assertThat(createResp.getBody().name()).isEqualTo(SNAPSHOT_NAME);

        // Step 3: write post-snapshot records (these should disappear after restore).
        DuckDBTestSupport.insertCassetteRow(duckDB, TEST_TOPIC, 0, 2L, ts.plusSeconds(2), Instant.now(),
                "post-key-3", "post-value-3".getBytes(StandardCharsets.UTF_8));
        DuckDBTestSupport.insertCassetteRow(duckDB, TEST_TOPIC, 0, 3L, ts.plusSeconds(3), Instant.now(),
                "post-key-4", "post-value-4".getBytes(StandardCharsets.UTF_8));

        // Sanity check: all four records are visible before restore.
        assertThat(DuckDBTestSupport.countRows(duckDB, "lake.main.general_restore_test_topic"))
                .isEqualTo(4);

        // Step 4: call the restore endpoint.
        ResponseEntity<Void> restoreResp = restTemplate.postForEntity(
                url("/cassettes/snapshots/" + SNAPSHOT_NAME + "/restore"),
                null,
                Void.class);
        assertThat(restoreResp.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Step 5: only the two pre-snapshot records should be visible.
        ResponseEntity<PagedResponse<CassetteRecord>> replayResp = restTemplate.exchange(
                url("/cassettes/topics/" + TEST_TOPIC),
                HttpMethod.GET, null,
                new ParameterizedTypeReference<>() {});
        assertThat(replayResp.getStatusCode()).isEqualTo(HttpStatus.OK);

        List<CassetteRecord> records = replayResp.getBody().data();
        assertThat(records).hasSize(2);
        assertThat(records).extracting(CassetteRecord::key)
                .containsExactlyInAnyOrder("pre-key-1", "pre-key-2");
        assertThat(records).extracting(CassetteRecord::offset)
                .doesNotContain(2L, 3L);
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
