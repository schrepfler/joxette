package com.joxette.it;

import com.joxette.recording.RecordingCoordinator;
import com.joxette.support.DuckDBTestSupport;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for the snapshot-restore safety net:
 * {@code CassetteLifecycleService.restoreSnapshot()} must (1) pause active
 * recorders, (2) verify restored row counts against the snapshot's stored
 * metadata, (3) throw a typed exception on mismatch, and (4) resume recorders
 * in a {@code finally} regardless of verification outcome.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class RestoreSnapshotVerificationIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:4.0.2"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("joxette.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @LocalServerPort private int port;
    @Autowired private Connection duckDB;
    @Autowired private RecordingCoordinator recordingCoordinator;

    private final RestTemplate restTemplate = new RestTemplate();

    static final String TEST_TOPIC = "verify-restore-topic";
    static final String NORMALIZED_TABLE = "general_verify_restore_topic";
    static final String SNAPSHOT_NAME = "it-verify-restore-snap";

    private String url(String path) {
        return "http://localhost:" + port + "/v1" + path;
    }

    @BeforeEach
    void setUp() throws Exception {
        createKafkaTopic(TEST_TOPIC, 1);
        try (Statement st = duckDB.createStatement()) {
            st.execute("DELETE FROM snapshots");
        }
        Path snapshotDir = Path.of("snapshots", SNAPSHOT_NAME);
        if (Files.exists(snapshotDir)) {
            deleteDirectory(snapshotDir);
        }
    }

    @Test
    void restoreSnapshot_truncatedBackingFile_throwsTypedExceptionAndStillResumesRecorders() throws Exception {
        // Step 1: register recording and produce 3 messages so the general
        // cassette table (and the recorder) have real content.
        //
        // The general cassette table must exist BEFORE the recorder's first write:
        // lake.main.general_{topic} tables are only ever created for topics listed
        // in bootstrap YAML config (SchemaManager.createLakeTables() at startup) —
        // there is no dynamic per-topic table creation analogous to
        // SchemaManager.createEntityTable() for topics registered later via
        // POST /topics. The `it` profile boots with an empty bootstrap topic list,
        // so without this pre-creation the recorder's first write hits a permanent
        // "Catalog Error: Table ... does not exist" and crash-loops forever. This
        // mirrors the pre-creation RestoreSnapshotIT / SnapshotTruncateRestoreIT
        // already rely on for the same reason.
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TEST_TOPIC);
        restTemplate.postForEntity(url("/topics"),
                Map.of("topic", TEST_TOPIC, "mode", "general", "startFrom", "earliest"), Object.class);

        try (KafkaProducer<String, byte[]> producer = newProducer()) {
            for (int i = 0; i < 3; i++) {
                producer.send(new ProducerRecord<>(TEST_TOPIC, "key-" + i,
                        ("value-" + i).getBytes())).get(5, TimeUnit.SECONDS);
            }
        }

        // .ignoreExceptions(): Awaitility's untilAsserted only retries AssertionError
        // by default, not arbitrary exceptions — without this, a transient SQLException
        // from an early poll (e.g. before the consumer group has finished assigning) would
        // fail the test immediately instead of retrying until the timeout.
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100)).ignoreExceptions().untilAsserted(() ->
                assertThat(DuckDBTestSupport.countRows(duckDB, "lake.main." + NORMALIZED_TABLE)).isEqualTo(3));
        await().atMost(Duration.ofSeconds(10)).ignoreExceptions().untilAsserted(() ->
                assertThat(recordingCoordinator.activeTopics()).contains(TEST_TOPIC));

        // Step 2: snapshot — records row_counts = {general_verify_restore_topic: 3}.
        var createResp = restTemplate.postForEntity(
                url("/cassettes/snapshots"), Map.of("name", SNAPSHOT_NAME), Object.class);
        assertThat(createResp.getStatusCode().value()).isEqualTo(201);

        // Step 3: deliberately truncate the snapshot's backing Parquet file to
        // zero rows — the declared row_counts metadata (3) no longer matches
        // what IMPORT DATABASE will actually restore (0).
        Path parquet = Path.of("snapshots", SNAPSHOT_NAME, "lake", NORMALIZED_TABLE + ".parquet");
        assertThat(Files.exists(parquet)).as("snapshot Parquet export must exist").isTrue();
        try (Statement st = duckDB.createStatement()) {
            st.execute("COPY (SELECT * FROM lake.main." + NORMALIZED_TABLE + " LIMIT 0) TO '"
                    + parquet + "' (FORMAT PARQUET)");
        }

        // Step 4: restore must fail with a typed 409, not silently succeed with data loss.
        assertThatThrownBy(() -> restTemplate.postForEntity(
                url("/cassettes/snapshots/" + SNAPSHOT_NAME + "/restore"), null, Void.class))
                .isInstanceOf(HttpClientErrorException.class)
                .satisfies(ex -> {
                    HttpClientErrorException hce = (HttpClientErrorException) ex;
                    assertThat(hce.getStatusCode().value()).isEqualTo(409);
                    assertThat(hce.getResponseBodyAsString()).contains("ERR_SNAPSHOT_VERIFICATION_FAILED");
                });

        // Step 5: even though restore failed, the recorder paused for the restore
        // attempt must have been resumed — never left stopped indefinitely.
        await().atMost(Duration.ofSeconds(15)).ignoreExceptions().untilAsserted(() ->
                assertThat(recordingCoordinator.activeTopics()).contains(TEST_TOPIC));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private KafkaProducer<String, byte[]> newProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    private void createKafkaTopic(String topic, int partitions) {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);
        } catch (Exception ignored) {}
    }

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
