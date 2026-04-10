package com.joxette.it;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.PagedResponse;
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
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full API integration test: record → replay round-trip.
 *
 * <h2>Strategy</h2>
 * <p>The recording pipeline ({@link com.joxette.recording.TopicRecorder}) writes
 * to per-topic DuckLake tables ({@code lake.main.general_<normalized_topic>}).
 * This test exercises the full round-trip by:
 * <ol>
 *   <li>Inserting records directly into {@code lake.main.general_<topic>} via the
 *       shared DuckDB connection (simulating what a complete recording pipeline would
 *       produce) — this validates the MVC → Service → DuckDB → HTTP response
 *       chain in full.</li>
 *   <li>Additionally starting a live recorder for a test topic, publishing
 *       Kafka messages, and verifying they appear in the per-topic cassette
 *       table — this validates the Kafka → DuckDB recording path.</li>
 * </ol>
 *
 * <p>The Spring Boot context uses the {@code it} profile which points DuckDB at
 * an in-memory database and disables bootstrap topic seeding.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class RecordReplayRoundTripIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("joxette.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @LocalServerPort
    private int port;

    private RestTemplate restTemplate;

    private String baseUrl;

    /** Shared DuckDB connection from the Spring context — same instance used by all services. */
    @Autowired
    private Connection duckDB;

    private static final String TEST_TOPIC = "integration.test.events";

    @BeforeEach
    void setUp() throws Exception {
        restTemplate = new RestTemplate();
        baseUrl = "http://localhost:" + port;
        // Ensure the per-topic cassette table exists and is empty before each test.
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TEST_TOPIC);
        try (Statement st = duckDB.createStatement()) {
            st.execute("DELETE FROM lake.main.general_integration_test_events");
        }
    }

    // -------------------------------------------------------------------------
    // Replay API round-trip (direct insert → REST query)
    // -------------------------------------------------------------------------

    @Test
    void replayApi_returnsInsertedRecords() throws Exception {
        Instant ts = Instant.parse("2024-07-01T10:00:00Z");
        DuckDBTestSupport.insertCassetteRow(duckDB, TEST_TOPIC, 0, 0L, ts, Instant.now(),
                "order-key-1", "{\"order_id\":\"ORD-1\"}".getBytes(StandardCharsets.UTF_8));
        DuckDBTestSupport.insertCassetteRow(duckDB, TEST_TOPIC, 0, 1L, ts.plusSeconds(1),
                Instant.now(), "order-key-2",
                "{\"order_id\":\"ORD-2\"}".getBytes(StandardCharsets.UTF_8));

        ResponseEntity<PagedResponse<CassetteRecord>> response = restTemplate.exchange(
                baseUrl + "/cassettes/topics/" + TEST_TOPIC,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<>() {});

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        PagedResponse<CassetteRecord> body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.data()).hasSize(2);

        CassetteRecord first = body.data().get(0);
        assertThat(first.topic()).isEqualTo(TEST_TOPIC);
        assertThat(first.partition()).isEqualTo(0);
        assertThat(first.offset()).isEqualTo(0L);
        assertThat(first.key()).isEqualTo("order-key-1");
        assertThat(new String(Base64.getUrlDecoder().decode(first.value()), StandardCharsets.UTF_8))
                .isEqualTo("{\"order_id\":\"ORD-1\"}");
    }

    @Test
    void replayApi_emptyTopic_returnsEmptyPage() {
        ResponseEntity<PagedResponse<CassetteRecord>> response = restTemplate.exchange(
                baseUrl + "/cassettes/topics/" + TEST_TOPIC,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<>() {});

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().data()).isEmpty();
        assertThat(response.getBody().hasMore()).isFalse();
    }

    @Test
    void replayApi_pagination_cursorNavigatesPages() throws Exception {
        Instant base = Instant.parse("2024-08-01T00:00:00Z");
        for (int i = 0; i < 5; i++) {
            DuckDBTestSupport.insertCassetteRow(duckDB, TEST_TOPIC, 0, i,
                    base.plusSeconds(i), Instant.now(), "k" + i, null);
        }

        ResponseEntity<PagedResponse<CassetteRecord>> page1 = restTemplate.exchange(
                baseUrl + "/cassettes/topics/" + TEST_TOPIC + "?limit=2",
                HttpMethod.GET, null, new ParameterizedTypeReference<>() {});

        assertThat(page1.getBody()).isNotNull();
        assertThat(page1.getBody().data()).hasSize(2);
        assertThat(page1.getBody().hasMore()).isTrue();
        String cursor = page1.getBody().nextCursor();
        assertThat(cursor).isNotNull();

        ResponseEntity<PagedResponse<CassetteRecord>> page2 = restTemplate.exchange(
                baseUrl + "/cassettes/topics/" + TEST_TOPIC + "?limit=2&cursor=" + cursor,
                HttpMethod.GET, null, new ParameterizedTypeReference<>() {});

        assertThat(page2.getBody()).isNotNull();
        assertThat(page2.getBody().data()).hasSize(2);
        assertThat(page2.getBody().data().get(0).offset()).isEqualTo(2L);

        String cursor2 = page2.getBody().nextCursor();
        ResponseEntity<PagedResponse<CassetteRecord>> page3 = restTemplate.exchange(
                baseUrl + "/cassettes/topics/" + TEST_TOPIC + "?limit=2&cursor=" + cursor2,
                HttpMethod.GET, null, new ParameterizedTypeReference<>() {});

        assertThat(page3.getBody()).isNotNull();
        assertThat(page3.getBody().data()).hasSize(1);
        assertThat(page3.getBody().hasMore()).isFalse();
    }

    @Test
    void replayApi_deduplication_querySideMostRecentWins() throws Exception {
        Instant ts = Instant.parse("2024-09-01T12:00:00Z");
        // Insert same (topic, partition, offset) twice — DuckDB allows this.
        DuckDBTestSupport.insertCassetteRow(duckDB, TEST_TOPIC, 0, 0L, ts,
                Instant.parse("2024-09-01T12:00:00Z"), "old-key", b("old"));
        DuckDBTestSupport.insertCassetteRow(duckDB, TEST_TOPIC, 0, 0L, ts,
                Instant.parse("2024-09-01T12:00:05Z"), "new-key", b("new"));

        ResponseEntity<PagedResponse<CassetteRecord>> response = restTemplate.exchange(
                baseUrl + "/cassettes/topics/" + TEST_TOPIC,
                HttpMethod.GET, null, new ParameterizedTypeReference<>() {});

        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().data()).hasSize(1);
        assertThat(response.getBody().data().get(0).key()).isEqualTo("new-key");
    }

    // -------------------------------------------------------------------------
    // Kafka recording path (record side of the round-trip)
    // -------------------------------------------------------------------------

    @Test
    void kafkaRecording_messagesAppearInPerTopicCassetteTable() throws Exception {
        // Create a Kafka topic and register it via the management REST API.
        String recordingTopic = "recording.live.test";
        createKafkaTopic(recordingTopic, 1);
        registerTopicViaApi(recordingTopic, "general");

        // Publish messages to Kafka.
        int msgCount = 3;
        try (KafkaProducer<String, byte[]> producer = newProducer()) {
            for (int i = 0; i < msgCount; i++) {
                producer.send(new ProducerRecord<>(recordingTopic, "k" + i,
                        ("v" + i).getBytes(StandardCharsets.UTF_8))).get(5, TimeUnit.SECONDS);
            }
        }

        // The recorder writes to lake.main.general_<normalized_topic>. Wait for rows to appear.
        String normalized = recordingTopic.toLowerCase().replaceAll("[^a-z0-9_]", "_");
        String cassetteTable = "lake.main.general_" + normalized;
        awaitRowCount(cassetteTable, msgCount, Duration.ofSeconds(20));

        long count = DuckDBTestSupport.countRows(duckDB, cassetteTable);
        assertThat(count).isGreaterThanOrEqualTo(msgCount);
    }

    // -------------------------------------------------------------------------
    // Health endpoint
    // -------------------------------------------------------------------------

    @Test
    void healthEndpoint_returnsUp() {
        ResponseEntity<Map> response =
                restTemplate.getForEntity(baseUrl + "/actuator/health", Map.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().get("status")).isEqualTo("UP");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void registerTopicViaApi(String topic, String mode) {
        // TopicController is mapped to POST /topics
        Map<String, Object> body = new HashMap<>();
        body.put("topic", topic);
        body.put("mode", mode);
        restTemplate.postForEntity(baseUrl + "/topics", body, Object.class);
    }

    private void createKafkaTopic(String topic, int partitions) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);
        }
    }

    private KafkaProducer<String, byte[]> newProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    private void awaitRowCount(String table, long expected, Duration timeout) throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            try (Statement st = duckDB.createStatement()) {
                try (var rs = st.executeQuery("SELECT COUNT(*) FROM " + table)) {
                    if (rs.next() && rs.getLong(1) >= expected) return;
                }
            } catch (Exception ignored) {
                // table not yet created by the recorder
            }
            Thread.sleep(100);
        }
        // Produce a meaningful assertion failure.
        long actual = 0;
        try (Statement st = duckDB.createStatement();
             var rs = st.executeQuery("SELECT COUNT(*) FROM " + table)) {
            if (rs.next()) actual = rs.getLong(1);
        } catch (Exception ignored) {}
        assertThat(actual)
                .as("Expected ≥%d rows in %s within %s", expected, table, timeout)
                .isGreaterThanOrEqualTo(expected);
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
