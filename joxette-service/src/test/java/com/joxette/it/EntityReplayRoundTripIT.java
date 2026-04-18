package com.joxette.it;

import com.joxette.replay.EntityInfo;
import com.joxette.replay.EntityRecord;
import com.joxette.replay.EntityStats;
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
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for the full entity recording → replay round-trip.
 *
 * <h2>Scenario</h2>
 * <ol>
 *   <li>Register an entity type via {@code POST /entities} with a source mapping
 *       that extracts {@code entity_id} from {@code $.order_id} in the message value.</li>
 *   <li>Register the test topic for {@code 'both'} general + entity recording via
 *       {@code POST /topics}.</li>
 *   <li>Produce JSON messages to the Kafka topic with a recognisable entity ID
 *       embedded in the value.</li>
 *   <li>Wait for rows to appear in {@code lake.main.entity_{type}} (recording
 *       pipeline latency).</li>
 *   <li>Assert {@code GET /cassettes/entities/{type}/{id}} returns the expected
 *       records with the correct entity ID, partition, and offsets.</li>
 *   <li>Assert {@code GET /cassettes/entities/{type}} lists the entity ID in the
 *       known-entities registry.</li>
 *   <li>Assert {@code GET /cassettes/entities/{type}/{id}/stats} reports
 *       {@code messageCount} equal to the number of messages produced.</li>
 * </ol>
 *
 * <p>The Spring Boot context uses the {@code it} profile (in-memory DuckDB, fast batching,
 * no bootstrap seeding). Registration calls are idempotent — {@code 409 Conflict} is
 * silently ignored so the same context can be reused across test methods.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class EntityReplayRoundTripIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:4.0.2"));

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

    private static final String ENTITY_TYPE  = "itorder";
    private static final String TEST_TOPIC   = "entity.roundtrip.test";
    private static final String ENTITY_ID    = "ORD-ROUNDTRIP-1";
    private static final String MESSAGE_TYPE = "order_event";

    @BeforeEach
    void setUp() throws Exception {
        restTemplate = new RestTemplate();
        baseUrl = "http://localhost:" + port;

        // Create Kafka topic once; ignore AlreadyExistsException on subsequent test methods.
        createKafkaTopic(TEST_TOPIC, 1);

        // Ensure the entity cassette table exists before we attempt to delete from it.
        DuckDBTestSupport.createEntityTable(duckDB, ENTITY_TYPE);

        // Register entity type — idempotent, ignore 409 if already registered.
        try {
            Map<String, Object> entityBody = new HashMap<>();
            entityBody.put("type", ENTITY_TYPE);
            entityBody.put("buckets", 256);
            restTemplate.postForEntity(baseUrl + "/entities", entityBody, Object.class);
        } catch (HttpClientErrorException ignored) { /* 409 Conflict — already registered */ }

        // Add source mapping: extract entity_id from $.order_id in the message value.
        try {
            Map<String, Object> sourceBody = new HashMap<>();
            sourceBody.put("topic", TEST_TOPIC);
            sourceBody.put("mode", "entity_only");
            sourceBody.put("matchers", List.of(Map.of(
                    "messageType",  MESSAGE_TYPE,
                    "idSource",     "value",
                    "idExpression", "$.order_id")));
            restTemplate.postForEntity(
                    baseUrl + "/entities/" + ENTITY_TYPE + "/sources", sourceBody, Object.class);
        } catch (HttpClientErrorException ignored) { /* 409 Conflict — mapping already exists */ }

        // Register the test topic for 'both' general + entity recording.
        try {
            Map<String, Object> topicBody = new HashMap<>();
            topicBody.put("topic", TEST_TOPIC);
            topicBody.put("mode", "both");
            restTemplate.postForEntity(baseUrl + "/topics", topicBody, Object.class);
        } catch (HttpClientErrorException ignored) { /* 409 Conflict — topic already registered */ }

        // Wipe data written by any previous test run sharing this JVM context.
        try (Statement st = duckDB.createStatement()) {
            st.execute("DELETE FROM lake.main.entity_" + ENTITY_TYPE);
            st.execute("DELETE FROM known_entities WHERE entity_type = '" + ENTITY_TYPE + "'");
        }
    }

    // -------------------------------------------------------------------------
    // Full entity recording → replay round-trip
    // -------------------------------------------------------------------------

    @Test
    void entityRecording_fullRoundTrip_recordsAppearInAllReplayEndpoints() throws Exception {
        int msgCount = 3;

        // Produce messages whose JSON value embeds the recognisable entity ID.
        try (KafkaProducer<String, byte[]> producer = newProducer()) {
            for (int i = 0; i < msgCount; i++) {
                String value = "{\"order_id\":\"" + ENTITY_ID + "\",\"seq\":" + i + "}";
                producer.send(new ProducerRecord<>(TEST_TOPIC, "order-key-" + i,
                        value.getBytes(StandardCharsets.UTF_8))).get(5, TimeUnit.SECONDS);
            }
        }

        // Wait for all messages to be routed into the entity cassette table by the pipeline.
        String entityTable = "lake.main.entity_" + ENTITY_TYPE;
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> {
                    try (Statement st = duckDB.createStatement();
                         var rs = st.executeQuery("SELECT COUNT(*) FROM " + entityTable)) {
                        return rs.next() && rs.getLong(1) >= msgCount;
                    } catch (Exception e) {
                        return false; // table not yet visible — keep polling
                    }
                });

        // Wait for known_entities to be populated — written in the same batch as entity rows.
        await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> {
                    try (var ps = duckDB.prepareStatement(
                            "SELECT COUNT(*) FROM known_entities WHERE entity_type = ? AND entity_id = ?")) {
                        ps.setString(1, ENTITY_TYPE);
                        ps.setString(2, ENTITY_ID);
                        try (var rs = ps.executeQuery()) {
                            return rs.next() && rs.getLong(1) > 0;
                        }
                    } catch (Exception e) {
                        return false;
                    }
                });

        // -----------------------------------------------------------------
        // (6) GET /cassettes/entities/{type}/{entity_id} — replay entity events
        // -----------------------------------------------------------------
        ResponseEntity<PagedResponse<EntityRecord>> replayResponse = restTemplate.exchange(
                baseUrl + "/cassettes/entities/" + ENTITY_TYPE + "/" + ENTITY_ID,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<>() {});

        assertThat(replayResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        PagedResponse<EntityRecord> page = replayResponse.getBody();
        assertThat(page).isNotNull();
        assertThat(page.data()).hasSize(msgCount);

        for (EntityRecord record : page.data()) {
            assertThat(record.entityId()).isEqualTo(ENTITY_ID);
            assertThat(record.topic()).isEqualTo(TEST_TOPIC);
            assertThat(record.partition()).isGreaterThanOrEqualTo(0);
            assertThat(record.offset()).isGreaterThanOrEqualTo(0L);
        }

        // Single-partition topic → offsets are 0, 1, 2.
        List<Long> offsets = page.data().stream().map(EntityRecord::offset).sorted().toList();
        assertThat(offsets).containsExactly(0L, 1L, 2L);

        // -----------------------------------------------------------------
        // (7) GET /cassettes/entities/{type} — known-entities registry listing
        // -----------------------------------------------------------------
        ResponseEntity<PagedResponse<EntityInfo>> listResponse = restTemplate.exchange(
                baseUrl + "/cassettes/entities/" + ENTITY_TYPE,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<>() {});

        assertThat(listResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        PagedResponse<EntityInfo> entityList = listResponse.getBody();
        assertThat(entityList).isNotNull();
        List<String> entityIds = entityList.data().stream()
                .map(EntityInfo::entityId)
                .toList();
        assertThat(entityIds).contains(ENTITY_ID);

        // -----------------------------------------------------------------
        // (8) GET /cassettes/entities/{type}/{entity_id}/stats
        // -----------------------------------------------------------------
        ResponseEntity<EntityStats> statsResponse = restTemplate.getForEntity(
                baseUrl + "/cassettes/entities/" + ENTITY_TYPE + "/" + ENTITY_ID + "/stats",
                EntityStats.class);

        assertThat(statsResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        EntityStats stats = statsResponse.getBody();
        assertThat(stats).isNotNull();
        assertThat(stats.entityType()).isEqualTo(ENTITY_TYPE);
        assertThat(stats.entityId()).isEqualTo(ENTITY_ID);
        assertThat(stats.messageCount()).isEqualTo(msgCount);
        assertThat(stats.countByTopic()).containsKey(TEST_TOPIC);
        assertThat(stats.countByTopic().get(TEST_TOPIC)).isEqualTo((long) msgCount);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void createKafkaTopic(String topic, int partitions) {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // Topic already exists — acceptable.
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

}
