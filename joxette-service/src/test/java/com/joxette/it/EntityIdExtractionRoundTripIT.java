package com.joxette.it;

import com.joxette.support.DuckDBTestSupport;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
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

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for the full Kafka → EntityIdExtractor → entity cassette round-trip,
 * parameterised over all three {@code IdSource} variants.
 *
 * <h2>Gap closed</h2>
 * <p>The unit path ({@code EntityIdExtractorTest}) covers KEY / VALUE / HEADER extraction
 * in isolation.  This IT exercises the live pipeline end-to-end: a real Kafka message is
 * produced, the recording pipeline extracts the entity ID via the configured source and
 * expression, and the extracted ID lands in {@code lake.main.entity_{type}}.
 *
 * <h2>Scenarios</h2>
 * <ol>
 *   <li><b>KEY</b> — entity ID is the raw message key string.</li>
 *   <li><b>VALUE</b> — entity ID extracted via JSONPath from the message value bytes.</li>
 *   <li><b>HEADER</b> — entity ID decoded from a named Kafka header (UTF-8).</li>
 * </ol>
 *
 * <p>Each case uses a dedicated entity type and topic to avoid cross-case interference.
 * Awaitility is used for all async assertions (never {@code Thread.sleep}).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class EntityIdExtractionRoundTripIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:4.0.2"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("joxette.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @LocalServerPort int port;
    @Autowired Connection duckDB;

    private final RestTemplate rest = new RestTemplate();

    // -------------------------------------------------------------------------
    // Scenario model
    // -------------------------------------------------------------------------

    record ExtractionCase(
            String description,
            String entityType,
            String topic,
            String idSource,
            String idExpression,    // null → omitted from the matcher body
            String expectedEntityId,
            String msgKey,
            byte[] msgValue,
            Map<String, String> msgHeaders  // header-name → UTF-8 value
    ) {
        @Override
        public String toString() { return description; }
    }

    static Stream<ExtractionCase> extractionScenarios() {
        return Stream.of(
                new ExtractionCase(
                        "KEY: entity ID from message key",
                        "it_eid_key",
                        "eid-test-key",
                        "key", null,
                        "KEY-ENTITY-42",
                        "KEY-ENTITY-42",
                        "{\"data\":\"irrelevant\"}".getBytes(StandardCharsets.UTF_8),
                        Map.of()
                ),
                new ExtractionCase(
                        "VALUE: entity ID via JSONPath on message value",
                        "it_eid_value",
                        "eid-test-value",
                        "value", "$.device_id",
                        "DEVICE-999",
                        null,
                        "{\"device_id\":\"DEVICE-999\",\"event\":\"ping\"}".getBytes(StandardCharsets.UTF_8),
                        Map.of()
                ),
                new ExtractionCase(
                        "HEADER: entity ID from x-entity-id header",
                        "it_eid_header",
                        "eid-test-header",
                        "header", "x-entity-id",
                        "HEADER-ENT-7",
                        null,
                        "{\"payload\":\"test\"}".getBytes(StandardCharsets.UTF_8),
                        Map.of("x-entity-id", "HEADER-ENT-7")
                )
        );
    }

    // -------------------------------------------------------------------------
    // Parameterised round-trip test
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("extractionScenarios")
    void entityIdExtraction_fullKafkaRoundTrip(ExtractionCase c) throws Exception {
        // Step 1: create Kafka topic.
        // Do NOT create the entity cassette table manually here — POST /entities calls
        // SchemaManager.createEntityTable() which registers it with DuckLake.  Creating
        // it with DuckDBTestSupport first would produce a raw table that DuckLake does
        // not know about, causing entity writes to fail.
        createKafkaTopic(c.topic(), 1);

        // Step 2: produce the message BEFORE registering the Joxette recording pipeline.
        // Producing first guarantees the message is at offset 0; when the recorder starts
        // with startFrom=earliest it will consume it retroactively.
        try (KafkaProducer<String, byte[]> producer = newProducer()) {
            ProducerRecord<String, byte[]> record =
                    new ProducerRecord<>(c.topic(), c.msgKey(), c.msgValue());
            c.msgHeaders().forEach((name, val) ->
                    record.headers().add(new RecordHeader(name,
                            val.getBytes(StandardCharsets.UTF_8))));
            producer.send(record).get(5, TimeUnit.SECONDS);
        }

        // Step 3: register entity type (idempotent — 409 silently ignored).
        try {
            rest.postForEntity(url("/entities"),
                    Map.of("type", c.entityType(), "buckets", 256), Object.class);
        } catch (HttpClientErrorException ignored) {}

        // Step 4: register source mapping with the scenario-specific idSource + idExpression.
        Map<String, Object> matcherBody = new HashMap<>();
        matcherBody.put("messageType", "test_event");
        matcherBody.put("idSource", c.idSource());
        // KEY source ignores idExpression; empty string is a harmless placeholder.
        matcherBody.put("idExpression", c.idExpression() != null ? c.idExpression() : "");
        try {
            Map<String, Object> sourceBody = new HashMap<>();
            sourceBody.put("topic", c.topic());
            sourceBody.put("mode", "entity_only");
            sourceBody.put("matchers", List.of(matcherBody));
            rest.postForEntity(url("/entities/" + c.entityType() + "/sources"),
                    sourceBody, Object.class);
        } catch (HttpClientErrorException ignored) {}

        // Step 5: wait for the source mapping to be persisted in DuckDB.
        // The MessageRouter.reload() fires asynchronously via Pekko pub/sub after the
        // EntityConfigChanged event.  Confirming the DB write ensures the router will
        // have the mapping loaded by the time the recorder starts consuming.
        await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    try (var ps = duckDB.prepareStatement(
                            "SELECT COUNT(*) FROM entity_source_matchers WHERE entity_type = ?")) {
                        ps.setString(1, c.entityType());
                        try (var rs = ps.executeQuery()) {
                            assertThat(rs.next()).isTrue();
                            assertThat(rs.getLong(1)).isGreaterThanOrEqualTo(1L);
                        }
                    }
                });

        // Step 6: register the Kafka topic for entity-only recording with startFrom=earliest.
        // The recorder starts and seeks to offset 0, consuming the message produced in step 2.
        try {
            Map<String, Object> topicBody = new HashMap<>();
            topicBody.put("topic", c.topic());
            topicBody.put("mode", "entity_only");
            topicBody.put("startFrom", "earliest");
            rest.postForEntity(url("/topics"), topicBody, Object.class);
        } catch (HttpClientErrorException ignored) {}

        // Step 7: wait for the pipeline to route the message into the entity cassette table.
        String entityTable = "lake.main.entity_" + c.entityType();
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    try (Statement st = duckDB.createStatement();
                         var rs = st.executeQuery("SELECT COUNT(*) FROM " + entityTable)) {
                        assertThat(rs.next()).isTrue();
                        assertThat(rs.getLong(1)).isGreaterThanOrEqualTo(1L);
                    }
                });

        // Step 8: assert the entity ID was correctly extracted from the configured source.
        try (var ps = duckDB.prepareStatement(
                "SELECT entity_id FROM " + entityTable + " LIMIT 1")) {
            try (var rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("entity_id")).isEqualTo(c.expectedEntityId());
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

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
}
