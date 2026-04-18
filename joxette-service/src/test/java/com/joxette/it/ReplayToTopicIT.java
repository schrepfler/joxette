package com.joxette.it;

import com.joxette.replay.ReplayProgress;
import com.joxette.replay.ReplayToTopicRequest;
import com.joxette.support.DuckDBTestSupport;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the replay-to-topic path.
 *
 * <p>Seeds a small general cassette with three text messages at known kafka
 * timestamps, then POSTs to {@code /cassettes/topics/{topic}/replay-to-topic}.
 * The {@link com.joxette.replay.ReplayEngine} reads the cassette through
 * {@link com.joxette.replay.sink.kafka.KafkaRecordSink} and produces each
 * record to a pre-created target topic on the Testcontainers Kafka broker.
 *
 * <p>The test validates:
 * <ul>
 *   <li>all three messages arrive at the target topic,</li>
 *   <li>their values/keys match what was stored in the cassette,</li>
 *   <li>their order matches the original kafka-timestamp order,</li>
 *   <li>the inter-message wall-clock gap reflects the requested speed
 *       multiplier (original gaps were 300 ms; at {@code speed=2.0} the
 *       replayed gap must be roughly 150 ms).</li>
 * </ul>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class ReplayToTopicIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:4.0.2"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("joxette.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @LocalServerPort
    int port;

    @Autowired
    Connection duckDB;

    private RestTemplate restTemplate;
    private String baseUrl;

    private static final String SOURCE_TOPIC = "replay.source.events";
    private static final String TARGET_TOPIC = "replay.target.events";

    @BeforeEach
    void setUp() throws Exception {
        restTemplate = new RestTemplate();
        baseUrl = "http://localhost:" + port;

        DuckDBTestSupport.createGeneralCassetteTable(duckDB, SOURCE_TOPIC);
        try (Statement st = duckDB.createStatement()) {
            st.execute("DELETE FROM lake.main.general_" +
                       SOURCE_TOPIC.toLowerCase().replaceAll("[^a-z0-9_]", "_"));
        }
    }

    @Test
    void replayTopicToTopic_preservesOrderKeysValuesAndHonoursSpeed() throws Exception {
        createKafkaTopic(TARGET_TOPIC, 1);

        Instant t0 = Instant.parse("2024-10-01T10:00:00Z");
        Instant t1 = t0.plusMillis(300);
        Instant t2 = t0.plusMillis(600);
        Instant recordedAt = Instant.now();

        DuckDBTestSupport.insertCassetteRow(duckDB, SOURCE_TOPIC, 0, 0L, t0, recordedAt,
                "k0", "hello".getBytes(StandardCharsets.UTF_8));
        DuckDBTestSupport.insertCassetteRow(duckDB, SOURCE_TOPIC, 0, 1L, t1, recordedAt,
                "k1", "world".getBytes(StandardCharsets.UTF_8));
        DuckDBTestSupport.insertCassetteRow(duckDB, SOURCE_TOPIC, 0, 2L, t2, recordedAt,
                "k2", "!".getBytes(StandardCharsets.UTF_8));

        ReplayToTopicRequest body = new ReplayToTopicRequest(
                TARGET_TOPIC, null, null, null, null, null, null);

        double speed = 2.0;
        // Expected wall-clock gap between produced messages ≈ 300ms / 2 = 150ms.

        long replayStart = System.currentTimeMillis();
        ResponseEntity<ReplayProgress> response = restTemplate.postForEntity(
                baseUrl + "/cassettes/topics/" + SOURCE_TOPIC + "/replay-to-topic?speed=" + speed,
                body, ReplayProgress.class);
        long replayDurationMs = System.currentTimeMillis() - replayStart;

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        ReplayProgress progress = response.getBody();
        assertThat(progress).isNotNull();
        assertThat(progress.status()).isEqualTo("completed");
        assertThat(progress.sentCount()).isEqualTo(3);
        assertThat(progress.errorCount()).isZero();

        // Total sleep the engine issues = gap(t0→t1)/speed + gap(t1→t2)/speed
        //                               = 150 + 150 = 300ms (approx).
        // Allow a generous upper bound to absorb Kafka send latency.
        assertThat(replayDurationMs).isGreaterThanOrEqualTo(250L);
        assertThat(replayDurationMs).isLessThan(5_000L);

        // Verify messages landed on target topic in the correct order with the
        // original keys, values and timestamps.
        List<ConsumerRecord<String, byte[]>> consumed = consumeTarget(TARGET_TOPIC, 3,
                Duration.ofSeconds(15));

        assertThat(consumed).hasSize(3);
        assertThat(consumed.get(0).key()).isEqualTo("k0");
        assertThat(new String(consumed.get(0).value(), StandardCharsets.UTF_8)).isEqualTo("hello");
        assertThat(consumed.get(0).timestamp()).isEqualTo(t0.toEpochMilli());

        assertThat(consumed.get(1).key()).isEqualTo("k1");
        assertThat(new String(consumed.get(1).value(), StandardCharsets.UTF_8)).isEqualTo("world");
        assertThat(consumed.get(1).timestamp()).isEqualTo(t1.toEpochMilli());

        assertThat(consumed.get(2).key()).isEqualTo("k2");
        assertThat(new String(consumed.get(2).value(), StandardCharsets.UTF_8)).isEqualTo("!");
        assertThat(consumed.get(2).timestamp()).isEqualTo(t2.toEpochMilli());

        // Sanity: kafka-timestamp order matches iteration order.
        assertThat(consumed.get(0).timestamp()).isLessThan(consumed.get(1).timestamp());
        assertThat(consumed.get(1).timestamp()).isLessThan(consumed.get(2).timestamp());

        // Base64url sanity: ReplayEngine decoded what the cassette encoded — if
        // the decode was broken we'd see the literal base64 payload on the wire
        // instead of the original UTF-8 bytes. Guard against that regression.
        String encoded = Base64.getUrlEncoder().withoutPadding()
                .encodeToString("hello".getBytes(StandardCharsets.UTF_8));
        assertThat(new String(consumed.get(0).value(), StandardCharsets.UTF_8))
                .isNotEqualTo(encoded);
    }

    // -------------------------------------------------------------------------
    // Kafka helpers
    // -------------------------------------------------------------------------

    private static void createKafkaTopic(String topic, int partitions) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);
        }
    }

    private static List<ConsumerRecord<String, byte[]>> consumeTarget(
            String topic, int expected, Duration timeout) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "replay-to-topic-it-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        List<ConsumerRecord<String, byte[]>> collected = new ArrayList<>();
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (collected.size() < expected && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, byte[]> batch = consumer.poll(Duration.ofMillis(500));
                batch.forEach(collected::add);
            }
        }
        return collected;
    }
}
