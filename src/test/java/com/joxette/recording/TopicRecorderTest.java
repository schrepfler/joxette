package com.joxette.recording;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test: records Kafka messages through {@link TopicRecorder} +
 * {@link CassetteBatchWriter} into an in-memory DuckDB database.
 *
 * <p>Messages are produced to a real Kafka broker supplied by Testcontainers.
 * The recorder is given {@code auto.offset.reset=earliest} so it captures
 * messages that were published before the consumer subscribed.
 *
 * <p>The batch writer creates its own per-topic table:
 * {@code lake.cassette_<sanitized_topic>}.
 */
@Testcontainers
class TopicRecorderTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    private static final String TOPIC = "recorder.test.events";
    private static final String SANITIZED = "recorder_test_events";
    private static final String CASSETTE_TABLE = "lake.cassette_" + SANITIZED;

    private Connection duckDB;
    private TopicRecorder recorder;
    private Thread recorderThread;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DriverManager.getConnection("jdbc:duckdb:");
        createKafkaTopic(TOPIC, 1);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (recorder != null) recorder.stop();
        if (recorderThread != null) recorderThread.join(5_000);
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
        deleteKafkaTopic(TOPIC);
        deleteKafkaTopic("recorder.multi.test");
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    @Test
    void recorder_writesMessagesToCassetteTable() throws Exception {
        // Publish messages to Kafka before starting the recorder.
        // With auto.offset.reset=earliest the recorder will pick them all up.
        int msgCount = 5;
        try (KafkaProducer<String, byte[]> producer = newProducer()) {
            for (int i = 0; i < msgCount; i++) {
                producer.send(new ProducerRecord<>(TOPIC, "key-" + i,
                        ("{\"id\":" + i + "}").getBytes(StandardCharsets.UTF_8))).get();
            }
        }

        recorder = new TopicRecorder(TOPIC, consumerProps(), duckDB, 100, 200);
        recorderThread = Thread.ofVirtual().name("test-recorder").start(() -> {
            try {
                recorder.run();
            } catch (Exception e) {
                // expected on stop()
            }
        });

        // Poll DuckDB until all messages are written (up to 15 s).
        awaitRowCount(CASSETTE_TABLE, msgCount, Duration.ofSeconds(15));

        recorder.stop();
        recorderThread.join(5_000);

        // Verify all rows are present with correct content.
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT partition, \"offset\", key, value FROM " + CASSETTE_TABLE
                             + " ORDER BY \"offset\"")) {
            for (int i = 0; i < msgCount; i++) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("key")).isEqualTo("key-" + i);
                // Use positional index: DuckDB JDBC 1.5.x does not support getBytes(String) for BLOB
                byte[] value = rs.getBytes(4);
                assertThat(new String(value, StandardCharsets.UTF_8))
                        .isEqualTo("{\"id\":" + i + "}");
            }
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    void recorder_handlesHeadersAndNullKey() throws Exception {
        try (KafkaProducer<String, byte[]> producer = newProducer()) {
            ProducerRecord<String, byte[]> rec = new ProducerRecord<>(TOPIC, null,
                    "payload".getBytes(StandardCharsets.UTF_8));
            rec.headers().add("x-source", "test".getBytes(StandardCharsets.UTF_8));
            producer.send(rec).get();
        }

        recorder = new TopicRecorder(TOPIC, consumerProps(), duckDB, 10, 200);
        recorderThread = Thread.ofVirtual().name("test-recorder-hdr").start(() -> {
            try { recorder.run(); } catch (Exception ignored) {}
        });

        awaitRowCount(CASSETTE_TABLE, 1, Duration.ofSeconds(10));

        recorder.stop();
        recorderThread.join(5_000);

        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT key, headers FROM " + CASSETTE_TABLE)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("key")).isNull();
            // Headers are stored as JSON by CassetteBatchWriter.
            String headers = rs.getString("headers");
            assertThat(headers).contains("x-source");
        }
    }

    @Test
    void recorder_batchesMultiplePartitions() throws Exception {
        // Use a topic with 3 partitions to verify per-partition offset tracking.
        String multiTopic = "recorder.multi.test";
        String multiSanitized = "recorder_multi_test";
        createKafkaTopic(multiTopic, 3);

        int total = 12;
        try (KafkaProducer<String, byte[]> producer = newProducer()) {
            for (int i = 0; i < total; i++) {
                producer.send(new ProducerRecord<>(multiTopic, i % 3, "k" + i,
                        ("v" + i).getBytes(StandardCharsets.UTF_8))).get();
            }
        }

        TopicRecorder multiRecorder = new TopicRecorder(multiTopic, consumerProps(), duckDB, 20, 300);
        Thread multiThread = Thread.ofVirtual().name("test-multi-recorder").start(() -> {
            try { multiRecorder.run(); } catch (Exception ignored) {}
        });

        String table = "lake.cassette_" + multiSanitized;
        awaitRowCount(table, total, Duration.ofSeconds(15));

        multiRecorder.stop();
        multiThread.join(5_000);

        // All 3 partitions should have data.
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT COUNT(DISTINCT partition) AS p FROM " + table)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong("p")).isEqualTo(3);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // Use earliest so messages published before subscription are captured.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private KafkaProducer<String, byte[]> newProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    private void createKafkaTopic(String topic, int partitions) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            long deadline = System.currentTimeMillis() + 15_000;
            while (true) {
                try {
                    admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                            .all().get(10, TimeUnit.SECONDS);
                    return;
                } catch (ExecutionException e) {
                    // Kafka topic deletion is async; retry if topic is still being deleted.
                    if (e.getCause() != null
                            && e.getCause().getMessage() != null
                            && e.getCause().getMessage().contains("marked for deletion")
                            && System.currentTimeMillis() < deadline) {
                        Thread.sleep(300);
                    } else {
                        throw e;
                    }
                }
            }
        }
    }

    private void deleteKafkaTopic(String topic) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.deleteTopics(List.of(topic)).all().get(5, TimeUnit.SECONDS);
            // Wait until deletion completes (Kafka deletion is async)
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                var topics = admin.listTopics().names().get(5, TimeUnit.SECONDS);
                if (!topics.contains(topic)) return;
                Thread.sleep(200);
            }
        } catch (Exception ignored) {
            // topic may not exist — that's fine
        }
    }

    /**
     * Polls {@code table} until it contains at least {@code expected} rows or
     * {@code timeout} elapses, then asserts the count.
     */
    private void awaitRowCount(String table, long expected, Duration timeout) throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            try (Statement st = duckDB.createStatement()) {
                // Table may not exist yet if the recorder hasn't written its first batch.
                try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + table)) {
                    if (rs.next() && rs.getLong(1) >= expected) return;
                }
            } catch (SQLException ignored) {
                // table doesn't exist yet — keep waiting
            }
            Thread.sleep(100);
        }
        // Final assert to produce a clear failure message.
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + table)) {
            long actual = rs.next() ? rs.getLong(1) : 0;
            assertThat(actual)
                    .as("Expected %d rows in %s within %s", expected, table, timeout)
                    .isGreaterThanOrEqualTo(expected);
        }
    }
}
