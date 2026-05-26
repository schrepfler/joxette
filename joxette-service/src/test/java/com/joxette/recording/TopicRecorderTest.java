package com.joxette.recording;

import com.joxette.config.JoxetteProperties;
import com.softwaremill.jox.kafka.ConsumerSettings;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
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
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.joxette.management.ConfigRepository;
import com.joxette.management.TopicConfig;
import com.joxette.replay.EntityIdExtractor;
import com.joxette.replay.MessageRouter;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test: records Kafka messages through {@link TopicRecorder} +
 * {@link CassetteBatchWriter} into an in-memory DuckDB database.
 *
 * <p>Messages are produced to a real Kafka broker supplied by Testcontainers.
 * The recorder is given {@code auto.offset.reset=earliest} so it captures
 * messages that were published before the consumer subscribed.
 *
 * <p>The batch writer creates its own per-topic DuckLake table:
 * {@code lake.main.general_<normalized_topic>}.
 */
@Testcontainers
class TopicRecorderTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:4.0.2"));

    private static final String TOPIC = "recorder.test.events";
    private static final String SANITIZED = "recorder_test_events";
    // CassetteBatchWriter writes to lake.main.general_{normalized_topic}
    private static final String CASSETTE_TABLE = "lake.main.general_" + SANITIZED;

    private Connection duckDB;
    private DuckLakeWriteChannel writeChannel;
    private TopicRecorder recorder;
    private Thread recorderThread;
    /** Routes all messages to the general cassette (no entity routing). */
    private MessageRouter generalRouter;
    /** No-op registry — not tested here. */
    private com.joxette.replay.KnownEntitiesRepository noopEntities;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DriverManager.getConnection("jdbc:duckdb:");
        // Attach a secondary in-memory DB as 'lake' so CassetteBatchWriter can resolve
        // three-part names (lake.main.general_*).
        try (java.sql.Statement st = duckDB.createStatement()) {
            st.execute("ATTACH ':memory:' AS lake");
        }
        // Pre-create the cassette tables that CassetteBatchWriter will INSERT into.
        // SchemaManager does this in production; here we replicate it for the test.
        com.joxette.support.DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC);
        // Also create the multi-partition topic table used by recorder_batchesMultiplePartitions
        com.joxette.support.DuckDBTestSupport.createGeneralCassetteTable(duckDB, "recorder.multi.test");

        // Build a minimal schema for config tables so ConfigRepository can be constructed
        com.joxette.support.DuckDBTestSupport.initSchema(duckDB);

        // Seed both test topics as mode=general so MessageRouter routes to the general cassette
        try (java.sql.PreparedStatement ps = duckDB.prepareStatement(
                "INSERT INTO topic_configs (topic, mode) VALUES (?, 'general') ON CONFLICT DO NOTHING")) {
            ps.setString(1, TOPIC);
            ps.addBatch();
            ps.setString(1, "recorder.multi.test");
            ps.addBatch();
            ps.executeBatch();
        }

        JoxetteProperties props = new JoxetteProperties();
        writeChannel = new DuckLakeWriteChannel(duckDB, props, new CassetteRecordingBus(props));
        writeChannel.start();

        ConfigRepository configRepo = new ConfigRepository(duckDB, props);
        generalRouter  = new MessageRouter(configRepo, new EntityIdExtractor(), new com.joxette.config.InstanceRoles());
        noopEntities   = new com.joxette.replay.KnownEntitiesRepository(
                org.jooq.impl.DSL.using(duckDB, org.jooq.SQLDialect.DUCKDB));

        createKafkaTopic(TOPIC, 1);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (recorder != null) recorder.stop();
        if (recorderThread != null) recorderThread.join(5_000);
        if (writeChannel != null) writeChannel.stop();
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

        recorder = new TopicRecorder(TOPIC, consumerSettings(), writeChannel, 100, 200, generalRouter, noopEntities, "earliest");
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
                     "SELECT kafka_partition, kafka_offset, kafka_key, kafka_value FROM " + CASSETTE_TABLE
                             + " ORDER BY kafka_offset")) {
            for (int i = 0; i < msgCount; i++) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("kafka_key")).isEqualTo("key-" + i);
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

        recorder = new TopicRecorder(TOPIC, consumerSettings(), writeChannel, 10, 200, generalRouter, noopEntities, "earliest");
        recorderThread = Thread.ofVirtual().name("test-recorder-hdr").start(() -> {
            try { recorder.run(); } catch (Exception ignored) {}
        });

        awaitRowCount(CASSETTE_TABLE, 1, Duration.ofSeconds(10));

        recorder.stop();
        recorderThread.join(5_000);

        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT kafka_key, headers FROM " + CASSETTE_TABLE)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("kafka_key")).isNull();
            // Headers are stored as STRUCT(key VARCHAR, value VARCHAR)[] by CassetteBatchWriter.
            String headers = rs.getString("headers");
            assertThat(headers).contains("x-source");
        }
    }

    /**
     * Feeds one message with a known key, value, and two headers through
     * {@link TopicRecorder} and asserts that every column in the general
     * cassette table is populated with the correct value.
     *
     * <p>This is the primary "batch → DuckDB write" correctness test.  It
     * covers fields that the other tests only touch partially:
     * <ul>
     *   <li>{@code recorded_at}   — set by the writer, must be after the test started</li>
     *   <li>{@code kafka_offset}  — 0 for the first message on a fresh topic</li>
     *   <li>{@code kafka_partition} — 0 (single-partition topic)</li>
     *   <li>{@code kafka_timestamp} — Kafka producer timestamp, must match exactly</li>
     *   <li>{@code kafka_key}     — exact string key</li>
     *   <li>{@code kafka_value}   — exact raw bytes (BLOB)</li>
     *   <li>{@code kafka_value_str} — UTF-8 string view of the value</li>
     *   <li>{@code metadata}      — NULL (not set by the writer)</li>
     *   <li>{@code headers}       — both key <em>and</em> value of every header</li>
     * </ul>
     */
    @Test
    void recorder_writesAllFieldValuesCorrectlyIncludingHeaders() throws Exception {
        String msgKey   = "order-99";
        byte[] msgValue = "{\"order_id\":\"99\"}".getBytes(StandardCharsets.UTF_8);
        long   producedAtMs;
        try (KafkaProducer<String, byte[]> producer = newProducer()) {
            ProducerRecord<String, byte[]> rec = new ProducerRecord<>(TOPIC, msgKey, msgValue);
            rec.headers().add("x-correlation-id", "corr-42".getBytes(StandardCharsets.UTF_8));
            rec.headers().add("event-type",        "OrderCreated".getBytes(StandardCharsets.UTF_8));
            producedAtMs = producer.send(rec).get().timestamp();
        }

        // Capture time just before the recorder writes so we can bound recorded_at.
        java.time.Instant beforeRecord = java.time.Instant.now();

        recorder = new TopicRecorder(TOPIC, consumerSettings(), writeChannel, 100, 200, generalRouter, noopEntities, "earliest");
        recorderThread = Thread.ofVirtual().name("test-recorder-fields").start(() -> {
            try { recorder.run(); } catch (Exception ignored) {}
        });

        awaitRowCount(CASSETTE_TABLE, 1, Duration.ofSeconds(10));
        recorder.stop();
        recorderThread.join(5_000);

        // Query all columns in declaration order so the positional index used for
        // the BLOB column (kafka_value) is predictable.
        // Positions: 1=recorded_at, 2=kafka_offset, 3=kafka_partition,
        //            4=kafka_timestamp, 5=kafka_key, 6=kafka_value (BLOB),
        //            7=kafka_value_str, 8=metadata, 9=headers
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT recorded_at, kafka_offset, kafka_partition, kafka_timestamp," +
                     "       kafka_key, kafka_value, kafka_value_str, metadata, headers" +
                     " FROM " + CASSETTE_TABLE)) {

            assertThat(rs.next()).isTrue();

            // recorded_at is set by CassetteBatchWriter to Instant.now() — must be
            // at or after the moment we captured before launching the recorder.
            assertThat(rs.getTimestamp("recorded_at").toInstant())
                    .isAfterOrEqualTo(beforeRecord.minusMillis(500));

            // First (and only) message on a fresh single-partition topic.
            assertThat(rs.getInt("kafka_partition")).isEqualTo(0);
            assertThat(rs.getLong("kafka_offset")).isEqualTo(0L);

            // Kafka stores the producer timestamp; it must survive the write round-trip.
            assertThat(rs.getTimestamp("kafka_timestamp").getTime()).isEqualTo(producedAtMs);

            assertThat(rs.getString("kafka_key")).isEqualTo(msgKey);

            // kafka_value is BLOB — DuckDB JDBC 1.5.x does not support getBytes(String),
            // so access by positional index (column 6 in the SELECT above).
            assertThat(rs.getBytes(6)).isEqualTo(msgValue);

            assertThat(rs.getString("kafka_value_str"))
                    .isEqualTo(new String(msgValue, StandardCharsets.UTF_8));

            assertThat(rs.getString("metadata")).isNull();

            // Headers are stored as STRUCT(key VARCHAR, value VARCHAR)[].
            // Verify both the key *and* the value for every header sent.
            String headers = rs.getString("headers");
            assertThat(headers)
                    .contains("x-correlation-id").contains("corr-42")
                    .contains("event-type").contains("OrderCreated");

            assertThat(rs.next()).isFalse();
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

        TopicRecorder multiRecorder = new TopicRecorder(multiTopic, consumerSettings(), writeChannel, 20, 300, generalRouter, noopEntities, "earliest");
        Thread multiThread = Thread.ofVirtual().name("test-multi-recorder").start(() -> {
            try { multiRecorder.run(); } catch (Exception ignored) {}
        });

        String table = "lake.main.general_" + multiSanitized;
        awaitRowCount(table, total, Duration.ofSeconds(15));

        multiRecorder.stop();
        multiThread.join(5_000);

        // All 3 partitions should have data.
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT COUNT(DISTINCT kafka_partition) AS p FROM " + table)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong("p")).isEqualTo(3);
        }
    }

    /**
     * Verifies that {@code startFrom="earliest"} causes {@link TopicRecorder} to seek
     * to the beginning of each partition on assignment, picking up messages that were
     * published before the consumer subscribed.
     *
     * <p>The base consumer properties use {@code auto.offset.reset=latest} to prove
     * that it is TopicRecorder's explicit {@code seekToBeginning()} — not the reset
     * policy — that captures pre-published messages.
     */
    @Test
    void recorder_startsFromEarliest_picksUpPrePublishedMessages() throws Exception {
        int msgCount = 4;
        try (KafkaProducer<String, byte[]> producer = newProducer()) {
            for (int i = 0; i < msgCount; i++) {
                producer.send(new ProducerRecord<>(TOPIC, "early-" + i,
                        ("val-" + i).getBytes(StandardCharsets.UTF_8))).get();
            }
        }

        // Base props deliberately use "latest" — TopicRecorder must override via seek.
        recorder = new TopicRecorder(TOPIC, consumerSettingsWithLatestDefault(), writeChannel,
                100, 500, generalRouter, noopEntities, "earliest");
        recorderThread = Thread.ofVirtual().name("test-recorder-earliest").start(() -> {
            try { recorder.run(); } catch (Exception ignored) {}
        });

        awaitRowCount(CASSETTE_TABLE, msgCount, Duration.ofSeconds(15));

        recorder.stop();
        recorderThread.join(5_000);

        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + CASSETTE_TABLE)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).isEqualTo(msgCount);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private ConsumerSettings<String, byte[]> consumerSettings() {
        // Use earliest so messages published before subscription are captured.
        return ConsumerSettings.defaults("joxette-test")
                .bootstrapServers(kafka.getBootstrapServers())
                .keyDeserializer(new StringDeserializer())
                .valueDeserializer(new ByteArrayDeserializer())
                .autoOffsetReset(ConsumerSettings.AutoOffsetReset.EARLIEST)
                .property("enable.auto.commit", "false");
    }

    /** Base consumer settings that default to {@code auto.offset.reset=latest}. */
    private ConsumerSettings<String, byte[]> consumerSettingsWithLatestDefault() {
        return ConsumerSettings.defaults("joxette-test")
                .bootstrapServers(kafka.getBootstrapServers())
                .keyDeserializer(new StringDeserializer())
                .valueDeserializer(new ByteArrayDeserializer())
                .autoOffsetReset(ConsumerSettings.AutoOffsetReset.LATEST)
                .property("enable.auto.commit", "false");
    }

    private KafkaProducer<String, byte[]> newProducer() {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"));
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
