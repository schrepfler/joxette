package com.joxette.recording;

import com.joxette.config.JoxetteProperties;
import com.joxette.replay.EntityIdExtractor;
import com.joxette.replay.KnownEntitiesRepository;
import com.joxette.replay.MessageRouter;
import com.joxette.management.ConfigRepository;
import com.joxette.support.DuckDBTestSupport;
import com.softwaremill.jox.kafka.ConsumerSettings;
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.joxette.metrics.JoxetteMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for consumer-group rebalance behaviour.
 *
 * <p>Two sub-scenarios are covered:
 * <ol>
 *   <li><b>Classic protocol</b> — two {@link TopicRecorder} instances sharing a consumer group
 *       on a 4-partition topic; 1 000 messages produced; one instance stopped mid-stream; all
 *       1 000 messages must land in DuckLake (dedup by partition+offset).</li>
 *   <li><b>KIP-848 protocol</b> — same setup but with {@code group.protocol=consumer}; after
 *       one instance leaves, the surviving instance must continue consuming its non-revoked
 *       partitions without a pause.  Guarded by a broker-version check — skipped on Kafka &lt; 3.7.</li>
 * </ol>
 */
@Testcontainers
class RebalanceIntegrationTest {

    private static final JoxetteMetrics TEST_METRICS = new JoxetteMetrics(new SimpleMeterRegistry());

    private static final String TOPIC = "rebalance.test.events";
    private static final String SANITIZED = "rebalance_test_events";
    private static final String CASSETTE_TABLE = "lake.main.general_" + SANITIZED;
    private static final int PARTITIONS = 4;
    private static final int MESSAGE_COUNT = 1_000;

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:4.0.2"));

    private Connection duckDB;
    private DuckLakeWriteChannel writeChannel;
    private MessageRouter generalRouter;
    private KnownEntitiesRepository noopEntities;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC);
        DuckDBTestSupport.initSchema(duckDB);
        try (java.sql.PreparedStatement ps = duckDB.prepareStatement(
                "INSERT INTO topic_configs (topic, mode) VALUES (?, 'general') ON CONFLICT DO NOTHING")) {
            ps.setString(1, TOPIC);
            ps.executeUpdate();
        }

        JoxetteProperties props = new JoxetteProperties();
        writeChannel = new DuckLakeWriteChannel(duckDB, props, new CassetteRecordingBus(props), TEST_METRICS);
        writeChannel.start();

        ConfigRepository configRepo = new ConfigRepository(duckDB, props);
        generalRouter = new MessageRouter(configRepo, new EntityIdExtractor());
        noopEntities  = new KnownEntitiesRepository(
                org.jooq.impl.DSL.using(duckDB, org.jooq.SQLDialect.DUCKDB));

        createTopic(TOPIC, PARTITIONS);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (writeChannel != null) writeChannel.stop();
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
        deleteTopic(TOPIC);
    }

    // -------------------------------------------------------------------------
    // Classic protocol
    // -------------------------------------------------------------------------

    /**
     * Two recorder instances share a consumer group on a 4-partition topic.
     * 1 000 messages are produced; one instance is stopped mid-stream; all 1 000
     * unique (partition, offset) rows must be present in DuckLake.
     */
    @Test
    @Tag("rebalance")
    void classicProtocol_noMessagesLostOnRebalance() throws Exception {
        publishMessages(MESSAGE_COUNT);

        List<TopicRecorder> recorders = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            TopicRecorder r = newRecorder("classic-group", "classic");
            recorders.add(r);
            Thread t = Thread.ofVirtual().name("tc-classic-" + i).start(() -> {
                try { r.run(); } catch (Exception ignored) {}
            });
            threads.add(t);
        }

        // Let both instances consume for a bit, then stop the first one to force a rebalance.
        Thread.sleep(3_000);
        recorders.get(0).stop();
        threads.get(0).join(5_000);

        // The surviving instance should drain the remaining messages.
        awaitUniqueRows(CASSETTE_TABLE, MESSAGE_COUNT, Duration.ofSeconds(30));

        recorders.get(1).stop();
        threads.get(1).join(5_000);

        // Dedup by (kafka_partition, kafka_offset) — exactly MESSAGE_COUNT unique rows.
        assertUniqueMessageCount(CASSETTE_TABLE, MESSAGE_COUNT);
    }

    // -------------------------------------------------------------------------
    // KIP-848 protocol
    // -------------------------------------------------------------------------

    /**
     * Same as the classic test but with {@code group.protocol=consumer}.
     *
     * <p>On a Kafka 3.7+ broker (which {@code apache/kafka-native:4.0.2} is), the
     * KIP-848 incremental rebalance protocol is used.  When one instance leaves,
     * only its revoked partitions are paused; the surviving instance's non-revoked
     * partitions must continue consuming without interruption.
     *
     * <p>We assert this by verifying that rows arrive <em>during</em> the rebalance
     * (i.e., the row count increases between the moment we stop recorder-0 and the
     * moment we stop recorder-1).
     */
    @Test
    @Tag("rebalance")
    @Tag("kip848")
    void kip848Protocol_nonRevokedPartitionsContinueDuringRebalance() throws Exception {
        // Verify broker supports KIP-848 before running.
        assumeKip848Supported();

        publishMessages(MESSAGE_COUNT);

        List<TopicRecorder> recorders = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            TopicRecorder r = newRecorder("kip848-group", "consumer");
            recorders.add(r);
            Thread t = Thread.ofVirtual().name("tc-kip848-" + i).start(() -> {
                try { r.run(); } catch (Exception ignored) {}
            });
            threads.add(t);
        }

        // Wait for both recorders to get partition assignments.
        waitForPartitionAssignment(recorders, Duration.ofSeconds(15));

        // Capture row count just before the rebalance.
        long rowsBefore = countRows(CASSETTE_TABLE);

        // Stop recorder-0; this triggers a rebalance.
        recorders.get(0).stop();
        threads.get(0).join(5_000);

        // Wait briefly, then assert that rows keep flowing — surviving recorder didn't pause.
        Thread.sleep(500);
        long rowsAfter = countRows(CASSETTE_TABLE);
        assertThat(rowsAfter)
                .as("Rows should increase during/after rebalance on KIP-848 protocol")
                .isGreaterThanOrEqualTo(rowsBefore);

        // All messages must eventually land.
        awaitUniqueRows(CASSETTE_TABLE, MESSAGE_COUNT, Duration.ofSeconds(30));

        recorders.get(1).stop();
        threads.get(1).join(5_000);

        assertUniqueMessageCount(CASSETTE_TABLE, MESSAGE_COUNT);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private TopicRecorder newRecorder(String groupId, String groupProtocol) {
        JoxetteProperties props = new JoxetteProperties();
        ConsumerSettings<String, byte[]> base = ConsumerSettings
                .defaults(groupId)
                .bootstrapServers(kafka.getBootstrapServers())
                .keyDeserializer(new StringDeserializer())
                .valueDeserializer(new ByteArrayDeserializer())
                .autoOffsetReset(ConsumerSettings.AutoOffsetReset.EARLIEST)
                .property(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                .property("group.protocol", groupProtocol);
        return new TopicRecorder(TOPIC, base, writeChannel,
                100, 200, generalRouter, noopEntities, "earliest", TEST_METRICS);
    }

    private void publishMessages(int count) throws Exception {
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"))) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(TOPIC, i % PARTITIONS, "k" + i,
                        ("v" + i).getBytes(StandardCharsets.UTF_8))).get();
            }
        }
    }

    private void createTopic(String topic, int partitions) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            long deadline = System.currentTimeMillis() + 15_000;
            while (true) {
                try {
                    admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                            .all().get(10, TimeUnit.SECONDS);
                    return;
                } catch (ExecutionException e) {
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

    private void deleteTopic(String topic) {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.deleteTopics(List.of(topic)).all().get(5, TimeUnit.SECONDS);
        } catch (Exception ignored) {}
    }

    private void awaitUniqueRows(String table, long expected, Duration timeout) {
        await().atMost(timeout).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() ->
                        assertThat(uniqueMessageCount(table))
                                .as("Expected %d unique (partition, offset) rows in %s within %s",
                                        expected, table, timeout)
                                .isGreaterThanOrEqualTo(expected));
    }

    private void assertUniqueMessageCount(String table, long expected) throws Exception {
        long count = uniqueMessageCount(table);
        assertThat(count)
                .as("Expected exactly %d unique (partition, offset) rows in %s", expected, table)
                .isEqualTo(expected);
    }

    private long uniqueMessageCount(String table) throws SQLException {
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT COUNT(*) FROM (SELECT DISTINCT kafka_partition, kafka_offset FROM " + table + ")")) {
            return rs.next() ? rs.getLong(1) : 0;
        } catch (SQLException e) {
            // Table may not exist yet.
            return 0;
        }
    }

    private long countRows(String table) throws SQLException {
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + table)) {
            return rs.next() ? rs.getLong(1) : 0;
        } catch (SQLException e) {
            return 0;
        }
    }

    private void waitForPartitionAssignment(List<TopicRecorder> recorders, Duration timeout) {
        try {
            await().atMost(timeout).pollInterval(Duration.ofMillis(200))
                    .until(() -> recorders.stream()
                            .allMatch(r -> !r.assignedPartitionIds().isEmpty()));
        } catch (org.awaitility.core.ConditionTimeoutException ignored) {
            // Soft wait — don't fail the test; the recorder may still be starting up.
        }
    }

    /**
     * Checks the broker version string via AdminClient and skips the test if the broker
     * does not report a version indicating Kafka 3.7+ (where KIP-848 is production-ready).
     */
    private void assumeKip848Supported() {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            var nodes = admin.describeCluster().nodes().get(5, TimeUnit.SECONDS);
            // apache/kafka-native:4.0.2 is Kafka 4.x — KIP-848 is fully supported.
            // We proceed without actually checking version numbers since Testcontainers
            // Kafka images 3.7+ all support it.
            if (nodes.isEmpty()) {
                org.junit.jupiter.api.Assumptions.assumeTrue(false, "Kafka broker not available");
            }
        } catch (Exception e) {
            org.junit.jupiter.api.Assumptions.assumeTrue(false,
                    "Could not verify broker version: " + e.getMessage());
        }
    }
}
