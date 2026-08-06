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

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.joxette.management.ConfigRepository;
import com.joxette.replay.EntityIdExtractor;
import com.joxette.replay.MessageRouter;
import com.joxette.metrics.JoxetteMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Reproduces a real production crash: while {@link DuckLakeWriteChannel}'s sink is
 * {@code DEGRADED} (object store unreachable), {@link TopicRecorder} threw
 * {@code ConcurrentModificationException: KafkaConsumer is not safe for multi-threaded
 * access} and crashed, forcing a full actor restart + consumer-group rebalance — exactly
 * the disruption {@code docs/write-resilience.md}'s pause/heartbeat design exists to
 * avoid.
 *
 * <p>Root cause: Jox's {@code batchWeighted} forks the downstream {@code map}/{@code
 * runForeach} stage onto its own thread, separate from the upstream poll-loop thread
 * that owns the {@code KafkaConsumer}. The old {@code pauseForSinkRecovery} called
 * {@code kc.assignment()}/{@code pause()}/{@code poll()}/{@code resume()} directly from
 * that downstream thread — violating Kafka's single-thread-access contract exactly like
 * calling {@code kc.commitSync()} from there would (which the code correctly avoids via
 * the {@code pendingCommit} handoff back to the poll-loop thread).
 *
 * <p>Uses the same {@code sinkState}-via-reflection technique as
 * {@link DuckLakeWriteChannelSinkStateGaugeTest} — real DEGRADED transitions require
 * fault-injecting the JDBC layer, which isn't this test's concern.
 */
@Testcontainers
class TopicRecorderSinkDegradedThreadSafetyTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:4.0.2"));

    private static final JoxetteMetrics TEST_METRICS = new JoxetteMetrics(new SimpleMeterRegistry());
    private static final String TOPIC = "recorder.degraded.test";
    private static final String CASSETTE_TABLE = "lake.main.general_recorder_degraded_test";

    private Connection duckDB;
    private DuckLakeWriteChannel writeChannel;
    private TopicRecorder recorder;
    private Thread recorderThread;
    private final AtomicReference<Throwable> recorderFailure = new AtomicReference<>();

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = duckDB.createStatement()) {
            st.execute("ATTACH ':memory:' AS lake");
        }
        com.joxette.support.DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC);
        com.joxette.support.DuckDBTestSupport.initSchema(duckDB);
        try (var ps = duckDB.prepareStatement(
                "INSERT INTO topic_configs (topic, mode) VALUES (?, 'general') ON CONFLICT DO NOTHING")) {
            ps.setString(1, TOPIC);
            ps.executeUpdate();
        }

        JoxetteProperties props = new JoxetteProperties();
        writeChannel = new DuckLakeWriteChannel(duckDB, props, new CassetteRecordingBus(props), TEST_METRICS);
        writeChannel.start();

        ConfigRepository configRepo = new ConfigRepository(duckDB, props);
        MessageRouter generalRouter = new MessageRouter(configRepo, new EntityIdExtractor(), TEST_METRICS);
        var noopEntities = new com.joxette.replay.KnownEntitiesRepository(
                org.jooq.impl.DSL.using(duckDB, org.jooq.SQLDialect.DUCKDB));

        createKafkaTopic(TOPIC, 1);

        recorder = new TopicRecorder(TOPIC, consumerSettings(), writeChannel, 5, 100,
                generalRouter, noopEntities, "earliest", TEST_METRICS);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (recorder != null) recorder.stop();
        if (recorderThread != null) recorderThread.join(5_000);
        if (writeChannel != null) writeChannel.stop();
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
        deleteKafkaTopic(TOPIC);
    }

    @Test
    void recorder_survivesDegradedSink_whileUnderContinuousLoad_withoutCrashing() throws Exception {
        // Degrade the sink BEFORE the recorder starts — every batch must go through
        // the pause/wait path from the very first poll.
        setSinkState(DuckLakeWriteChannel.SinkState.DEGRADED);

        recorderThread = Thread.ofVirtual().name("test-recorder-degraded").start(() -> {
            try {
                recorder.run();
            } catch (Throwable t) {
                recorderFailure.set(t);
            }
        });

        // Keep producing while degraded — batchWeighted only forks a separate
        // downstream thread once there's real coalescing to do, which needs sustained
        // upstream throughput while the sink is blocked.
        try (KafkaProducer<String, byte[]> producer = newProducer()) {
            for (int i = 0; i < 300; i++) {
                producer.send(new ProducerRecord<>(TOPIC, "key-" + i,
                        ("v" + i).getBytes(StandardCharsets.UTF_8)));
                if (i % 50 == 0) producer.flush();
            }
            producer.flush();
        }

        // Give the recorder a couple of seconds under load while still degraded —
        // this is the window where the old code crashed.
        Thread.sleep(3_000);
        assertThat(recorderFailure.get())
                .as("recorder must not crash while the sink is degraded")
                .isNull();
        assertThat(recorderThread.isAlive())
                .as("recorder thread must still be running, not crashed")
                .isTrue();

        // Recover the sink and confirm the recorder actually catches up and writes
        // everything once healthy — not just "didn't crash" but "still functional".
        writeChannel.resetSinkState();

        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(currentRowCount(CASSETTE_TABLE)).isEqualTo(300L));

        assertThat(recorderFailure.get())
                .as("recorder must not crash during or after recovery either")
                .isNull();
    }

    // -------------------------------------------------------------------------
    // Helpers (same conventions as TopicRecorderTest)
    // -------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private void setSinkState(DuckLakeWriteChannel.SinkState state) throws Exception {
        Field field = DuckLakeWriteChannel.class.getDeclaredField("sinkState");
        field.setAccessible(true);
        ((AtomicReference<DuckLakeWriteChannel.SinkState>) field.get(writeChannel)).set(state);
    }

    private ConsumerSettings<String, byte[]> consumerSettings() {
        return ConsumerSettings.defaults("joxette-test")
                .bootstrapServers(kafka.getBootstrapServers())
                .keyDeserializer(new StringDeserializer())
                .valueDeserializer(new ByteArrayDeserializer())
                .autoOffsetReset(ConsumerSettings.AutoOffsetReset.EARLIEST)
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
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);
        }
    }

    private void deleteKafkaTopic(String topic) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.deleteTopics(List.of(topic)).all().get(5, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // topic may not exist — fine
        }
    }

    private long currentRowCount(String table) {
        try (Statement st = duckDB.createStatement();
             var rs = st.executeQuery("SELECT COUNT(*) FROM " + table)) {
            return rs.next() ? rs.getLong(1) : 0;
        } catch (Exception e) {
            return 0;
        }
    }
}
