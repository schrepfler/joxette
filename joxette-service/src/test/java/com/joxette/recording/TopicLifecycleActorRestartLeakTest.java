package com.joxette.recording;

import com.joxette.config.BrokerConnectionFactory;
import com.joxette.config.JoxetteProperties;
import com.joxette.management.BrokerRepository;
import com.joxette.management.ConfigRepository;
import com.joxette.metrics.JoxetteMetrics;
import com.joxette.replay.EntityIdExtractor;
import com.joxette.replay.KnownEntitiesRepository;
import com.joxette.replay.MessageRouter;
import com.joxette.support.DuckDBTestSupport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.testkit.typed.javadsl.TestProbe;
import org.apache.pekko.actor.typed.ActorRef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves that a failure on ONE partition's recorder does not leak the OTHER,
 * healthy recorders when {@link TopicLifecycleActor} restarts.
 *
 * <p>Only partition 0 ever receives a message; the write for that message
 * fails with a non-retryable DuckDB "Catalog Error" because the general
 * cassette table is deliberately never created. This forces exactly one
 * {@code RecorderFailed}, which makes the whole actor throw and restart.
 * Partitions 1 and 2 never write anything and stay healthy the entire time —
 * they are exactly the siblings that leaked before this fix.
 */
@Testcontainers
class TopicLifecycleActorRestartLeakTest {

    private static final JoxetteMetrics TEST_METRICS = new JoxetteMetrics(new SimpleMeterRegistry());
    private static final String TOPIC = "lifecycle.leak.test.events";
    private static final int PARTITIONS = 3;

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:4.0.2"));

    private ActorTestKit kit;
    private Connection duckDB;
    private DuckLakeWriteChannel writeChannel;
    private JoxetteProperties props;

    @BeforeEach
    void setUp() throws Exception {
        kit = ActorTestKit.create();
        duckDB = DuckDBTestSupport.newConnection();
        // Deliberately do NOT create lake.main.general_lifecycle_leak_test_events —
        // any write attempt fails with a non-retryable "Catalog Error".
        try (PreparedStatement ps = duckDB.prepareStatement(
                "INSERT INTO topic_configs (topic, mode) VALUES (?, 'general') ON CONFLICT DO NOTHING")) {
            ps.setString(1, TOPIC);
            ps.executeUpdate();
        }

        props = new JoxetteProperties();
        props.getKafka().setBootstrapServers(kafka.getBootstrapServers());
        props.getRecording().setRetryInitialIntervalMs(200);
        props.getRecording().setRetryMaxIntervalMs(500);
        props.getRecording().setBatchSize(10);
        props.getRecording().setBatchTimeoutMs(100);

        writeChannel = new DuckLakeWriteChannel(duckDB, props, new CassetteRecordingBus(props), TEST_METRICS);
        writeChannel.start();

        createTopic(TOPIC, PARTITIONS);
    }

    @AfterEach
    void tearDown() throws Exception {
        kit.shutdownTestKit();
        if (writeChannel != null) writeChannel.stop();
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
        deleteTopic(TOPIC);
    }

    @Test
    void restartAfterSinkFailure_keepsActorAliveInsteadOfPrematurelyStopping() throws Exception {
        // Unlike the RecorderFailed scenario above (one poisoned partition, two
        // healthy siblings), the SinkFailed path is triggered when the DRAIN VT
        // itself escalates a write failure to FAILED — at that point ALL recorders
        // are still healthy (none of them individually failed). PreRestart then
        // stops all SINKFAIL_PARTITIONS of them, producing SINKFAIL_PARTITIONS
        // stale RecorderFinished messages against the new generation — exactly
        // enough to hit the pre-fix `finishedCount[0] >= recorders.size()` guard
        // in recording() and stop the actor outright.
        //
        // kafka_key is declared INTEGER here (schema mismatch vs. the VARCHAR key
        // TopicRecorder actually binds) so every write throws DuckDB's
        // "Conversion Error: Could not convert string '...' to INT32" — a message
        // with no recognized non-transient prefix, so DuckDbErrors.isTransient()
        // classifies it as transient (see docs/write-resilience.md, "denylist-first
        // strategy"). With writeRetryMaxAttempts=1, the very first failed write
        // escalates straight from HEALTHY to FAILED and fires the SinkFailed path.
        try (Statement st = duckDB.createStatement()) {
            st.execute(String.format("""
                    CREATE TABLE lake.main.general_%s (
                        recorded_at     TIMESTAMPTZ NOT NULL,
                        kafka_offset    BIGINT      NOT NULL,
                        kafka_partition INTEGER     NOT NULL,
                        kafka_timestamp TIMESTAMPTZ NOT NULL,
                        kafka_key       INTEGER,
                        kafka_value     BLOB,
                        metadata        VARCHAR,
                        headers         STRUCT(key VARCHAR, value VARCHAR)[],
                        message_type    VARCHAR
                    )""", SINKFAIL_SANITIZED));
        }
        try (PreparedStatement ps = duckDB.prepareStatement(
                "INSERT INTO topic_configs (topic, mode) VALUES (?, 'general') ON CONFLICT DO NOTHING")) {
            ps.setString(1, SINKFAIL_TOPIC);
            ps.executeUpdate();
        }
        createTopic(SINKFAIL_TOPIC, SINKFAIL_PARTITIONS);

        // A dedicated writeChannel (NOT the shared field from setUp()) so
        // writeRetryMaxAttempts=1 takes effect: DuckLakeWriteChannel reads it once,
        // at construction time, into a final field.
        JoxetteProperties sinkFailProps = new JoxetteProperties();
        sinkFailProps.getKafka().setBootstrapServers(kafka.getBootstrapServers());
        sinkFailProps.getRecording().setRetryInitialIntervalMs(200);
        sinkFailProps.getRecording().setRetryMaxIntervalMs(500);
        sinkFailProps.getRecording().setBatchSize(10);
        sinkFailProps.getRecording().setBatchTimeoutMs(100);
        sinkFailProps.getThreading().setWriteRetryMaxAttempts(1);
        sinkFailProps.getThreading().setWriteRetryInitialMs(50);

        DuckLakeWriteChannel sinkFailWriteChannel =
                new DuckLakeWriteChannel(duckDB, sinkFailProps, new CassetteRecordingBus(sinkFailProps), TEST_METRICS);
        sinkFailWriteChannel.start();
        try {
            BrokerRepository brokerRepository = new BrokerRepository(duckDB, sinkFailProps);
            BrokerConnectionFactory brokerFactory = new BrokerConnectionFactory(brokerRepository, sinkFailProps);
            ConfigRepository configRepo = new ConfigRepository(duckDB, sinkFailProps);
            MessageRouter router = new MessageRouter(configRepo, new EntityIdExtractor(), TEST_METRICS);
            KnownEntitiesRepository knownEntities =
                    new KnownEntitiesRepository(org.jooq.impl.DSL.using(duckDB, org.jooq.SQLDialect.DUCKDB));
            Executor vtExecutor = Executors.newVirtualThreadPerTaskExecutor();

            ActorRef<TopicLifecycleActor.Cmd> actor = kit.spawn(
                    TopicLifecycleActor.create(SINKFAIL_TOPIC, "earliest", Instant.now(), sinkFailProps, brokerFactory,
                            null, sinkFailWriteChannel, router, knownEntities, vtExecutor, TEST_METRICS));

            await().atMost(Duration.ofSeconds(15))
                   .untilAsserted(() -> assertThat(TopicRecorder.liveConsumerCount())
                           .isGreaterThanOrEqualTo(SINKFAIL_PARTITIONS));

            // Any message on partition 0 triggers the write failure that escalates
            // straight to FAILED (writeRetryMaxAttempts=1) and fires SinkFailed. Its
            // offset is never committed (the write never succeeds), so every subsequent
            // generation re-reads and re-fails on it too — a genuine restart STORM, not
            // just one restart. This is deliberate: it is the sharpest possible signal
            // for the premature-stop bug. Pre-fix, the very FIRST restart's PreRestart
            // cleanup (stopping all-healthy recorders) produces exactly SINKFAIL_PARTITIONS
            // stale RecorderFinished messages, which stop the brand-new generation before
            // it ever gets to fail again — so the restarts counter freezes at 1 forever
            // and the actor goes silent. Post-fix, the generation check discards those
            // stale messages, so the actor keeps genuinely failing and restarting: the
            // restarts counter climbs past 1 and GetStatus keeps being answered.
            publishToTopic(SINKFAIL_TOPIC, 0, "trigger");

            var restartsCounter = TEST_METRICS.recordingMetrics(SINKFAIL_TOPIC).restarts();
            await().atMost(Duration.ofSeconds(20))
                   .pollInterval(Duration.ofMillis(200))
                   .untilAsserted(() -> assertThat(restartsCounter.count())
                           .as("actor must survive past the FIRST restart — freezing at 1 is the " +
                                   "premature-stop bug: PreRestart's stale RecorderFinished messages " +
                                   "stopped the new generation before it could fail (and restart) again")
                           .isGreaterThanOrEqualTo(2.0));

            // The actor must still be alive and answering — not just "restarts counted"
            // (which happens in the OLD generation right before it throws, so a subtler
            // bug could still show a restarts count without a live actor behind it).
            TestProbe<RecorderStatus> probe = kit.createTestProbe(RecorderStatus.class);
            actor.tell(new TopicLifecycleActor.GetStatus(probe.ref()));
            RecorderStatus status = probe.receiveMessage(Duration.ofSeconds(5));
            assertThat(status).isNotNull();
        } finally {
            sinkFailWriteChannel.stop();
            deleteTopic(SINKFAIL_TOPIC);
        }
    }

    private static final String SINKFAIL_TOPIC = "lifecycle.sinkfail.test.events";
    private static final String SINKFAIL_SANITIZED = "lifecycle_sinkfail_test_events";
    private static final int SINKFAIL_PARTITIONS = 3;

    @Test
    void restartAfterOnePartitionFailure_stopsAllOldRecordersInsteadOfLeakingHealthySiblings() throws Exception {
        BrokerRepository brokerRepository = new BrokerRepository(duckDB, props);
        BrokerConnectionFactory brokerFactory = new BrokerConnectionFactory(brokerRepository, props);
        ConfigRepository configRepo = new ConfigRepository(duckDB, props);
        MessageRouter router = new MessageRouter(configRepo, new EntityIdExtractor(), TEST_METRICS);
        KnownEntitiesRepository knownEntities =
                new KnownEntitiesRepository(org.jooq.impl.DSL.using(duckDB, org.jooq.SQLDialect.DUCKDB));
        Executor vtExecutor = Executors.newVirtualThreadPerTaskExecutor();

        ActorRef<TopicLifecycleActor.Cmd> actor = kit.spawn(
                TopicLifecycleActor.create(TOPIC, "earliest", Instant.now(), props, brokerFactory,
                        null, writeChannel, router, knownEntities, vtExecutor, TEST_METRICS));

        // Wait for all 3 per-partition recorders to open a live KafkaConsumer.
        await().atMost(Duration.ofSeconds(15))
               .untilAsserted(() -> assertThat(TopicRecorder.liveConsumerCount()).isEqualTo(PARTITIONS));

        // Only partition 0 ever gets a message; partitions 1 and 2 stay idle and
        // healthy while partition 0's write fails against the missing table.
        publishToPartition(0, "trigger");

        // Wait for the actor to come back up with a fresh generation of exactly
        // PARTITIONS recorders (proves the restart completed).
        TestProbe<RecorderStatus> probe = kit.createTestProbe(RecorderStatus.class);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            actor.tell(new TopicLifecycleActor.GetStatus(probe.ref()));
            RecorderStatus status = probe.receiveMessage(Duration.ofSeconds(2));
            assertThat(status.assignedPartitions()).hasSize(PARTITIONS);
        });

        // Before the fix: partitions 1 and 2's original KafkaConsumers are never
        // stopped, so once the new generation's 3 recorders also come up the live
        // count sits at 6 forever. After the fix: PreRestart cleanup stops all 3
        // old recorders, so the count settles back to exactly PARTITIONS.
        await().atMost(Duration.ofSeconds(15))
               .untilAsserted(() -> assertThat(TopicRecorder.liveConsumerCount())
                       .as("old generation's recorders must be stopped, not doubled up with the new generation")
                       .isEqualTo(PARTITIONS));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void publishToPartition(int partition, String value) throws Exception {
        publishToTopic(TOPIC, partition, value);
    }

    private void publishToTopic(String topic, int partition, String value) throws Exception {
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"))) {
            producer.send(new ProducerRecord<>(topic, partition, "k", value.getBytes(StandardCharsets.UTF_8))).get();
        }
    }

    private void createTopic(String topic, int partitions) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                    .all().get(15, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    private void deleteTopic(String topic) {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.deleteTopics(List.of(topic)).all().get(5, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception ignored) {}
    }
}
