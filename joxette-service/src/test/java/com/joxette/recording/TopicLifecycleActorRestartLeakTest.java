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

    /**
     * NOTE (round-2 re-review correction): the original premise behind this test — that a single
     * {@code SinkFailed} restart stops ALL {@code SINKFAIL_PARTITIONS} recorders and thereby
     * produces {@code SINKFAIL_PARTITIONS} stale {@code RecorderFinished} messages, enough by
     * itself to cross the pre-fix {@code finishedCount[0] >= recorders.size()} guard — is
     * factually wrong. Only the partition whose write actually failed throws inside
     * {@code TopicRecorder.run()} (its blocking {@code writeChannel.submit()} call receives the
     * same exception that also fires the {@code SinkFailed} callback), so it produces its own real
     * {@code RecorderFailed}, not a stale {@code RecorderFinished}. {@code PreRestart} then calls
     * {@code stop()} on the other {@code SINKFAIL_PARTITIONS - 1} still-healthy siblings, which
     * exit their poll loops cleanly and produce {@code SINKFAIL_PARTITIONS - 1} stale
     * {@code RecorderFinished} — one short of the {@code recorders.size()} threshold. A single
     * restart can therefore never trip {@code finishedCount}'s premature-stop path through this
     * mechanism alone; see {@link #staleGenerationRecorderFinished_afterCrossingThreshold_doesNotStopTheActor}
     * for the deterministic test that actually reproduces (and would fail on) that specific bug.
     *
     * <p>This test is kept as an honest, weaker smoke test: it drives a real restart STORM
     * against live Kafka + DuckDB (the failing offset is never committed, so every new generation
     * re-reads and re-fails on it) and asserts the actor keeps genuinely restarting and answering
     * {@code GetStatus} rather than going silent. That is real coverage for "the actor survives
     * repeated real {@code SinkFailed} escalations end-to-end," which is a legitimate thing to
     * verify, but on its own — as the re-reviewer correctly found — it does NOT distinguish
     * pre-fix from post-fix code, since ordinary repeated real failures satisfy
     * {@code restarts >= 2} regardless of whether the generation guard is present.
     */
    @Test
    void restartAfterSinkFailure_survivesRepeatedRealFailuresEndToEnd() throws Exception {
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
            // straight to FAILED (writeRetryMaxAttempts=1) and fires SinkFailed for that
            // partition's own RecorderFailed. Its offset is never committed (the write
            // never succeeds), so every subsequent generation re-reads and re-fails on it
            // too — a genuine restart STORM, not just one restart. This is a real
            // end-to-end smoke check that the actor keeps functioning under repeated
            // SinkFailed escalations (see the class-level note on this method for why it
            // is NOT, by itself, a regression test for the generation-guard fix).
            publishToTopic(SINKFAIL_TOPIC, 0, "trigger");

            var restartsCounter = TEST_METRICS.recordingMetrics(SINKFAIL_TOPIC).restarts();
            await().atMost(Duration.ofSeconds(20))
                   .pollInterval(Duration.ofMillis(200))
                   .untilAsserted(() -> assertThat(restartsCounter.count())
                           .as("actor must survive past the FIRST restart and keep genuinely " +
                                   "failing/restarting against the live, uncommitted-offset failure")
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

    /**
     * Real generations from {@link TopicLifecycleActor#create} always start at 1 (the
     * {@code generation[0]} counter in {@code create()} is pre-incremented from 0 before the
     * first {@code starting()} call). 0 can therefore never be a real generation — using it here
     * unambiguously marks every message built with it as "stale", exactly like a message stashed
     * by Pekko's {@code restart-stash-capacity} from a torn-down prior generation would be once
     * redelivered to a new one.
     */
    private static final long STALE_GENERATION = 0L;

    /**
     * Deterministic, unit-level reproduction of the bug {@code 7fffde7} fixed for
     * {@code RecorderFinished}, sidestepping the timing problems that made
     * {@link #restartAfterSinkFailure_survivesRepeatedRealFailuresEndToEnd} an unreliable
     * regression test for it (see that method's corrected comment above).
     *
     * <p>Rather than trying to time a real restart so that Pekko's stash-and-redeliver produces
     * stale messages naturally, this test constructs {@code recorders.size()} stale
     * {@code RecorderFinished} messages directly — now possible because {@code RecorderFinished}
     * was loosened from {@code private record} to package-private specifically for this — and
     * tells them straight to a live actor's {@code ActorRef<Cmd>}. This is exactly the payload
     * shape that trips the pre-fix guard in {@code recording()}:
     * {@code finishedCount[0] >= recorders.size()} — reached by ANY combination of
     * {@code recorders.size()} {@code RecorderFinished} deliveries, stale or not, since pre-fix
     * code could not tell the difference. Post-fix, the generation check discards every one of
     * them before they touch {@code finishedCount}, so the actor must still be alive and
     * answering {@code GetStatus} afterwards.
     *
     * <p><b>RED evidence (obtained manually, not automated here):</b> commenting out the
     * {@code if (msg.generation() != myGeneration) return Behaviors.same();} guard at the top of
     * {@code recording()}'s {@code RecorderFinished} handler and re-running this test fails it —
     * {@code finishedCount[0]} reaches {@code recorders.size()} on the last synthetic message,
     * {@code recording()} returns {@code Behaviors.stopped()}, and the final {@code GetStatus}
     * round-trip times out because no behavior is left to answer it. Restoring the guard makes
     * the test pass again. See the round-2 report for the exact commands used.
     */
    @Test
    void staleGenerationRecorderFinished_afterCrossingThreshold_doesNotStopTheActor() throws Exception {
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

        TestProbe<RecorderStatus> probe = kit.createTestProbe(RecorderStatus.class);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            actor.tell(new TopicLifecycleActor.GetStatus(probe.ref()));
            RecorderStatus status = probe.receiveMessage(Duration.ofSeconds(2));
            assertThat(status.assignedPartitions()).hasSize(PARTITIONS);
        });

        // recorders.size() == PARTITIONS stale RecorderFinished messages — exactly enough to
        // cross the pre-fix `finishedCount[0] >= recorders.size()` guard.
        for (int p = 0; p < PARTITIONS; p++) {
            actor.tell(new TopicLifecycleActor.RecorderFinished(p, STALE_GENERATION));
        }

        // Also inject a stale RecorderFailed for good measure — pre-fix it would have thrown
        // unconditionally and forced a spurious restart; post-fix it must be discarded too.
        actor.tell(new TopicLifecycleActor.RecorderFailed(0, new RuntimeException("stale"), STALE_GENERATION));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            actor.tell(new TopicLifecycleActor.GetStatus(probe.ref()));
            RecorderStatus status = probe.receiveMessage(Duration.ofSeconds(3));
            assertThat(status)
                    .as("actor must still be alive and answering GetStatus — the stale messages " +
                            "must have been discarded by the generation guard, not counted toward " +
                            "finishedCount or thrown as a real failure")
                    .isNotNull();
            assertThat(status.assignedPartitions()).hasSize(PARTITIONS);
        });
    }

    /**
     * Direct unit-level coverage for the {@code SinkFailed} generation guard added alongside this
     * test (round-2 fix — {@code SinkFailed} previously carried no generation tag at all and was
     * unconditionally trusted by {@code recording()}). A stale {@code SinkFailed} redelivered from
     * a torn-down generation must not increment {@code joxette.recorder.restarts} nor throw.
     */
    @Test
    void staleGenerationSinkFailed_doesNotTriggerASpuriousRestart() throws Exception {
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

        TestProbe<RecorderStatus> probe = kit.createTestProbe(RecorderStatus.class);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            actor.tell(new TopicLifecycleActor.GetStatus(probe.ref()));
            RecorderStatus status = probe.receiveMessage(Duration.ofSeconds(2));
            assertThat(status.assignedPartitions()).hasSize(PARTITIONS);
        });

        var restartsCounter = TEST_METRICS.recordingMetrics(TOPIC).restarts();
        double restartsBefore = restartsCounter.count();

        actor.tell(new TopicLifecycleActor.SinkFailed(new RuntimeException("stale sink failure"), STALE_GENERATION));

        // Pre-fix this would throw synchronously in the message handler and increment the
        // counter immediately; give it a beat, then assert the count is unchanged and the actor
        // is still answering with its original recorder set (i.e. it never restarted).
        await().pollDelay(Duration.ofMillis(500))
               .atMost(Duration.ofSeconds(5))
               .untilAsserted(() -> assertThat(restartsCounter.count())
                       .as("stale SinkFailed must be discarded by the generation guard, not counted as a real failure")
                       .isEqualTo(restartsBefore));

        actor.tell(new TopicLifecycleActor.GetStatus(probe.ref()));
        RecorderStatus status = probe.receiveMessage(Duration.ofSeconds(3));
        assertThat(status).isNotNull();
        assertThat(status.assignedPartitions()).hasSize(PARTITIONS);
    }

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
