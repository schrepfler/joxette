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
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"))) {
            producer.send(new ProducerRecord<>(TOPIC, partition, "k", value.getBytes(StandardCharsets.UTF_8))).get();
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
