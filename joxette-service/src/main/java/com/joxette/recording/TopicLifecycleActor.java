package com.joxette.recording;

import com.joxette.config.BrokerConnectionFactory;
import com.joxette.config.JoxetteProperties;
import com.joxette.metrics.JoxetteMetrics;
import com.joxette.replay.KnownEntitiesRepository;
import com.joxette.replay.MessageRouter;
import com.softwaremill.jox.kafka.ConsumerSettings;
import com.softwaremill.jox.structured.Scopes;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.SupervisorStrategy;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Per-topic lifecycle actor.
 *
 * <p>Wraps one {@link TopicRecorder} run inside a VT launched via
 * {@code context.pipeToSelf}.  Pekko's exponential-backoff supervisor
 * (declared in {@link RecordingCoordinatorActor}) restarts this actor on
 * failure, replacing the Resilience4j retry used by the old
 * {@link RecordingCoordinator}.
 *
 * <h2>States</h2>
 * <ul>
 *   <li>{@code starting} — VT is being launched; messages queue in the mailbox</li>
 *   <li>{@code recording} — VT is live; status queries are answered from cached state</li>
 *   <li>{@code stopping} — stop has been requested; waits for VT completion</li>
 * </ul>
 */
public class TopicLifecycleActor {

    private static final Logger log = LoggerFactory.getLogger(TopicLifecycleActor.class);

    // -------------------------------------------------------------------------
    // Command protocol
    // -------------------------------------------------------------------------

    public sealed interface Cmd {}

    public record Stop(org.apache.pekko.actor.typed.ActorRef<StopReply> replyTo) implements Cmd {}
    public record GetStatus(org.apache.pekko.actor.typed.ActorRef<RecorderStatus> replyTo) implements Cmd {}

    private record RecorderFinished(int partition) implements Cmd {}
    private record RecorderFailed(int partition, Throwable cause) implements Cmd {}
    // (old single-recorder variants shadowed by the new per-partition versions above)

    public sealed interface StopReply {}
    public record Stopped() implements StopReply {}

    // -------------------------------------------------------------------------
    // Factory — Pekko exponential-backoff supervisor is declared HERE so it
    // applies automatically on exception from the running behavior.
    // -------------------------------------------------------------------------

    public static Behavior<Cmd> create(
            String topic,
            String startFrom,
            Instant startedAt,
            JoxetteProperties props,
            BrokerConnectionFactory brokerFactory,
            String brokerId,
            DuckLakeWriteChannel writeChannel,
            MessageRouter router,
            KnownEntitiesRepository knownEntities,
            Executor vtExecutor,
            JoxetteMetrics joxetteMetrics) {

        JoxetteProperties.Recording cfg = props.getRecording();
        Behavior<Cmd> coreBehavior = Behaviors.setup(ctx ->
                starting(ctx, topic, startFrom, startedAt, props, brokerFactory, brokerId,
                         writeChannel, router, knownEntities, vtExecutor, joxetteMetrics));

        return Behaviors.supervise(coreBehavior)
                .onFailure(Exception.class, SupervisorStrategy.restartWithBackoff(
                        Duration.ofMillis(cfg.getRetryInitialIntervalMs()),
                        Duration.ofMillis(cfg.getRetryMaxIntervalMs()),
                        0.2   // jitter (20 %)
                ).withLoggingEnabled(true));
    }

    // -------------------------------------------------------------------------
    // States
    // -------------------------------------------------------------------------

    private static Behavior<Cmd> starting(
            ActorContext<Cmd> ctx,
            String topic,
            String startFrom,
            Instant startedAt,
            JoxetteProperties props,
            BrokerConnectionFactory brokerFactory,
            String brokerId,
            DuckLakeWriteChannel writeChannel,
            MessageRouter router,
            KnownEntitiesRepository knownEntities,
            Executor vtExecutor,
            JoxetteMetrics joxetteMetrics) {

        JoxetteProperties.Recording cfg = props.getRecording();
        ConsumerSettings<String, byte[]> baseSettings = brokerFactory.consumerSettings(brokerId);

        // Look up partition count using a short-lived consumer — no group join needed.
        int partitionCount = 1;
        try {
            ConsumerSettings<String, byte[]> metaSettings = baseSettings
                    .groupId(baseSettings.groupId() + "-meta-" + topic);
            try (KafkaConsumer<String, byte[]> probe = metaSettings.toConsumer()) {
                partitionCount = probe.partitionsFor(topic).size();
            }
        } catch (Exception e) {
            log.warn("TopicLifecycleActor: could not determine partition count for '{}'; defaulting to 1: {}",
                    topic, e.getMessage());
        }

        log.info("TopicLifecycleActor: launching {} per-partition recorder(s) for topic '{}'",
                partitionCount, topic);

        List<TopicRecorder> recorders = new ArrayList<>(partitionCount);
        for (int p = 0; p < partitionCount; p++) {
            int partition = p;
            TopicRecorder recorder = new TopicRecorder(
                    topic, partition, baseSettings, writeChannel,
                    cfg.getBatchSize(), cfg.getBatchTimeoutMs(),
                    router, knownEntities, startFrom, joxetteMetrics);
            recorders.add(recorder);

            ctx.pipeToSelf(
                    CompletableFuture.supplyAsync(() -> {
                        try {
                            recorder.run();
                            return (Cmd) new RecorderFinished(partition);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }, vtExecutor),
                    (result, ex) -> ex != null ? new RecorderFailed(partition, ex) : result
            );
        }

        return recording(ctx, topic, startFrom, startedAt, recorders,
                         props, brokerFactory, brokerId, writeChannel, router, knownEntities,
                         vtExecutor, joxetteMetrics);
    }

    private static Behavior<Cmd> recording(
            ActorContext<Cmd> ctx,
            String topic,
            String startFrom,
            Instant startedAt,
            List<TopicRecorder> recorders,
            JoxetteProperties props,
            BrokerConnectionFactory brokerFactory,
            String brokerId,
            DuckLakeWriteChannel writeChannel,
            MessageRouter router,
            KnownEntitiesRepository knownEntities,
            Executor vtExecutor,
            JoxetteMetrics joxetteMetrics) {

        int[] finishedCount = {0};

        return Behaviors.receive(Cmd.class)
                .onMessage(GetStatus.class, msg -> {
                    msg.replyTo().tell(buildStatus(topic, startedAt, recorders, null));
                    return Behaviors.same();
                })
                .onMessage(Stop.class, msg -> {
                    recorders.forEach(TopicRecorder::stop);
                    return stopping(topic, startedAt, recorders, recorders.size(), msg.replyTo());
                })
                .onMessage(RecorderFinished.class, msg -> {
                    finishedCount[0]++;
                    log.info("TopicLifecycleActor: partition {} finished for topic '{}' ({}/{})",
                            msg.partition(), topic, finishedCount[0], recorders.size());
                    if (finishedCount[0] >= recorders.size()) {
                        log.info("TopicLifecycleActor: all partitions finished for topic '{}'", topic);
                        return Behaviors.stopped();
                    }
                    return Behaviors.same();
                })
                .onMessage(RecorderFailed.class, msg -> {
                    log.error("TopicLifecycleActor: partition {} failed for topic '{}': {}",
                            msg.partition(), topic, msg.cause().getMessage());
                    joxetteMetrics.recordingMetrics(topic).restarts().increment();
                    throw new RuntimeException(msg.cause());
                })
                .build();
    }

    private static Behavior<Cmd> stopping(
            String topic,
            Instant startedAt,
            List<TopicRecorder> recorders,
            int totalPartitions,
            org.apache.pekko.actor.typed.ActorRef<StopReply> replyTo) {

        int[] stoppedCount = {0};

        return Behaviors.receive(Cmd.class)
                .onMessage(RecorderFinished.class, msg -> {
                    stoppedCount[0]++;
                    if (stoppedCount[0] >= totalPartitions) {
                        log.info("TopicLifecycleActor: all partitions stopped for topic '{}'", topic);
                        replyTo.tell(new Stopped());
                        return Behaviors.stopped();
                    }
                    return Behaviors.same();
                })
                .onMessage(RecorderFailed.class, msg -> {
                    stoppedCount[0]++;
                    log.warn("TopicLifecycleActor: partition {} stopped with error for topic '{}': {}",
                            msg.partition(), topic, msg.cause().getMessage());
                    if (stoppedCount[0] >= totalPartitions) {
                        replyTo.tell(new Stopped());
                        return Behaviors.stopped();
                    }
                    return Behaviors.same();
                })
                .onMessage(GetStatus.class, msg -> {
                    msg.replyTo().tell(buildStatus(topic, startedAt, recorders, null));
                    return Behaviors.same();
                })
                .onMessage(Stop.class, msg -> {
                    msg.replyTo().tell(new Stopped());
                    return Behaviors.same();
                })
                .build();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    static RecorderStatus buildStatus(String topic, Instant startedAt, List<TopicRecorder> recorders, String lastError) {
        // Aggregate across all per-partition recorders
        long lag = recorders.stream().mapToLong(TopicRecorder::consumerLag).filter(l -> l >= 0).sum();
        Set<Integer> partitions = new java.util.HashSet<>();
        recorders.forEach(r -> partitions.addAll(r.assignedPartitionIds()));
        boolean running = recorders.stream().anyMatch(r -> !r.isStopped());
        Instant lastBatch = recorders.stream()
                .map(TopicRecorder::lastBatchAt)
                .filter(java.util.Objects::nonNull)
                .max(java.util.Comparator.naturalOrder())
                .orElse(null);
        long consumed = recorders.stream().mapToLong(TopicRecorder::messagesConsumed).sum();
        long written  = recorders.stream().mapToLong(TopicRecorder::messagesWritten).sum();
        String protocol = recorders.isEmpty() ? "unknown" : recorders.get(0).negotiatedProtocol();
        return new RecorderStatus(
                topic,
                running,
                startedAt,
                lastBatch,
                lag,
                lastError,
                protocol,
                partitions,
                consumed,
                written
        );
    }
}
