package com.joxette.recording;

import com.joxette.config.BrokerConnectionFactory;
import com.joxette.config.JoxetteProperties;
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

import java.time.Duration;
import java.time.Instant;
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

    private record RecorderFinished() implements Cmd {}
    private record RecorderFailed(Throwable cause) implements Cmd {}

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
            Executor vtExecutor) {

        JoxetteProperties.Recording cfg = props.getRecording();
        Behavior<Cmd> coreBehavior = Behaviors.setup(ctx ->
                starting(ctx, topic, startFrom, startedAt, props, brokerFactory, brokerId,
                         writeChannel, router, knownEntities, vtExecutor));

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
            Executor vtExecutor) {

        JoxetteProperties.Recording cfg = props.getRecording();
        ConsumerSettings<String, byte[]> baseSettings = brokerFactory.consumerSettings(brokerId);

        TopicRecorder recorder = new TopicRecorder(
                topic, baseSettings, writeChannel,
                cfg.getBatchSize(), cfg.getBatchTimeoutMs(),
                router, knownEntities, startFrom);

        // Launch in VT; pipe result back as Cmd
        ctx.pipeToSelf(
                CompletableFuture.supplyAsync(() -> {
                    try {
                        recorder.run();
                        return (Cmd) new RecorderFinished();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, vtExecutor),
                (result, ex) -> ex != null ? new RecorderFailed(ex) : result
        );

        log.info("TopicLifecycleActor: launching recorder for topic '{}'", topic);
        return recording(ctx, topic, startFrom, startedAt, recorder,
                         props, brokerFactory, brokerId, writeChannel, router, knownEntities, vtExecutor);
    }

    private static Behavior<Cmd> recording(
            ActorContext<Cmd> ctx,
            String topic,
            String startFrom,
            Instant startedAt,
            TopicRecorder recorder,
            JoxetteProperties props,
            BrokerConnectionFactory brokerFactory,
            String brokerId,
            DuckLakeWriteChannel writeChannel,
            MessageRouter router,
            KnownEntitiesRepository knownEntities,
            Executor vtExecutor) {

        return Behaviors.receive(Cmd.class)
                .onMessage(GetStatus.class, msg -> {
                    msg.replyTo().tell(buildStatus(topic, startedAt, recorder, null));
                    return Behaviors.same();
                })
                .onMessage(Stop.class, msg -> {
                    recorder.stop();
                    return stopping(topic, startedAt, recorder, msg.replyTo());
                })
                .onMessage(RecorderFinished.class, msg -> {
                    log.info("TopicLifecycleActor: recorder finished cleanly for topic '{}'", topic);
                    // Finished without stop — let the supervisor decide (backoff on failure).
                    return Behaviors.stopped();
                })
                .onMessage(RecorderFailed.class, msg -> {
                    log.error("TopicLifecycleActor: recorder failed for topic '{}'", topic, msg.cause());
                    // Throw so the supervisor's backoff strategy kicks in.
                    throw new RuntimeException(msg.cause());
                })
                .build();
    }

    private static Behavior<Cmd> stopping(
            String topic,
            Instant startedAt,
            TopicRecorder recorder,
            org.apache.pekko.actor.typed.ActorRef<StopReply> replyTo) {

        return Behaviors.receive(Cmd.class)
                .onMessage(RecorderFinished.class, msg -> {
                    log.info("TopicLifecycleActor: stopped cleanly for topic '{}'", topic);
                    replyTo.tell(new Stopped());
                    return Behaviors.stopped();
                })
                .onMessage(RecorderFailed.class, msg -> {
                    log.warn("TopicLifecycleActor: stopped with error for topic '{}': {}",
                            topic, msg.cause().getMessage());
                    replyTo.tell(new Stopped());
                    return Behaviors.stopped();
                })
                .onMessage(GetStatus.class, msg -> {
                    msg.replyTo().tell(buildStatus(topic, startedAt, recorder, null));
                    return Behaviors.same();
                })
                .onMessage(Stop.class, msg -> {
                    // Already stopping — acknowledge immediately
                    msg.replyTo().tell(new Stopped());
                    return Behaviors.same();
                })
                .build();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    static RecorderStatus buildStatus(String topic, Instant startedAt, TopicRecorder recorder, String lastError) {
        long lag = recorder.consumerLag();
        Set<Integer> partitions = recorder.assignedPartitionIds();
        return new RecorderStatus(
                topic,
                !recorder.isStopped(),
                startedAt,
                recorder.lastBatchAt(),
                lag,
                lastError,
                recorder.negotiatedProtocol(),
                partitions,
                recorder.messagesConsumed(),
                recorder.messagesWritten()
        );
    }
}
