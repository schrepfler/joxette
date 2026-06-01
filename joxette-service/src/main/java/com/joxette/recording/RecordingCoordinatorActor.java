package com.joxette.recording;

import com.joxette.config.BrokerConnectionFactory;
import com.joxette.config.JoxetteProperties;
import com.joxette.management.ConfigRepository;
import com.joxette.management.TopicConfig;
import com.joxette.replay.KnownEntitiesRepository;
import com.joxette.replay.MessageRouter;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.SupervisorStrategy;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Coordinator actor that manages per-topic {@link TopicLifecycleActor} children.
 *
 * <p>Each child is spawned with a Pekko exponential-backoff supervisor
 * (configured inside {@link TopicLifecycleActor#create}).  The parent tracks
 * child actor refs in a {@link HashMap} keyed by topic name.
 *
 * <h2>Thread safety</h2>
 * All state mutations happen inside the actor's single-threaded mailbox.
 * Ask-pattern calls from Spring beans are safe from any thread.
 */
public class RecordingCoordinatorActor {

    private static final Logger log = LoggerFactory.getLogger(RecordingCoordinatorActor.class);
    static final Duration ASK_TIMEOUT = Duration.ofSeconds(10);

    // -------------------------------------------------------------------------
    // Command protocol
    // -------------------------------------------------------------------------

    public sealed interface CoordinatorCommand {}

    public record StartTopic(
            String topic,
            String startFrom,
            ActorRef<StartReply> replyTo
    ) implements CoordinatorCommand {}

    public record StopTopic(
            String topic,
            ActorRef<StopReply> replyTo
    ) implements CoordinatorCommand {}

    public record RestartTopic(
            String topic,
            ActorRef<StartReply> replyTo
    ) implements CoordinatorCommand {}

    public record ListRunning(
            ActorRef<Map<String, RecorderStatus>> replyTo
    ) implements CoordinatorCommand {}

    public record ActiveTopics(
            ActorRef<Set<String>> replyTo
    ) implements CoordinatorCommand {}

    /**
     * Declarative reconciliation: align running recorders with the desired state
     * from the catalog. Called by {@link com.joxette.recording.RecordingConfigWatcher}
     * on startup, on a 30 s tick, and immediately after a pub/sub config event.
     *
     * <p>The coordinator:
     * <ul>
     *   <li>starts topics that are in {@code desired} (not paused) but not running</li>
     *   <li>stops topics that are running but absent from {@code desired} or paused</li>
     *   <li>restarts topics whose {@code startFrom} changed</li>
     * </ul>
     */
    public record ReconcileTopics(
            java.util.List<com.joxette.management.TopicConfig> desired,
            ActorRef<ReconcileReply> replyTo
    ) implements CoordinatorCommand {}

    public record ReconcileReply(int started, int stopped, int unchanged) {}

    /**
     * Sent internally when a child actor's stop sequence completes.
     * Carries the original caller's replyTo so we can acknowledge only after
     * the recorder VT has actually exited.
     */
    private record ChildStopped(String topic, ActorRef<StopReply> callerReplyTo) implements CoordinatorCommand {}

    public sealed interface StartReply {}
    public record Started(boolean newScope) implements StartReply {}

    public sealed interface StopReply {}
    public record StopComplete(boolean wasStopped) implements StopReply {}

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    public static Behavior<CoordinatorCommand> create(
            JoxetteProperties props,
            BrokerConnectionFactory brokerFactory,
            ConfigRepository configRepo,
            DuckLakeWriteChannel writeChannel,
            MessageRouter router,
            KnownEntitiesRepository knownEntities,
            Executor vtExecutor) {

        return Behaviors.setup(ctx ->
                coordinator(ctx, new HashMap<>(), new HashMap<>(),
                            props, brokerFactory, configRepo, writeChannel, router, knownEntities, vtExecutor));
    }

    // -------------------------------------------------------------------------
    // Behavior
    // -------------------------------------------------------------------------

    private static Behavior<CoordinatorCommand> coordinator(
            ActorContext<CoordinatorCommand> ctx,
            Map<String, ActorRef<TopicLifecycleActor.Cmd>> children,
            Map<String, Instant> startedAts,
            JoxetteProperties props,
            BrokerConnectionFactory brokerFactory,
            ConfigRepository configRepo,
            DuckLakeWriteChannel writeChannel,
            MessageRouter router,
            KnownEntitiesRepository knownEntities,
            Executor vtExecutor) {

        return Behaviors.receive(CoordinatorCommand.class)
                .onMessage(StartTopic.class, msg -> {
                    if (children.containsKey(msg.topic())) {
                        log.debug("Recorder for topic '{}' already running", msg.topic());
                        msg.replyTo().tell(new Started(false));
                        return Behaviors.same();
                    }
                    String brokerId = lookupBrokerId(msg.topic(), configRepo);
                    ActorRef<TopicLifecycleActor.Cmd> child = ctx.spawn(
                            TopicLifecycleActor.create(
                                    msg.topic(), msg.startFrom(), Instant.now(),
                                    props, brokerFactory, brokerId,
                                    writeChannel, router, knownEntities, vtExecutor),
                            "topic-" + sanitizeName(msg.topic()));
                    ctx.watchWith(child, new ChildStopped(msg.topic(), null));
                    children.put(msg.topic(), child);
                    startedAts.put(msg.topic(), Instant.now());
                    log.info("Started recorder actor for topic '{}'", msg.topic());
                    msg.replyTo().tell(new Started(true));
                    return Behaviors.same();
                })
                .onMessage(StopTopic.class, msg -> {
                    ActorRef<TopicLifecycleActor.Cmd> child = children.get(msg.topic());
                    if (child == null) {
                        log.debug("No active recorder for topic '{}'", msg.topic());
                        msg.replyTo().tell(new StopComplete(false));
                        return Behaviors.same();
                    }
                    // Remove before asking — the watchWith fallback fires only for unexpected
                    // terminations. The ask callback below is the authoritative "stopped" signal
                    // for explicit StopTopic requests, preventing a double ChildStopped log.
                    children.remove(msg.topic());
                    startedAts.remove(msg.topic());
                    ctx.unwatch(child);
                    ctx.ask(
                            TopicLifecycleActor.StopReply.class,
                            child,
                            ASK_TIMEOUT,
                            TopicLifecycleActor.Stop::new,
                            (reply, ex) -> new ChildStopped(msg.topic(), msg.replyTo())
                    );
                    return Behaviors.same();
                })
                .onMessage(RestartTopic.class, msg -> {
                    ActorRef<TopicLifecycleActor.Cmd> existing = children.remove(msg.topic());
                    startedAts.remove(msg.topic());
                    if (existing != null) {
                        // Fire-and-forget stop of old child; no caller waiting on it.
                        existing.tell(new TopicLifecycleActor.Stop(
                                ctx.messageAdapter(TopicLifecycleActor.StopReply.class,
                                        r -> new ChildStopped(msg.topic(), null))));
                    }
                    // Re-use the same startFrom stored in topic config
                    String brokerId = lookupBrokerId(msg.topic(), configRepo);
                    String startFrom = lookupStartFrom(msg.topic(), configRepo);
                    ActorRef<TopicLifecycleActor.Cmd> child = ctx.spawn(
                            TopicLifecycleActor.create(
                                    msg.topic(), startFrom, Instant.now(),
                                    props, brokerFactory, brokerId,
                                    writeChannel, router, knownEntities, vtExecutor),
                            "topic-" + sanitizeName(msg.topic()) + "-r" + System.nanoTime());
                    ctx.watchWith(child, new ChildStopped(msg.topic(), null));
                    children.put(msg.topic(), child);
                    startedAts.put(msg.topic(), Instant.now());
                    msg.replyTo().tell(new Started(true));
                    return Behaviors.same();
                })
                .onMessage(ListRunning.class, msg -> {
                    Map<String, RecorderStatus> result = new LinkedHashMap<>();
                    for (Map.Entry<String, ActorRef<TopicLifecycleActor.Cmd>> e : children.entrySet()) {
                        String topic = e.getKey();
                        // Ask each child for its status synchronously within the mailbox
                        RecorderStatus status = AskPattern.ask(
                                e.getValue(),
                                TopicLifecycleActor.GetStatus::new,
                                Duration.ofSeconds(2),
                                ctx.getSystem().scheduler()
                        ).toCompletableFuture().join();
                        result.put(topic, status);
                    }
                    msg.replyTo().tell(Map.copyOf(result));
                    return Behaviors.same();
                })
                .onMessage(ActiveTopics.class, msg -> {
                    msg.replyTo().tell(Set.copyOf(children.keySet()));
                    return Behaviors.same();
                })
                .onMessage(ReconcileTopics.class, msg -> {
                    int started = 0, stopped = 0, unchanged = 0;

                    // Build desired map: topic → startFrom (only non-paused topics)
                    Map<String, String> desired = new HashMap<>();
                    for (com.joxette.management.TopicConfig tc : msg.desired()) {
                        if (!tc.paused()) desired.put(tc.topic(), tc.startFrom() != null ? tc.startFrom() : "latest");
                    }

                    // Stop running topics not in desired (removed or paused)
                    for (String running : new ArrayList<>(children.keySet())) {
                        if (!desired.containsKey(running)) {
                            ActorRef<TopicLifecycleActor.Cmd> child = children.remove(running);
                            startedAts.remove(running);
                            ctx.unwatch(child);
                            child.tell(new TopicLifecycleActor.Stop(
                                    ctx.messageAdapter(TopicLifecycleActor.StopReply.class,
                                            r -> new ChildStopped(running, null))));
                            log.info("RecordingCoordinatorActor: reconcile stopping topic '{}'", running);
                            stopped++;
                        }
                    }

                    // Start or restart desired topics
                    for (Map.Entry<String, String> entry : desired.entrySet()) {
                        String topic = entry.getKey();
                        String startFrom = entry.getValue();
                        if (!children.containsKey(topic)) {
                            String brokerId = lookupBrokerId(topic, configRepo);
                            ActorRef<TopicLifecycleActor.Cmd> child = ctx.spawn(
                                    TopicLifecycleActor.create(topic, startFrom, Instant.now(),
                                            props, brokerFactory, brokerId, writeChannel, router, knownEntities, vtExecutor),
                                    "topic-" + sanitizeName(topic));
                            ctx.watchWith(child, new ChildStopped(topic, null));
                            children.put(topic, child);
                            startedAts.put(topic, Instant.now());
                            log.info("RecordingCoordinatorActor: reconcile starting topic '{}'", topic);
                            started++;
                        } else {
                            unchanged++;
                        }
                    }

                    log.debug("RecordingCoordinatorActor: reconcile complete — started={} stopped={} unchanged={}",
                            started, stopped, unchanged);
                    msg.replyTo().tell(new ReconcileReply(started, stopped, unchanged));
                    return Behaviors.same();
                })
                .onMessage(ChildStopped.class, msg -> {
                    // Child terminated — clean up any residual ref (already removed on explicit stop).
                    children.remove(msg.topic());
                    startedAts.remove(msg.topic());
                    log.info("RecordingCoordinatorActor: child for topic '{}' terminated", msg.topic());
                    // Reply to the original StopTopic caller now that the recorder VT has exited.
                    if (msg.callerReplyTo() != null) {
                        msg.callerReplyTo().tell(new StopComplete(true));
                    }
                    return Behaviors.same();
                })
                .build();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static String sanitizeName(String topic) {
        return topic.replaceAll("[^a-zA-Z0-9_-]", "_");
    }

    private static String lookupBrokerId(String topic, ConfigRepository configRepo) {
        try {
            return configRepo.findTopic(topic)
                    .map(TopicConfig::brokerId)
                    .orElse(null);
        } catch (SQLException e) {
            log.warn("Could not look up brokerId for topic '{}'; using default: {}", topic, e.getMessage());
            return null;
        }
    }

    private static String lookupStartFrom(String topic, ConfigRepository configRepo) {
        try {
            return configRepo.findTopic(topic)
                    .map(TopicConfig::startFrom)
                    .orElse("latest");
        } catch (SQLException e) {
            return "latest";
        }
    }
}
