package com.joxette.replay;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Coordinator actor for replay-to-topic operations.
 *
 * <p>Mirrors {@code RecordingCoordinatorActor}: manages per-request
 * {@link ReplayActor} children and answers {@code ListActive} queries.
 * Completed children linger in a separate map for {@value #LINGER_SECONDS}s
 * so the next SSE snapshot can still surface them; stale entries are evicted
 * lazily on the next {@code ListActive}.
 *
 * <h2>Cancellation</h2>
 * {@code SseEmitter.onCompletion} calls
 * {@code coordinator.tell(new CancelReplay(id))} → forwarded as
 * {@code ReplayActor.Cancel} → VT interrupted → Jox supervised scope
 * propagates cancellation to the DuckDB source.
 */
public class ReplayCoordinatorActor {

    private static final Logger log = LoggerFactory.getLogger(ReplayCoordinatorActor.class);
    static final Duration ASK_TIMEOUT = Duration.ofSeconds(5);
    private static final long LINGER_SECONDS = 30L;

    // -------------------------------------------------------------------------
    // Public types shared with ReplayActor
    // -------------------------------------------------------------------------

    public record ReplaySnapshot(
            String id,
            String sourceTopic,
            String targetTopic,
            Instant startedAt,
            long sentCount,
            ReplayStatus status
    ) {}

    // -------------------------------------------------------------------------
    // Command protocol
    // -------------------------------------------------------------------------

    public sealed interface Cmd {}

    public record StartReplay(
            String sourceTopic,
            String targetTopic,
            FlowReplayEngine engine,
            ReplayWork work,       // lambda describing which engine method to call
            Executor vtExecutor,
            ActorRef<String> replyTo  // reply with the assigned id
    ) implements Cmd {}

    public record CancelReplay(String id) implements Cmd {}

    /** Cancel all running replays — sent during shutdown before the actor system stops. */
    public record CancelAll(ActorRef<Integer> replyTo) implements Cmd {}

    public record ListActive(ActorRef<List<ReplaySnapshot>> replyTo) implements Cmd {}

    /** Sent by ReplayActor when its VT completes (normally, failed, or cancelled). */
    public record ChildDone(
            String id, String sourceTopic, String targetTopic,
            Instant startedAt, long sentCount, ReplayStatus status
    ) implements Cmd {}

    /** Functional interface for the actual engine call — allows both topic and entity replays. */
    @FunctionalInterface
    public interface ReplayWork {
        void run(Consumer<ReplayProgress> progressSink) throws Exception;
    }

    // -------------------------------------------------------------------------
    // Internal linger entry
    // -------------------------------------------------------------------------

    private record LingerEntry(ReplaySnapshot status, Instant completedAt) {}

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    public static Behavior<Cmd> create() {
        return Behaviors.setup(ctx -> behavior(ctx, new HashMap<>(), new HashMap<>()));
    }

    // -------------------------------------------------------------------------
    // Behavior
    // -------------------------------------------------------------------------

    private static Behavior<Cmd> behavior(
            ActorContext<Cmd> ctx,
            Map<String, ActorRef<ReplayActor.Cmd>> children,
            Map<String, LingerEntry> linger) {

        return Behaviors.receive(Cmd.class)

                .onMessage(StartReplay.class, msg -> {
                    String id = UUID.randomUUID().toString().substring(0, 8);
                    AtomicLong sentCounter = new AtomicLong();
                    Thread[] vtHolder = {null};

                    // Progress sink: update the counter (read by GetStatus) and forward to SSE
                    // sink. The SSE sink Consumer is captured inside the ReplayWork lambda.
                    Runnable vtWork = () -> {
                        try {
                            msg.work().run(p -> sentCounter.set(p.sentCount()));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    };

                    ActorRef<ReplayActor.Cmd> child = ctx.spawn(
                            ReplayActor.create(
                                    id,
                                    msg.sourceTopic(), msg.targetTopic(), Instant.now(),
                                    vtWork, vtHolder, sentCounter,
                                    ctx.getSelf().narrow(),
                                    msg.vtExecutor()),
                            "replay-" + id);
                    ctx.watchWith(child, new ChildDone(id, msg.sourceTopic(), msg.targetTopic(),
                            Instant.now(), 0, ReplayStatus.FAILED));  // watchWith fallback if actor crashes hard

                    children.put(id, child);
                    log.info("ReplayCoordinatorActor: started replay[{}] {} → {}", id, msg.sourceTopic(), msg.targetTopic());
                    msg.replyTo().tell(id);
                    return behavior(ctx, children, linger);
                })

                .onMessage(CancelReplay.class, msg -> {
                    ActorRef<ReplayActor.Cmd> child = children.get(msg.id());
                    if (child != null) {
                        log.debug("ReplayCoordinatorActor: cancelling replay[{}]", msg.id());
                        child.tell(new ReplayActor.Cancel());
                    }
                    return Behaviors.same();
                })

                .onMessage(CancelAll.class, msg -> {
                    int count = children.size();
                    if (count > 0) {
                        log.info("ReplayCoordinatorActor: cancelling {} active replay(s) for shutdown", count);
                        children.values().forEach(c -> c.tell(new ReplayActor.Cancel()));
                    }
                    msg.replyTo().tell(count);
                    return Behaviors.same();
                })

                .onMessage(ChildDone.class, msg -> {
                    children.remove(msg.id());
                    linger.put(msg.id(), new LingerEntry(
                            new ReplaySnapshot(msg.id(), msg.sourceTopic(), msg.targetTopic(),
                                    msg.startedAt(), msg.sentCount(), msg.status()),
                            Instant.now()));
                    log.info("ReplayCoordinatorActor: replay[{}] → {} (sent={})", msg.id(), msg.status(), msg.sentCount());
                    return behavior(ctx, children, linger);
                })

                .onMessage(ListActive.class, msg -> {
                    Instant evictBefore = Instant.now().minusSeconds(LINGER_SECONDS);
                    linger.entrySet().removeIf(e -> e.getValue().completedAt().isBefore(evictBefore));

                    List<ReplaySnapshot> result = new ArrayList<>(children.size() + linger.size());
                    // Running: ask each child for a live snapshot
                    for (Map.Entry<String, ActorRef<ReplayActor.Cmd>> e : children.entrySet()) {
                        // Inline ask within the mailbox — safe, same pattern as RecordingCoordinatorActor.ListRunning
                        ReplaySnapshot s = org.apache.pekko.actor.typed.javadsl.AskPattern.ask(
                                e.getValue(),
                                ReplayActor.GetStatus::new,
                                Duration.ofSeconds(2),
                                ctx.getSystem().scheduler()
                        ).toCompletableFuture().join();
                        result.add(s);
                    }
                    // Lingering completed/failed/cancelled
                    for (LingerEntry le : linger.values()) {
                        result.add(le.status());
                    }
                    msg.replyTo().tell(List.copyOf(result));
                    return Behaviors.same();
                })

                .build();
    }
}
