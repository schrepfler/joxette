package com.joxette.replay;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Per-request replay lifecycle actor.
 *
 * <p>Mirrors {@code TopicLifecycleActor}: a VT runs the Jox supervised-scope
 * pipeline via {@link FlowReplayEngine}, piping {@code Finished/Failed} back to
 * this actor. Progress events flow directly from the VT to the SSE sink — the
 * mailbox is used only for lifecycle signals (Cancel, GetStatus, Completed).
 *
 * <h2>States</h2>
 * <ul>
 *   <li>{@code streaming} — VT running; status queries answered from AtomicLong counters.</li>
 *   <li>{@code stopping} — Cancel received; VT interrupted; waiting for pipe-back.</li>
 * </ul>
 *
 * <h2>Cancellation</h2>
 * When the SSE client disconnects, {@code SseEmitter.onCompletion} tells the
 * coordinator {@code CancelReplay(id)}, which forwards a {@code Cancel} to this
 * actor. The supervised scope in {@link FlowReplayEngine} is interrupted and the
 * DuckDB read stops immediately — no ghost messages, no {@code IllegalStateException}.
 */
public class ReplayActor {

    private static final Logger log = LoggerFactory.getLogger(ReplayActor.class);

    // -------------------------------------------------------------------------
    // Command protocol
    // -------------------------------------------------------------------------

    public sealed interface Cmd {}

    /** Tell the actor to cancel the running replay (e.g. client disconnected). */
    public record Cancel() implements Cmd {}

    /** Ask for a point-in-time status snapshot. */
    public record GetStatus(ActorRef<ReplayCoordinatorActor.ReplaySnapshot> replyTo) implements Cmd {}

    // Internal pipe-back messages
    private record Finished() implements Cmd {}
    private record Failed(Throwable cause) implements Cmd {}

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    public static Behavior<Cmd> create(
            String id,
            String sourceTopic,
            String targetTopic,
            Instant startedAt,
            Runnable replayWork,        // FlowReplayEngine call, blocking on VT
            Thread[] vtHolder,          // [0] set by VT so Cancel can interrupt it
            AtomicLong sentCounter,
            ActorRef<ReplayCoordinatorActor.ChildDone> parent,
            Executor vtExecutor) {

        return Behaviors.setup(ctx ->
                streaming(ctx, id, sourceTopic, targetTopic, startedAt,
                          replayWork, vtHolder, sentCounter, parent, vtExecutor));
    }

    // -------------------------------------------------------------------------
    // streaming state
    // -------------------------------------------------------------------------

    private static Behavior<Cmd> streaming(
            ActorContext<Cmd> ctx,
            String id,
            String sourceTopic,
            String targetTopic,
            Instant startedAt,
            Runnable replayWork,
            Thread[] vtHolder,
            AtomicLong sentCounter,
            ActorRef<ReplayCoordinatorActor.ChildDone> parent,
            Executor vtExecutor) {

        ctx.pipeToSelf(
                CompletableFuture.supplyAsync(() -> {
                    vtHolder[0] = Thread.currentThread();
                    try {
                        replayWork.run();
                        return (Cmd) new Finished();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, vtExecutor),
                (result, ex) -> ex != null ? new Failed(ex) : result
        );

        return Behaviors.receive(Cmd.class)
                .onMessage(GetStatus.class, msg -> {
                    msg.replyTo().tell(snapshot(id, sourceTopic, targetTopic, startedAt, sentCounter, ReplayStatus.RUNNING));
                    return Behaviors.same();
                })
                .onMessage(Cancel.class, msg -> {
                    log.debug("ReplayActor[{}]: cancel requested, interrupting VT", id);
                    Thread vt = vtHolder[0];
                    if (vt != null) vt.interrupt();
                    return stopping(ctx, id, sourceTopic, targetTopic, startedAt, sentCounter, parent, ReplayStatus.CANCELLED);
                })
                .onMessage(Finished.class, msg -> {
                    log.info("ReplayActor[{}]: replay completed normally", id);
                    parent.tell(new ReplayCoordinatorActor.ChildDone(id, sourceTopic, targetTopic, startedAt, sentCounter.get(), ReplayStatus.COMPLETED));
                    return Behaviors.stopped();
                })
                .onMessage(Failed.class, msg -> {
                    log.warn("ReplayActor[{}]: replay failed: {}", id, msg.cause().getMessage());
                    parent.tell(new ReplayCoordinatorActor.ChildDone(id, sourceTopic, targetTopic, startedAt, sentCounter.get(), ReplayStatus.FAILED));
                    return Behaviors.stopped();
                })
                .build();
    }

    // -------------------------------------------------------------------------
    // stopping state — waiting for VT to pipe Finished/Failed back
    // -------------------------------------------------------------------------

    private static Behavior<Cmd> stopping(
            ActorContext<Cmd> ctx,
            String id,
            String sourceTopic,
            String targetTopic,
            Instant startedAt,
            AtomicLong sentCounter,
            ActorRef<ReplayCoordinatorActor.ChildDone> parent,
            ReplayStatus terminalStatus) {

        return Behaviors.receive(Cmd.class)
                .onMessage(GetStatus.class, msg -> {
                    msg.replyTo().tell(snapshot(id, sourceTopic, targetTopic, startedAt, sentCounter, terminalStatus));
                    return Behaviors.same();
                })
                .onMessage(Cancel.class, msg -> Behaviors.same())  // already stopping
                .onMessage(Finished.class, msg -> {
                    parent.tell(new ReplayCoordinatorActor.ChildDone(id, sourceTopic, targetTopic, startedAt, sentCounter.get(), terminalStatus));
                    return Behaviors.stopped();
                })
                .onMessage(Failed.class, msg -> {
                    parent.tell(new ReplayCoordinatorActor.ChildDone(id, sourceTopic, targetTopic, startedAt, sentCounter.get(), terminalStatus));
                    return Behaviors.stopped();
                })
                .build();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    static ReplayCoordinatorActor.ReplaySnapshot snapshot(
            String id, String sourceTopic, String targetTopic,
            Instant startedAt, AtomicLong sentCounter, ReplayStatus status) {
        return new ReplayCoordinatorActor.ReplaySnapshot(
                id, sourceTopic, targetTopic, startedAt, sentCounter.get(), status);
    }
}
