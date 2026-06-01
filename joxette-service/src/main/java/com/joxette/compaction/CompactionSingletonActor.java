package com.joxette.compaction;

import com.joxette.config.JoxetteProperties;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.TimerScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.support.CronExpression;

import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Pekko ClusterSingleton behavior for compaction.
 *
 * <p>Exactly one instance of this actor runs across the cluster at any time.
 * It owns the compaction schedule (replacing {@code CompactionScheduler}) and
 * guarantees that only one run executes globally (replacing {@code CompactionLockManager}).
 *
 * <h2>States</h2>
 * <ul>
 *   <li>{@code idle} — waiting for the next scheduled or manual trigger</li>
 *   <li>{@code running} — compaction VT is executing; manual triggers are rejected</li>
 * </ul>
 *
 * <h2>Scheduling</h2>
 * <p>On entering {@code idle} (including the initial state), the actor parses the
 * cron expression from {@code joxette.compaction.schedule}, computes the delay until
 * the next fire, and sets a single-shot timer.  On each {@link ScheduledRun} message
 * it re-schedules the next slot before starting the run.
 *
 * <h2>Thread safety</h2>
 * <p>All state mutations happen in the actor's single-threaded mailbox.
 * The actual compaction work runs on a virtual thread via {@code context.pipeToSelf}.
 */
public class CompactionSingletonActor {

    private static final Logger log = LoggerFactory.getLogger(CompactionSingletonActor.class);

    // -------------------------------------------------------------------------
    // Command / reply protocol
    // -------------------------------------------------------------------------

    public sealed interface CompactionCommand {}

    public record TriggerCompaction(
            List<String> targets,
            TriggerSource triggeredBy,
            ActorRef<TriggerReply> replyTo
    ) implements CompactionCommand {}

    private record ScheduledRun() implements CompactionCommand {}
    private record RunCompleted(long runId) implements CompactionCommand {}
    private record RunErrored(long runId, Throwable cause) implements CompactionCommand {}

    public sealed interface TriggerReply {}
    public record CompactionAccepted(CompactionRun run) implements TriggerReply {}
    public record CompactionBusy() implements TriggerReply {}

    static final Object SCHEDULE_KEY = "compaction-schedule";

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    public static Behavior<CompactionCommand> create(
            CompactionService service,
            JoxetteProperties props,
            Executor vtExecutor) {
        return Behaviors.setup(ctx ->
            Behaviors.withTimers(timers -> {
                scheduleNext(timers, props);
                return idle(ctx, timers, service, props, vtExecutor);
            })
        );
    }

    // -------------------------------------------------------------------------
    // States
    // -------------------------------------------------------------------------

    private static Behavior<CompactionCommand> idle(
            ActorContext<CompactionCommand> ctx,
            TimerScheduler<CompactionCommand> timers,
            CompactionService service,
            JoxetteProperties props,
            Executor vtExecutor) {
        return Behaviors.receive(CompactionCommand.class)
                .onMessage(ScheduledRun.class, msg -> {
                    scheduleNext(timers, props);
                    return beginRun(ctx, timers, TriggerSource.SCHEDULED, null, null, service, props, vtExecutor);
                })
                .onMessage(TriggerCompaction.class, msg ->
                        beginRun(ctx, timers, msg.triggeredBy(), msg.targets(), msg.replyTo(),
                                service, props, vtExecutor))
                .onMessage(RunCompleted.class, msg -> Behaviors.same())
                .onMessage(RunErrored.class, msg -> Behaviors.same())
                .build();
    }

    private static Behavior<CompactionCommand> running(
            ActorContext<CompactionCommand> ctx,
            TimerScheduler<CompactionCommand> timers,
            long runId,
            CompactionService service,
            JoxetteProperties props,
            Executor vtExecutor) {
        return Behaviors.receive(CompactionCommand.class)
                .onMessage(TriggerCompaction.class, msg -> {
                    msg.replyTo().tell(new CompactionBusy());
                    return Behaviors.same();
                })
                .onMessage(ScheduledRun.class, msg -> {
                    // Missed scheduled slot while running — just re-schedule.
                    scheduleNext(timers, props);
                    return Behaviors.same();
                })
                .onMessage(RunCompleted.class, msg -> {
                    if (msg.runId() == runId) {
                        log.info("Compaction singleton: run {} completed", runId);
                        return idle(ctx, timers, service, props, vtExecutor);
                    }
                    return Behaviors.same();
                })
                .onMessage(RunErrored.class, msg -> {
                    if (msg.runId() == runId) {
                        log.error("Compaction singleton: run {} errored", runId, msg.cause());
                        return idle(ctx, timers, service, props, vtExecutor);
                    }
                    return Behaviors.same();
                })
                .build();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static Behavior<CompactionCommand> beginRun(
            ActorContext<CompactionCommand> ctx,
            TimerScheduler<CompactionCommand> timers,
            TriggerSource triggeredBy,
            List<String> targets,
            ActorRef<TriggerReply> replyTo,
            CompactionService service,
            JoxetteProperties props,
            Executor vtExecutor) {

        CompactionRun run;
        try {
            run = service.beginRun(triggeredBy, targets);
        } catch (com.joxette.api.error.ConflictException e) {
            if (replyTo != null) replyTo.tell(new CompactionBusy());
            return Behaviors.same();
        } catch (SQLException e) {
            log.error("Compaction singleton: failed to insert run record ({}); skipping", e.getMessage());
            if (replyTo != null) replyTo.tell(new CompactionBusy());
            return Behaviors.same();
        }

        if (replyTo != null) replyTo.tell(new CompactionAccepted(run));

        final long runId = run.id();
        final List<String> finalTargets = targets;
        ctx.pipeToSelf(
                CompletableFuture.supplyAsync(() -> {
                    service.executeRun(runId, finalTargets);
                    return new RunCompleted(runId);
                }, vtExecutor),
                (result, ex) -> ex != null ? new RunErrored(runId, ex) : result
        );

        return running(ctx, timers, runId, service, props, vtExecutor);
    }

    private static void scheduleNext(TimerScheduler<CompactionCommand> timers, JoxetteProperties props) {
        String expr = props.getCompaction().getSchedule();
        try {
            CronExpression cron = CronExpression.parse(expr);
            LocalDateTime next = cron.next(LocalDateTime.now());
            if (next == null) {
                log.warn("Compaction singleton: cron '{}' has no next fire time; scheduled runs disabled", expr);
                return;
            }
            Duration delay = Duration.between(LocalDateTime.now(), next);
            if (delay.isNegative() || delay.isZero()) delay = Duration.ofSeconds(1);
            timers.startSingleTimer(SCHEDULE_KEY, new ScheduledRun(), delay);
            log.debug("Compaction singleton: next scheduled run in {} (cron: {})", delay, expr);
        } catch (Exception e) {
            log.warn("Compaction singleton: cannot parse cron '{}': {}; scheduled runs disabled", expr, e.getMessage());
        }
    }
}
