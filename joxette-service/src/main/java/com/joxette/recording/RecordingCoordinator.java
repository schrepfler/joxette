package com.joxette.recording;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

/**
 * Thin Spring adapter over {@link RecordingCoordinatorActor}.
 *
 * <p>All public methods delegate to the actor via ask-pattern.  Spring beans
 * that previously depended on the old coordinator ({@link com.joxette.management.TopicController},
 * {@link com.joxette.management.HealthController},
 * {@link com.joxette.management.RecordingStartupRunner}) continue to work unchanged.
 *
 * <p>The actual lifecycle supervision and Resilience4j-style retry is now handled
 * by Pekko's exponential-backoff supervisor declared in
 * {@link TopicLifecycleActor#create}.
 */
@Lazy
@Component
public class RecordingCoordinator {

    private static final Logger log = LoggerFactory.getLogger(RecordingCoordinator.class);

    /** Longer timeout for stop: must exceed the child actor's internal stop ask (10 s) plus drain time. */
    private static final java.time.Duration STOP_TIMEOUT = java.time.Duration.ofSeconds(30);

    private final ActorRef<RecordingCoordinatorActor.CoordinatorCommand> coordinatorActor;
    private final ActorSystem<?> system;

    public RecordingCoordinator(
            ActorRef<RecordingCoordinatorActor.CoordinatorCommand> coordinatorActor,
            ActorSystem<?> system) {
        this.coordinatorActor = coordinatorActor;
        this.system           = system;
    }

    // -----------------------------------------------------------------------
    // Public API (unchanged contract)
    // -----------------------------------------------------------------------

    public boolean startTopic(String topic, String startFrom) {
        RecordingCoordinatorActor.StartReply reply = ask(
                ref -> new RecordingCoordinatorActor.StartTopic(topic, startFrom, ref));
        return reply instanceof RecordingCoordinatorActor.Started s && s.newScope();
    }

    public boolean startTopic(String topic) {
        return startTopic(topic, "latest");
    }

    public boolean stopTopic(String topic) {
        RecordingCoordinatorActor.StopReply reply = askWithTimeout(
                ref -> new RecordingCoordinatorActor.StopTopic(topic, ref), STOP_TIMEOUT);
        return reply instanceof RecordingCoordinatorActor.StopComplete sc && sc.wasStopped();
    }

    public boolean restartTopic(String topic) {
        RecordingCoordinatorActor.StartReply reply = ask(
                ref -> new RecordingCoordinatorActor.RestartTopic(topic, ref));
        return reply instanceof RecordingCoordinatorActor.Started s && s.newScope();
    }

    public Set<String> activeTopics() {
        Set<String> result = ask(RecordingCoordinatorActor.ActiveTopics::new);
        return result != null ? result : Set.of();
    }

    public Map<String, RecorderStatus> listRunning() {
        Map<String, RecorderStatus> result = ask(RecordingCoordinatorActor.ListRunning::new);
        return result != null ? result : Map.of();
    }

    public void stopAll() {
        log.info("RecordingCoordinator: stopping all active recorder scopes");
        Set<String> topics = activeTopics();
        log.info("RecordingCoordinator: stopping {} topic(s)", topics.size());
        for (String topic : topics) {
            stopTopic(topic);
        }
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private <R> R ask(java.util.function.Function<ActorRef<R>, RecordingCoordinatorActor.CoordinatorCommand> msgFactory) {
        return askWithTimeout(msgFactory, RecordingCoordinatorActor.ASK_TIMEOUT);
    }

    @SuppressWarnings("unchecked")
    private <R> R askWithTimeout(
            java.util.function.Function<ActorRef<R>, RecordingCoordinatorActor.CoordinatorCommand> msgFactory,
            java.time.Duration timeout) {
        try {
            return (R) AskPattern.ask(
                    coordinatorActor,
                    msgFactory::apply,
                    timeout,
                    system.scheduler()
            ).toCompletableFuture().join();
        } catch (java.util.concurrent.CompletionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof java.util.concurrent.TimeoutException
                    && cause.getMessage() != null
                    && cause.getMessage().contains("had already been terminated")) {
                // The coordinator is mid-restart — return null; callers handle null safely
                // via the actor's exponential-backoff supervisor coming back up shortly.
                log.debug("RecordingCoordinator: ask skipped — coordinator actor is restarting");
                return null;
            }
            throw ex;
        }
    }
}
