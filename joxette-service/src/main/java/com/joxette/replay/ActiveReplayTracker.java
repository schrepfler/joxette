package com.joxette.replay;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.actor.typed.ActorSystem;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * Thin façade over {@link ReplayCoordinatorActor} that keeps the same
 * {@link #listActive()} API used by {@code InstanceController} and the
 * live flow-map SSE endpoint.
 *
 * <p>The {@code ActiveReplay} record mirrors {@code ReplayStatus} so
 * callers (including the UI client type generated from the API) are unchanged.
 */
@Component
public class ActiveReplayTracker {

    public record ActiveReplay(
            String id,
            String sourceTopic,
            String targetTopic,
            Instant startedAt,
            long sentCount,
            ReplayStatus status
    ) {}

    private final ActorRef<ReplayCoordinatorActor.Cmd> coordinator;
    private final ActorSystem<?> system;

    public ActiveReplayTracker(
            ActorRef<ReplayCoordinatorActor.Cmd> replayCoordinator,
            ActorSystem<Void> actorSystem) {
        this.coordinator = replayCoordinator;
        this.system      = actorSystem;
    }

    public List<ActiveReplay> listActive() {
        List<ReplayCoordinatorActor.ReplaySnapshot> statuses = AskPattern.ask(
                coordinator,
                ReplayCoordinatorActor.ListActive::new,
                ReplayCoordinatorActor.ASK_TIMEOUT,
                system.scheduler()
        ).toCompletableFuture().join();

        return statuses.stream()
                .map(s -> new ActiveReplay(s.id(), s.sourceTopic(), s.targetTopic(),
                        s.startedAt(), s.sentCount(), s.status()))
                .toList();
    }
}
