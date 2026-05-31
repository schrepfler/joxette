package com.joxette.replay;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.testkit.typed.javadsl.TestProbe;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ReplayCoordinatorActor} state via the
 * {@link ActiveReplayTracker} façade, using Pekko's {@link ActorTestKit}.
 *
 * <p>VT lifecycle is simulated by sending {@link ReplayCoordinatorActor.ChildDone}
 * directly so tests stay deterministic without spawning real VTs.
 */
class ActiveReplayTrackerTest {

    private ActorTestKit kit;
    private ActorRef<ReplayCoordinatorActor.Cmd> coordinator;
    private ActiveReplayTracker tracker;

    @BeforeEach
    void setUp() {
        kit = ActorTestKit.create();
        coordinator = kit.spawn(ReplayCoordinatorActor.create(), "coordinator");

        tracker = new ActiveReplayTracker(coordinator, kit.system());
    }

    @AfterEach
    void tearDown() {
        kit.shutdownTestKit();
    }

    @Test
    void runningEntryIsAlwaysPresent() {
        String id = startReplay("source", "target");

        List<ActiveReplayTracker.ActiveReplay> active = tracker.listActive();
        assertThat(active).hasSize(1);
        assertThat(active.get(0).id()).isEqualTo(id);
        assertThat(active.get(0).status()).isEqualTo("running");

        // Clean up
        coordinator.tell(new ReplayCoordinatorActor.CancelReplay(id));
    }

    @Test
    void completedEntryLingersAfterChildDone() {
        String id = startReplay("source", "target");

        // Simulate VT completing normally
        coordinator.tell(new ReplayCoordinatorActor.ChildDone(
                id, "source", "target", Instant.now(), 5L, "completed"));

        // Linger window: entry should still be visible immediately
        List<ActiveReplayTracker.ActiveReplay> lingering = tracker.listActive();
        assertThat(lingering).hasSize(1);
        assertThat(lingering.get(0).status()).isEqualTo("completed");
        assertThat(lingering.get(0).sentCount()).isEqualTo(5L);
    }

    @Test
    void failedEntryLingers() {
        String id = startReplay("source", "target");

        coordinator.tell(new ReplayCoordinatorActor.ChildDone(
                id, "source", "target", Instant.now(), 0L, "failed"));

        List<ActiveReplayTracker.ActiveReplay> lingering = tracker.listActive();
        assertThat(lingering).hasSize(1);
        assertThat(lingering.get(0).status()).isEqualTo("failed");
    }

    @Test
    void cancelledEntryLingers() {
        String id = startReplay("source", "target");
        coordinator.tell(new ReplayCoordinatorActor.CancelReplay(id));

        // After cancel, ChildDone arrives from the actor with "cancelled" status
        coordinator.tell(new ReplayCoordinatorActor.ChildDone(
                id, "source", "target", Instant.now(), 3L, "cancelled"));

        List<ActiveReplayTracker.ActiveReplay> lingering = tracker.listActive();
        assertThat(lingering).hasSize(1);
        assertThat(lingering.get(0).status()).isEqualTo("cancelled");
    }

    @Test
    void multipleRunningReplaysAllListed() {
        String id1 = startReplay("topic-a", "out");
        String id2 = startReplay("topic-b", "out");

        List<ActiveReplayTracker.ActiveReplay> active = tracker.listActive();
        assertThat(active).hasSize(2);
        assertThat(active).extracting(ActiveReplayTracker.ActiveReplay::status)
                .containsOnly("running");

        coordinator.tell(new ReplayCoordinatorActor.CancelReplay(id1));
        coordinator.tell(new ReplayCoordinatorActor.CancelReplay(id2));
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    /**
     * Tells the coordinator to start a no-op replay and returns the assigned id.
     * Uses a work lambda that parks immediately so it doesn't race with test assertions.
     */
    private String startReplay(String sourceTopic, String targetTopic) {
        TestProbe<String> probe = kit.createTestProbe(String.class);
        coordinator.tell(new ReplayCoordinatorActor.StartReplay(
                sourceTopic, targetTopic, null,
                sink -> Thread.currentThread().join(),   // park until interrupted
                Executors.newVirtualThreadPerTaskExecutor(),
                probe.ref()));
        return probe.receiveMessage(Duration.ofSeconds(3));
    }
}
