package com.joxette.replay;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

class ActiveReplayTrackerTest {

    @Test
    void runningEntryIsAlwaysPresent() {
        ActiveReplayTracker tracker = new ActiveReplayTracker();
        try (ActiveReplayTracker.Handle h = tracker.start("source", "target")) {
            List<ActiveReplayTracker.ActiveReplay> active = tracker.listActive();
            assertThat(active).hasSize(1);
            assertThat(active.get(0).status()).isEqualTo("running");
        }
    }

    @Test
    void completedEntryLingersThenEvicted() throws Exception {
        ActiveReplayTracker tracker = new ActiveReplayTracker();
        try (ActiveReplayTracker.Handle h = tracker.start("source", "target")) {
            h.accept(new ReplayProgress("completed", "target", 5, 0, null, null));
        }

        // Immediately after close: entry should still be visible (within linger window).
        List<ActiveReplayTracker.ActiveReplay> immediate = tracker.listActive();
        assertThat(immediate).hasSize(1);
        assertThat(immediate.get(0).status()).isEqualTo("completed");

        // Backdate the completedAt timestamp to simulate 31 s elapsed.
        backdateCompletedAt(tracker, 31);

        // Now listActive() should evict it.
        assertThat(tracker.listActive()).isEmpty();
    }

    @Test
    void failedEntryLingersThenEvicted() throws Exception {
        ActiveReplayTracker tracker = new ActiveReplayTracker();
        tracker.start("source", "target").close(); // close without accept → status=failed

        List<ActiveReplayTracker.ActiveReplay> immediate = tracker.listActive();
        assertThat(immediate).hasSize(1);
        assertThat(immediate.get(0).status()).isEqualTo("failed");

        backdateCompletedAt(tracker, 31);
        assertThat(tracker.listActive()).isEmpty();
    }

    @Test
    void entryNotEvictedBeforeLingerWindow() throws Exception {
        ActiveReplayTracker tracker = new ActiveReplayTracker();
        tracker.start("source", "target").close();

        backdateCompletedAt(tracker, 29); // 29 s < 30 s linger
        assertThat(tracker.listActive()).hasSize(1);
    }

    @Test
    void sentCountUpdatedViaAccept() {
        ActiveReplayTracker tracker = new ActiveReplayTracker();
        try (ActiveReplayTracker.Handle h = tracker.start("src", "tgt")) {
            h.accept(new ReplayProgress("running", "tgt", 42, 0, null, null));
            assertThat(tracker.listActive().get(0).sentCount()).isEqualTo(42);
        }
    }

    // ── Test helper ────────────────────────────────────────────────────────────

    /**
     * Uses reflection to backdate the {@code completedAt} field of the single
     * entry in the tracker's internal map by {@code secondsAgo} seconds.
     */
    @SuppressWarnings("unchecked")
    private static void backdateCompletedAt(ActiveReplayTracker tracker, long secondsAgo)
            throws Exception {
        Field entriesField = ActiveReplayTracker.class.getDeclaredField("entries");
        entriesField.setAccessible(true);
        ConcurrentHashMap<String, ?> entries =
                (ConcurrentHashMap<String, ?>) entriesField.get(tracker);
        for (Object entry : entries.values()) {
            Field completedAtField = entry.getClass().getDeclaredField("completedAt");
            completedAtField.setAccessible(true);
            completedAtField.set(entry, Instant.now().minusSeconds(secondsAgo));
        }
    }
}
