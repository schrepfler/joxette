package com.joxette.compaction;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link CompactionService.LockHeartbeat} in isolation from any real
 * {@link CompactionLockManager} / DuckDB wiring.
 *
 * <p>{@link CompactionService#doCompactEntityType} and
 * {@link CompactionService#doCompactGeneralTopic} start one of these alongside every
 * held lock so a legitimately long {@code ducklake_merge_adjacent_files} call does not
 * let the lock's TTL expire out from under it (see
 * {@code CompactionLockManager#HEARTBEAT_INTERVAL_MINUTES} and
 * {@link CompactionLockManager#refresh}). These tests use a short, test-injected
 * interval instead of the real 10-minute one — proving the mechanism fires
 * periodically and stops cleanly does not require waiting out a real interval.
 */
class CompactionHeartbeatTest {

    private static final String TARGET = "entity:order";

    @Test
    void heartbeat_callsRefreshRepeatedlyWhileRunning() {
        AtomicInteger refreshCount = new AtomicInteger();
        CompactionLockManager lockManager = mock(CompactionLockManager.class);
        try {
            doAnswer(inv -> {
                refreshCount.incrementAndGet();
                return null;
            }).when(lockManager).refresh(anyString());
        } catch (Exception e) {
            throw new AssertionError(e); // refresh() declares SQLException; mock stubbing never throws here
        }

        CompactionService.LockHeartbeat heartbeat =
                new CompactionService.LockHeartbeat(lockManager, TARGET, Duration.ofMillis(20));
        try {
            await().atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> assertThat(refreshCount.get())
                            .as("heartbeat must call refresh() repeatedly while running")
                            .isGreaterThanOrEqualTo(3));
        } finally {
            heartbeat.stop();
        }
    }

    @Test
    void heartbeat_stopsCallingRefreshAfterStop() {
        AtomicInteger refreshCount = new AtomicInteger();
        CompactionLockManager lockManager = mock(CompactionLockManager.class);
        try {
            doAnswer(inv -> {
                refreshCount.incrementAndGet();
                return null;
            }).when(lockManager).refresh(anyString());
        } catch (Exception e) {
            throw new AssertionError(e);
        }

        CompactionService.LockHeartbeat heartbeat =
                new CompactionService.LockHeartbeat(lockManager, TARGET, Duration.ofMillis(20));

        await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(refreshCount.get()).isGreaterThanOrEqualTo(2));

        // stop() blocks until the loop has actually exited (see LockHeartbeat.stop()
        // javadoc), so the count is stable the instant it returns — no in-flight
        // refresh() call can land afterwards.
        heartbeat.stop();
        int countAtStop = refreshCount.get();

        // Give the (fully-stopped) loop a window during which it must NOT fire again —
        // a defensive re-check on top of the deterministic guarantee above.
        await().pollDelay(Duration.ofMillis(200))
                .atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(refreshCount.get())
                        .as("no refresh() calls should occur after stop() interrupts the loop")
                        .isEqualTo(countAtStop));
    }
}
