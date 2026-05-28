package com.joxette.lifecycle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class BackgroundTaskRegistryTest {

    private BackgroundTaskRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new BackgroundTaskRegistry();
        registry.start();
    }

    @Test
    void phaseIsCorrect() {
        assertThat(registry.getPhase()).isEqualTo(Integer.MAX_VALUE - 512);
    }

    @Test
    void submitTracksTaskAndSelfRemovesOnCompletion() throws Exception {
        CountDownLatch started  = new CountDownLatch(1);
        CountDownLatch proceed  = new CountDownLatch(1);

        registry.submit("test-task", () -> {
            started.countDown();
            try { proceed.await(); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
        });

        assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(registry.getRunningTasks()).hasSize(1);
        assertThat(registry.getRunningTasks().get(0).name()).isEqualTo("test-task");

        proceed.countDown();
        await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(registry.getRunningTasks()).isEmpty());
    }

    @Test
    void taskSelfRemovesOnException() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        registry.submit("failing-task", () -> {
            try { throw new RuntimeException("boom"); }
            finally { done.countDown(); }
        });

        assertThat(done.await(2, TimeUnit.SECONDS)).isTrue();
        await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(registry.getRunningTasks()).isEmpty());
    }

    @Test
    void stopInterruptsTrackedTasks() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        registry.submit("long-task", () -> {
            started.countDown();
            try { Thread.sleep(Long.MAX_VALUE); } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        });

        assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
        registry.stop();

        assertThat(registry.getRunningTasks()).isEmpty();
    }

    @Test
    void submitAfterStopThrows() {
        registry.stop();
        assertThatThrownBy(() -> registry.submit("rejected", () -> {}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("shutting down");
    }

    @Test
    void multipleTasksAllStopWithinDeadline() throws Exception {
        int count = 5;
        CountDownLatch allStarted = new CountDownLatch(count);

        for (int i = 0; i < count; i++) {
            registry.submit("task-" + i, () -> {
                allStarted.countDown();
                try { Thread.sleep(100); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        assertThat(allStarted.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(registry.getRunningTasks()).hasSize(count);

        long start = System.currentTimeMillis();
        registry.stop();
        long elapsed = System.currentTimeMillis() - start;

        assertThat(registry.getRunningTasks()).isEmpty();
        assertThat(elapsed).isLessThan(2_000);
    }

    @Test
    void isRunningReflectsLifecycleState() {
        assertThat(registry.isRunning()).isTrue();
        registry.stop();
        assertThat(registry.isRunning()).isFalse();
    }

    @Test
    void getRunningTasksIncludesNameAndStartedAt() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);

        registry.submit("named-task", () -> {
            started.countDown();
            try { proceed.await(); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
        });

        assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
        List<BackgroundTaskRegistry.TaskHandle> tasks = registry.getRunningTasks();
        assertThat(tasks).hasSize(1);
        BackgroundTaskRegistry.TaskHandle h = tasks.get(0);
        assertThat(h.name()).isEqualTo("named-task");
        assertThat(h.id()).isNotNull();
        assertThat(h.startedAt()).isNotNull();
        assertThat(h.thread()).isNotNull();

        proceed.countDown();
        registry.stop();
    }
}
