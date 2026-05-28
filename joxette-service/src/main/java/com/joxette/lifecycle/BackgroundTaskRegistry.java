package com.joxette.lifecycle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Central registry for ad-hoc background virtual threads that need coordinated shutdown.
 *
 * <p>Each task submitted via {@link #submit(String, Runnable)} is wrapped so that the
 * registry entry is removed automatically when the task finishes (normally, by exception,
 * or by interrupt).  On Spring context shutdown this bean interrupts all tracked threads
 * and joins them with a shared 30 s deadline before allowing the shutdown to proceed.
 *
 * <p>Phase ordering relative to other {@link SmartLifecycle} beans:
 * <ol>
 *   <li>{@code Integer.MAX_VALUE - 1024} — {@code SseReplayHandler} (replay SSE streams)
 *   <li>{@code Integer.MAX_VALUE - 2048} — this registry (exports, retention, live-metrics)
 *   <li>{@code @PreDestroy PekkoConfig} — recorders, write channel, Pekko shutdown
 * </ol>
 */
@Component
public class BackgroundTaskRegistry implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(BackgroundTaskRegistry.class);

    static final int PHASE = Integer.MAX_VALUE - 2048;
    private static final long STOP_TIMEOUT_MS = 30_000L;

    private final ConcurrentHashMap<UUID, TaskHandle> tasks = new ConcurrentHashMap<>();
    private volatile boolean accepting = true;
    private volatile boolean running   = false;

    /** Immutable snapshot of a running background task. */
    public record TaskHandle(UUID id, String name, Instant startedAt, Thread thread) {
        /** Requests cancellation by interrupting the task's virtual thread. */
        public void cancel() { thread.interrupt(); }
    }

    /**
     * Submits a named background task for execution on a new virtual thread.
     *
     * @param name short human-readable label (used in logs and health responses)
     * @param task the work to run; must honour {@link Thread#isInterrupted()} for prompt shutdown
     * @return a handle that can be used to cancel the task or identify it in health output
     * @throws IllegalStateException if the registry is already shutting down
     */
    public TaskHandle submit(String name, Runnable task) {
        if (!accepting) {
            throw new IllegalStateException(
                    "BackgroundTaskRegistry is shutting down; rejecting task: " + name);
        }
        UUID id = UUID.randomUUID();
        Instant startedAt = Instant.now();
        Thread vt = Thread.ofVirtual().name(name).unstarted(() -> {
            try {
                task.run();
            } finally {
                tasks.remove(id);
            }
        });
        TaskHandle handle = new TaskHandle(id, name, startedAt, vt);
        tasks.put(id, handle);
        vt.start();
        return handle;
    }

    /** Returns a snapshot of all currently running tasks. */
    public List<TaskHandle> getRunningTasks() {
        return List.copyOf(tasks.values());
    }

    // ── SmartLifecycle ──────────────────────────────────────────────────────────

    @Override
    public void start() {
        running   = true;
        accepting = true;
    }

    @Override
    public boolean isRunning() { return running; }

    @Override
    public boolean isAutoStartup() { return true; }

    @Override
    public int getPhase() { return PHASE; }

    @Override
    public void stop() {
        running   = false;
        accepting = false;

        Collection<TaskHandle> snapshot = List.copyOf(tasks.values());
        if (snapshot.isEmpty()) return;

        log.info("BackgroundTaskRegistry: interrupting {} in-flight task(s)", snapshot.size());
        snapshot.forEach(h -> h.thread().interrupt());

        long deadline = System.currentTimeMillis() + STOP_TIMEOUT_MS;
        for (TaskHandle h : snapshot) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                log.warn("BackgroundTaskRegistry: stop timeout reached; {} task(s) may still be alive",
                        tasks.size());
                break;
            }
            try {
                h.thread().join(remaining);
                if (h.thread().isAlive()) {
                    log.warn("BackgroundTaskRegistry: task '{}' ({}) did not stop within deadline",
                            h.name(), h.id());
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
