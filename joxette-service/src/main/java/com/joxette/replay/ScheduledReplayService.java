package com.joxette.replay;

import com.joxette.config.JoxetteProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * In-memory registry for pending and active scheduled replays.
 *
 * <p>Scheduled replays are not persisted — a service restart silently drops them all.
 * The number of concurrent scheduled replays is bounded by
 * {@code joxette.replay.max-scheduled} (default: 50).
 *
 * <h2>Lifecycle</h2>
 * <pre>
 *   register → pending → streaming → (removed)
 *                     ↘ cancelled → (removed)
 * </pre>
 */
@Component
public class ScheduledReplayService {

    private static final Logger log = LoggerFactory.getLogger(ScheduledReplayService.class);

    private final int maxScheduled;
    private final ConcurrentHashMap<String, ReplayEntry> entries = new ConcurrentHashMap<>();

    // -----------------------------------------------------------------------
    // Internal mutable state per replay
    // -----------------------------------------------------------------------

    static final class ReplayEntry {
        final ScheduledReplay meta;
        /** Counted down to 1→0 when the replay is cancelled; used to unblock {@link #awaitStart}. */
        final CountDownLatch cancelLatch = new CountDownLatch(1);
        volatile String status = "pending";

        ReplayEntry(ScheduledReplay meta) {
            this.meta = meta;
        }

        /**
         * Blocks for up to {@code delayMs} milliseconds, or until the replay is cancelled.
         *
         * @return {@code true} if the scheduled time elapsed and streaming should proceed;
         *         {@code false} if the replay was cancelled before the scheduled time.
         */
        boolean awaitStart(long delayMs) throws InterruptedException {
            if (delayMs <= 0) return !"cancelled".equals(status);
            // await() returns true when latch hits 0 (cancelled), false on timeout
            boolean cancelled = cancelLatch.await(delayMs, TimeUnit.MILLISECONDS);
            return !cancelled && !"cancelled".equals(status);
        }

        ScheduledReplay snapshot() {
            return new ScheduledReplay(
                    meta.id(), meta.kind(), meta.topic(), meta.entityType(), meta.entityId(),
                    meta.from(), meta.to(), meta.partition(), meta.offsetFrom(), meta.offsetTo(),
                    meta.scheduledAt(), meta.createdAt(), status);
        }
    }

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    public ScheduledReplayService(JoxetteProperties props) {
        this.maxScheduled = props.getReplay().getMaxScheduled();
    }

    // -----------------------------------------------------------------------
    // Registration
    // -----------------------------------------------------------------------

    public String registerTopicReplay(String topic, Instant scheduledAt,
                                       Instant from, Instant to,
                                       Integer partition, Long offsetFrom, Long offsetTo) {
        String id = UUID.randomUUID().toString();
        ScheduledReplay meta = new ScheduledReplay(
                id, "topic", topic, null, null,
                from, to, partition, offsetFrom, offsetTo,
                scheduledAt, Instant.now(), "pending");
        checkCapacity();
        entries.put(id, new ReplayEntry(meta));
        log.info("Registered scheduled topic replay [{}] topic={} at {}", id, topic, scheduledAt);
        return id;
    }

    public String registerEntityReplay(String entityType, String entityId, Instant scheduledAt,
                                        Instant from, Instant to) {
        String id = UUID.randomUUID().toString();
        ScheduledReplay meta = new ScheduledReplay(
                id, "entity", null, entityType, entityId,
                from, to, null, null, null,
                scheduledAt, Instant.now(), "pending");
        checkCapacity();
        entries.put(id, new ReplayEntry(meta));
        log.info("Registered scheduled entity replay [{}] {}:{} at {}", id, entityType, entityId, scheduledAt);
        return id;
    }

    private void checkCapacity() {
        long active = entries.values().stream()
                .filter(e -> "pending".equals(e.status) || "streaming".equals(e.status))
                .count();
        if (active >= maxScheduled) {
            log.warn("Scheduled replay capacity reached: {} active out of max {}", active, maxScheduled);
            throw com.joxette.api.error.ConflictException.scheduledReplayCapacityReached(maxScheduled);
        }
    }

    // -----------------------------------------------------------------------
    // Wait / cancel
    // -----------------------------------------------------------------------

    /**
     * Blocks until the scheduled start time (i.e. {@code delayMs} milliseconds from now)
     * or until the replay is cancelled — whichever comes first.
     *
     * @return {@code true} if the scheduled time was reached and streaming should proceed;
     *         {@code false} if the replay was cancelled.
     */
    public boolean awaitStart(String id, long delayMs) throws InterruptedException {
        ReplayEntry entry = entries.get(id);
        if (entry == null) return false;
        return entry.awaitStart(delayMs);
    }

    /**
     * Cancels a scheduled replay, unblocking any thread waiting in {@link #awaitStart}.
     * Removes the entry from the registry.
     *
     * @throws NoSuchElementException  if {@code id} is unknown
     * @throws IllegalStateException   if the replay is not in a cancellable state
     */
    public void cancel(String id) {
        ReplayEntry entry = entries.get(id);
        if (entry == null) throw com.joxette.api.error.ResourceNotFoundException.scheduledReplay(id);
        if (!"pending".equals(entry.status) && !"streaming".equals(entry.status)) {
            throw com.joxette.api.error.ConflictException.scheduledReplayCannotCancel(entry.status);
        }
        entry.status = "cancelled";
        entry.cancelLatch.countDown();
        entries.remove(id);
        log.info("Cancelled scheduled replay [{}]", id);
    }

    /**
     * Cancels the replay if it is still in {@code pending} status.
     * No-op if already streaming, cancelled, or unknown. Called when the SSE client disconnects.
     */
    public void cancelIfPending(String id) {
        ReplayEntry entry = entries.get(id);
        if (entry != null && "pending".equals(entry.status)) {
            entry.status = "cancelled";
            entry.cancelLatch.countDown();
            entries.remove(id);
            log.debug("Auto-cancelled pending scheduled replay [{}] (client disconnected)", id);
        }
    }

    // -----------------------------------------------------------------------
    // Status transitions (called by streaming handlers)
    // -----------------------------------------------------------------------

    public void markStreaming(String id) {
        ReplayEntry entry = entries.get(id);
        if (entry != null) {
            entry.status = "streaming";
            log.debug("Scheduled replay [{}] transitioned to streaming", id);
        }
    }

    public void markCompleted(String id) {
        entries.remove(id);
        log.debug("Scheduled replay [{}] completed", id);
    }

    public void markFailed(String id) {
        entries.remove(id);
        log.debug("Scheduled replay [{}] failed", id);
    }

    // -----------------------------------------------------------------------
    // Queries
    // -----------------------------------------------------------------------

    /** Returns all pending and streaming scheduled replays, ordered by scheduled start time. */
    public List<ScheduledReplay> list() {
        return entries.values().stream()
                .map(ReplayEntry::snapshot)
                .sorted(Comparator.comparing(ScheduledReplay::scheduledAt))
                .toList();
    }

    /**
     * Returns a snapshot of the scheduled replay with the given id.
     *
     * @throws NoSuchElementException if unknown
     */
    public ScheduledReplay get(String id) {
        ReplayEntry entry = entries.get(id);
        if (entry == null) throw com.joxette.api.error.ResourceNotFoundException.scheduledReplay(id);
        return entry.snapshot();
    }
}
