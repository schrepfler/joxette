package com.joxette.replay;

import com.joxette.recording.CassetteRecordingBus;

import java.time.Duration;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Per-stream state for a {@code follow=true} replay session.
 *
 * <p>Wraps a {@link CassetteRecordingBus.Subscription} and tracks the cursor of
 * the last record emitted to the client.  The cursor is the boundary between
 * the historical drain (served by the paginated query) and the live tail
 * (served by the bus).  When a record arrives on the bus whose cursor is
 * less-than-or-equal-to {@code lastEmittedCursor}, it is a duplicate already
 * observed by the drain and is discarded.
 *
 * <p>Two concrete parameterisations are used by the replay services:
 * <ul>
 *   <li>{@code FollowSubscription<CassetteRecord, TopicCursor>} for general cassettes</li>
 *   <li>{@code FollowSubscription<EntityRecord, EntityCursor>} for entity cassettes</li>
 * </ul>
 *
 * <p>This class is not thread-safe for concurrent callers; a single streaming
 * virtual thread owns it for its lifetime.  Close is idempotent and safe from
 * any thread (typically the SSE emitter completion hook).
 */
public final class FollowSubscription<R, C extends Comparable<C>> {

    private final CassetteRecordingBus.Subscription subscription;
    private final BlockingQueue<R> queue;
    private final Function<R, C> cursorOf;
    private final AtomicBoolean closed = new AtomicBoolean();

    private C lastEmittedCursor;

    private FollowSubscription(CassetteRecordingBus.Subscription subscription,
                                BlockingQueue<R> queue,
                                Function<R, C> cursorOf) {
        this.subscription = Objects.requireNonNull(subscription);
        this.queue = Objects.requireNonNull(queue);
        this.cursorOf = Objects.requireNonNull(cursorOf);
    }

    /**
     * Wraps a topic subscription.  The cursor is derived from the emitted
     * {@link CassetteRecord} as {@code (timestamp, partition, offset)}.
     */
    public static FollowSubscription<CassetteRecord, TopicCursor> forTopic(
            CassetteRecordingBus.TopicSubscription sub) {
        return new FollowSubscription<>(sub, sub.queue(),
                r -> new TopicCursor(r.timestamp(), r.partition(), r.offset()));
    }

    /**
     * Wraps an entity subscription.  The cursor is derived from the emitted
     * {@link EntityRecord} as {@code (timestamp, recordedAt, sourceTopic,
     * sourcePartition, sourceOffset)}.
     */
    public static FollowSubscription<EntityRecord, EntityCursor> forEntity(
            CassetteRecordingBus.EntitySubscription sub) {
        return new FollowSubscription<>(sub, sub.queue(),
                r -> new EntityCursor(
                        r.timestamp(), r.recordedAt(),
                        r.topic(), r.partition(), r.offset()));
    }

    /**
     * Advances the boundary cursor after the caller has emitted {@code record}
     * to the client.  Called by the replay service for every record handed to
     * the sink during the historical drain AND during the live tail.
     */
    public void onEmitted(R record) {
        lastEmittedCursor = cursorOf.apply(record);
    }

    /**
     * Drains any records already buffered on the bus queue, skipping those
     * whose cursor is less-than-or-equal-to {@link #lastEmittedCursor()} (the
     * historical drain already delivered them).  Remaining records are emitted
     * in arrival order and advance the cursor.
     *
     * <p>Invoked once, immediately after the historical pagination completes
     * and before the live {@link #awaitNext(Duration)} loop begins.
     */
    public void drainBuffered(Consumer<R> sink) {
        R record;
        while ((record = queue.poll()) != null) {
            if (shouldEmit(record)) {
                sink.accept(record);
                lastEmittedCursor = cursorOf.apply(record);
            }
        }
    }

    /**
     * Blocks up to {@code heartbeatTimeout} waiting for the next live record.
     * Returns the record (already past the cursor boundary, advances the
     * cursor) or {@code null} on timeout so the caller can emit a heartbeat
     * and loop.  Any duplicate that arrives before the timeout is silently
     * skipped without consuming the timeout budget of later records.
     */
    public R awaitNext(Duration heartbeatTimeout) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + heartbeatTimeout.toNanos();
        while (true) {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) return null;
            R record = queue.poll(remaining, TimeUnit.NANOSECONDS);
            if (record == null) return null;
            if (shouldEmit(record)) {
                lastEmittedCursor = cursorOf.apply(record);
                return record;
            }
            // duplicate — skip and keep waiting on the remaining budget
        }
    }

    /** True if the underlying bus subscription dropped a record due to full queue. */
    public boolean isOverflowed() {
        return subscription.isOverflowed();
    }

    /** Current boundary cursor, or {@code null} if no record has been emitted yet. */
    public C lastEmittedCursor() {
        return lastEmittedCursor;
    }

    /** Idempotent: unregisters from the bus and releases the queue. */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            subscription.close();
            queue.clear();
        }
    }

    private boolean shouldEmit(R record) {
        if (lastEmittedCursor == null) return true;
        C rc = cursorOf.apply(record);
        return Comparator.<C>naturalOrder().compare(rc, lastEmittedCursor) > 0;
    }
}
