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
 * <p>Wraps a {@link CassetteRecordingBus.Subscription} and tracks the
 * <b>maximum</b> cursor (in natural order) ever emitted to the client.  The
 * max-cursor is the boundary between the historical drain (served by the
 * paginated query) and the live tail (served by the bus): a record from the
 * bus is forwarded only if its cursor is strictly greater than the max so far.
 *
 * <p>The rule is identical for both {@link Order#ASC} and {@link Order#DESC}
 * query ordering:
 * <ul>
 *   <li>ASC drain: emissions grow monotonically; the max tracks the latest
 *       emitted record. A bus record is forwarded iff newer than the drain's
 *       high-water mark.</li>
 *   <li>DESC drain: emissions shrink monotonically; the max is set by the
 *       first (newest) drain emission and stays there.  A bus record is
 *       forwarded iff newer than the newest historical — i.e. it's a live
 *       record that was published after the drain snapshot.</li>
 * </ul>
 * A {@link Comparator} is still parameterised at construction time to allow
 * future extensions (e.g. pluggable cursor shapes), but the canonical
 * construction uses {@link Comparator#naturalOrder()} which matches both
 * directions correctly.
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
    private final Comparator<C> boundaryComparator;
    private final AtomicBoolean closed = new AtomicBoolean();

    private C maxEmittedCursor;

    private FollowSubscription(CassetteRecordingBus.Subscription subscription,
                                BlockingQueue<R> queue,
                                Function<R, C> cursorOf,
                                Comparator<C> boundaryComparator) {
        this.subscription = Objects.requireNonNull(subscription);
        this.queue = Objects.requireNonNull(queue);
        this.cursorOf = Objects.requireNonNull(cursorOf);
        this.boundaryComparator = Objects.requireNonNull(boundaryComparator);
    }

    /** Natural-order {@link #forTopic(CassetteRecordingBus.TopicSubscription, Order)} for ASC. */
    public static FollowSubscription<CassetteRecord, TopicCursor> forTopic(
            CassetteRecordingBus.TopicSubscription sub) {
        return forTopic(sub, Order.ASC);
    }

    /**
     * Wraps a topic subscription.  The {@code order} parameter is accepted for
     * API symmetry with the service layer; boundary tracking itself always
     * uses natural-order max, which is correct for both ASC and DESC (see
     * class javadoc).
     */
    public static FollowSubscription<CassetteRecord, TopicCursor> forTopic(
            CassetteRecordingBus.TopicSubscription sub, Order order) {
        return new FollowSubscription<>(sub, sub.queue(),
                r -> new TopicCursor(r.timestamp(), r.partition(), r.offset()),
                Comparator.<TopicCursor>naturalOrder());
    }

    /** Natural-order {@link #forEntity(CassetteRecordingBus.EntitySubscription, Order)} for ASC. */
    public static FollowSubscription<EntityRecord, EntityCursor> forEntity(
            CassetteRecordingBus.EntitySubscription sub) {
        return forEntity(sub, Order.ASC);
    }

    /** See {@link #forTopic(CassetteRecordingBus.TopicSubscription, Order)}. */
    public static FollowSubscription<EntityRecord, EntityCursor> forEntity(
            CassetteRecordingBus.EntitySubscription sub, Order order) {
        return new FollowSubscription<>(sub, sub.queue(),
                r -> new EntityCursor(
                        r.timestamp(), r.recordedAt(),
                        r.topic(), r.partition(), r.offset()),
                Comparator.<EntityCursor>naturalOrder());
    }

    /**
     * Advances the boundary cursor after the caller has emitted {@code record}
     * to the client.  Called by the replay service for every record handed to
     * the sink during the historical drain AND during the live tail.
     *
     * <p>Stores the maximum cursor observed in natural order.  For ASC streams
     * this grows monotonically; for DESC streams the first (newest) emission
     * pins the boundary and subsequent older emissions leave it unchanged.
     */
    public void onEmitted(R record) {
        C c = cursorOf.apply(record);
        if (maxEmittedCursor == null || c.compareTo(maxEmittedCursor) > 0) {
            maxEmittedCursor = c;
        }
    }

    /**
     * Drains any records already buffered on the bus queue, skipping those
     * whose cursor is less-than-or-equal-to the max emitted so far (the
     * historical drain already delivered them).  Remaining records are emitted
     * in arrival order and advance the boundary.
     *
     * <p>Invoked once, immediately after the historical pagination completes
     * and before the live {@link #awaitNext(Duration)} loop begins.
     */
    public void drainBuffered(Consumer<R> sink) {
        R record;
        while ((record = queue.poll()) != null) {
            if (shouldEmit(record)) {
                sink.accept(record);
                maxEmittedCursor = cursorOf.apply(record);
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
                maxEmittedCursor = cursorOf.apply(record);
                return record;
            }
            // duplicate — skip and keep waiting on the remaining budget
        }
    }

    /** True if the underlying bus subscription dropped a record due to full queue. */
    public boolean isOverflowed() {
        return subscription.isOverflowed();
    }

    /**
     * The maximum-cursor (natural order) boundary, or {@code null} if no
     * record has been emitted yet.  Named {@code lastEmittedCursor} for
     * historical reasons and test backwards-compatibility; for ASC streams it
     * is literally the cursor of the last emission, for DESC streams it is
     * the cursor of the first (newest) emission, which stays pinned as the
     * high-water mark.
     */
    public C lastEmittedCursor() {
        return maxEmittedCursor;
    }

    /** Idempotent: unregisters from the bus and releases the queue. */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            subscription.close();
            queue.clear();
        }
    }

    private boolean shouldEmit(R record) {
        if (maxEmittedCursor == null) return true;
        C rc = cursorOf.apply(record);
        return boundaryComparator.compare(rc, maxEmittedCursor) > 0;
    }
}
