package com.joxette.replay;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Shared fail-fast gate for object-store-touching reads (see
 * {@link TopicReplayService#withObjectStoreRetry}).
 *
 * <p>Opens when a caller reports a confirmed failure — {@code withObjectStoreRetry}
 * calls {@link #recordFailure} only after its own retry budget is exhausted, which is
 * the strongest signal available that the object store is genuinely down right now,
 * not just a one-off blip. While open, every subsequent read fails fast — no query
 * attempt, no retry loop — instead of each independently rediscovering and retrying
 * the same outage. That's what collapses a single page load's "storm" of
 * near-simultaneous retry-and-fail sequences (one per REST call it fires) into a
 * single failure.
 *
 * <p>Deliberately global, not keyed per query description: the object store is one
 * shared dependency, so once any read confirms it's down, every other read should
 * benefit from that knowledge immediately, not just repeats of the same query.
 *
 * <p>Thread-safe: {@link #isOpen}, {@link #recordFailure} and {@link #recordSuccess}
 * are called concurrently from every request-handling virtual thread.
 */
final class ObjectStoreCircuitBreaker {

    private final Duration cooldown;
    private final Clock clock;
    private final AtomicLong openUntilMillis = new AtomicLong(0);
    private final AtomicReference<Throwable> lastFailure = new AtomicReference<>();

    ObjectStoreCircuitBreaker(Duration cooldown) {
        this(cooldown, Clock.systemUTC());
    }

    ObjectStoreCircuitBreaker(Duration cooldown, Clock clock) {
        this.cooldown = cooldown;
        this.clock = clock;
    }

    /** {@code true} while a recorded failure's cooldown hasn't elapsed yet. */
    boolean isOpen() {
        return clock.millis() < openUntilMillis.get();
    }

    /** Opens (or re-opens, restarting the cooldown from now) the circuit. */
    void recordFailure() {
        openUntilMillis.set(clock.millis() + cooldown.toMillis());
    }

    /** Opens the circuit and remembers {@code cause} for {@link #lastFailure()}. */
    void recordFailure(Throwable cause) {
        lastFailure.set(cause);
        recordFailure();
    }

    /** Closes the circuit immediately, ahead of the natural cooldown expiry. */
    void recordSuccess() {
        openUntilMillis.set(0);
    }

    /** Most recent cause passed to {@link #recordFailure(Throwable)}, or {@code null}. */
    Throwable lastFailure() {
        return lastFailure.get();
    }

    /** Test seam: force-closes the circuit and clears the remembered cause. */
    void resetForTests() {
        openUntilMillis.set(0);
        lastFailure.set(null);
    }
}
