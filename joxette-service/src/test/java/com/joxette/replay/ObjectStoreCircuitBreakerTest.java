package com.joxette.replay;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link ObjectStoreCircuitBreaker} — shared fail-fast gate for object-store-touching
 * reads. Opens when a caller reports a confirmed failure (all retries exhausted),
 * closes again once the cooldown elapses or a success is explicitly recorded.
 */
class ObjectStoreCircuitBreakerTest {

    private static Clock fixedClockAt(Instant instant) {
        return Clock.fixed(instant, ZoneOffset.UTC);
    }

    @Test
    void startsClosed() {
        ObjectStoreCircuitBreaker breaker =
                new ObjectStoreCircuitBreaker(Duration.ofSeconds(5), fixedClockAt(Instant.EPOCH));

        assertThat(breaker.isOpen()).isFalse();
    }

    @Test
    void opensImmediatelyAfterRecordFailure() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        ObjectStoreCircuitBreaker breaker =
                new ObjectStoreCircuitBreaker(Duration.ofSeconds(5), fixedClockAt(now));

        breaker.recordFailure();

        assertThat(breaker.isOpen()).isTrue();
    }

    @Test
    void staysOpen_untilCooldownElapses() {
        Instant failureAt = Instant.parse("2026-01-01T00:00:00Z");
        java.util.concurrent.atomic.AtomicReference<Instant> now = new java.util.concurrent.atomic.AtomicReference<>(failureAt);
        Clock movableClock = new Clock() {
            @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(java.time.ZoneId zone) { throw new UnsupportedOperationException(); }
            @Override public Instant instant() { return now.get(); }
        };
        ObjectStoreCircuitBreaker breaker = new ObjectStoreCircuitBreaker(Duration.ofSeconds(5), movableClock);

        breaker.recordFailure();
        assertThat(breaker.isOpen()).isTrue();

        now.set(failureAt.plusSeconds(3));
        assertThat(breaker.isOpen()).as("still within the 5s cooldown").isTrue();

        now.set(failureAt.plusSeconds(5).plusMillis(1));
        assertThat(breaker.isOpen()).as("cooldown has elapsed").isFalse();
    }

    @Test
    void recordSuccess_closesAnOpenBreakerImmediately() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        ObjectStoreCircuitBreaker breaker =
                new ObjectStoreCircuitBreaker(Duration.ofSeconds(5), fixedClockAt(now));

        breaker.recordFailure();
        assertThat(breaker.isOpen()).isTrue();

        breaker.recordSuccess();

        assertThat(breaker.isOpen()).isFalse();
    }

    @Test
    void recordFailure_calledAgainWhileOpen_extendsTheCooldownFromNow() {
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        java.util.concurrent.atomic.AtomicReference<Instant> now = new java.util.concurrent.atomic.AtomicReference<>(t0);
        Clock movableClock = new Clock() {
            @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(java.time.ZoneId zone) { throw new UnsupportedOperationException(); }
            @Override public Instant instant() { return now.get(); }
        };
        ObjectStoreCircuitBreaker breaker = new ObjectStoreCircuitBreaker(Duration.ofSeconds(5), movableClock);

        breaker.recordFailure();
        now.set(t0.plusSeconds(4));
        breaker.recordFailure(); // still failing — cooldown should restart from t0+4s

        now.set(t0.plusSeconds(8));
        assertThat(breaker.isOpen()).as("cooldown restarted at t0+4s, so t0+8s is only 4s in").isTrue();

        now.set(t0.plusSeconds(9).plusMillis(1));
        assertThat(breaker.isOpen()).isFalse();
    }

    @Test
    void lastFailure_returnsMostRecentRecordedCause() {
        ObjectStoreCircuitBreaker breaker =
                new ObjectStoreCircuitBreaker(Duration.ofSeconds(5), fixedClockAt(Instant.EPOCH));

        assertThat(breaker.lastFailure()).isNull();

        RuntimeException cause = new RuntimeException("store down");
        breaker.recordFailure(cause);

        assertThat(breaker.lastFailure()).isSameAs(cause);
    }
}
