package com.joxette.replay;

import com.joxette.api.error.UpstreamUnavailableException;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code withObjectStoreRetry} wraps the object-storage-touching queries in
 * {@link TopicReplayService} and {@link EntityReplayService}: transient DuckDB
 * httpfs I/O failures (e.g. RustFS/MinIO briefly dropping a connection — real
 * production case: "IO Error: Server returned nothing (no headers, no data)")
 * get a few quick retries before surfacing to the caller as a typed
 * {@link UpstreamUnavailableException} instead of a raw 500.
 */
class TopicReplayServiceObjectStoreRetryTest {

    @BeforeEach
    void resetCircuit() {
        TopicReplayService.resetObjectStoreCircuitForTests();
    }

    private static DataAccessException ioError(String detail) {
        return new DataAccessException("SQL [...]", new SQLException("IO Error: " + detail));
    }

    private static DataAccessException nonIoError(String detail) {
        return new DataAccessException("SQL [...]", new SQLException("Binder Error: " + detail));
    }

    private static DataAccessException abortedTransaction() {
        return new DataAccessException("SQL [...]", new SQLException(
                "TransactionContext Error: Current transaction is aborted (please ROLLBACK)"));
    }

    @Test
    void returnsResultImmediately_whenSupplierSucceedsOnFirstAttempt() {
        AtomicInteger calls = new AtomicInteger();
        String result = TopicReplayService.withObjectStoreRetry("test", () -> {
            calls.incrementAndGet();
            return "ok";
        }, 0);

        assertThat(result).isEqualTo("ok");
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void retriesTransientIoError_thenReturnsResultOnceItSucceeds() {
        AtomicInteger calls = new AtomicInteger();
        String result = TopicReplayService.withObjectStoreRetry("test", () -> {
            int attempt = calls.incrementAndGet();
            if (attempt < 3) throw ioError("Server returned nothing (no headers, no data)");
            return "ok";
        }, 0);

        assertThat(result).isEqualTo("ok");
        assertThat(calls.get()).isEqualTo(3);
    }

    @Test
    void doesNotRetry_nonTransientError_rethrowsImmediately() {
        AtomicInteger calls = new AtomicInteger();
        DataAccessException thrown = nonIoError("column \"bogus\" not found");

        assertThatThrownBy(() -> TopicReplayService.withObjectStoreRetry("test", () -> {
            calls.incrementAndGet();
            throw thrown;
        }, 0)).isSameAs(thrown);

        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void exhaustsRetries_thenThrowsUpstreamUnavailableWrappingLastError() {
        AtomicInteger calls = new AtomicInteger();
        DataAccessException lastError = ioError("Server returned nothing (no headers, no data)");

        assertThatThrownBy(() -> TopicReplayService.withObjectStoreRetry("queryEntityEvents:fixture", () -> {
            calls.incrementAndGet();
            throw lastError;
        }, 0))
                .isInstanceOf(UpstreamUnavailableException.class)
                .hasCause(lastError)
                .hasMessageContaining("queryEntityEvents:fixture");

        assertThat(calls.get()).isEqualTo(TopicReplayService.OBJECT_STORE_RETRY_ATTEMPTS);
    }

    @Test
    void detectsTransientError_evenWhenNestedDeeperInCauseChain() {
        DataAccessException wrapped = new DataAccessException("outer", ioError("Timeout was reached"));
        AtomicInteger calls = new AtomicInteger();

        String result = TopicReplayService.withObjectStoreRetry("test", () -> {
            if (calls.incrementAndGet() < 2) throw wrapped;
            return "ok";
        }, 0);

        assertThat(result).isEqualTo("ok");
        assertThat(calls.get()).isEqualTo(2);
    }

    @Test
    void defaultEntryPoint_usesRealBackoffAndAttemptCount() {
        // Exercises the real (no explicit delay) overload used by production call sites —
        // confirms it delegates to the same attempt-count constant, just with real backoff.
        AtomicInteger calls = new AtomicInteger();
        String result = TopicReplayService.withObjectStoreRetry("test", () -> {
            int attempt = calls.incrementAndGet();
            if (attempt < TopicReplayService.OBJECT_STORE_RETRY_ATTEMPTS) throw ioError("Timeout was reached");
            return "ok";
        });

        assertThat(result).isEqualTo("ok");
        assertThat(calls.get()).isEqualTo(TopicReplayService.OBJECT_STORE_RETRY_ATTEMPTS);
    }

    @Test
    void exhaustingRetries_opensCircuit_soNextCallFailsFastWithoutInvokingSupplier() {
        DataAccessException lastError = ioError("Server returned nothing (no headers, no data)");
        assertThatThrownBy(() -> TopicReplayService.withObjectStoreRetry("first-call", () -> {
            throw lastError;
        }, 0)).isInstanceOf(UpstreamUnavailableException.class);

        AtomicInteger secondCallSupplierInvocations = new AtomicInteger();
        assertThatThrownBy(() -> TopicReplayService.withObjectStoreRetry("second-call", () -> {
            secondCallSupplierInvocations.incrementAndGet();
            return "should never get here";
        }, 0)).isInstanceOf(UpstreamUnavailableException.class);

        assertThat(secondCallSupplierInvocations.get())
                .as("circuit should be open from the first call's exhaustion — second call must fail fast")
                .isZero();
    }

    @Test
    void successAfterCircuitReset_closesCircuitAgain() {
        DataAccessException lastError = ioError("Connection timed out");
        assertThatThrownBy(() -> TopicReplayService.withObjectStoreRetry("first-call", () -> {
            throw lastError;
        }, 0)).isInstanceOf(UpstreamUnavailableException.class);

        // Simulate the cooldown having elapsed (the test seam used by @BeforeEach in
        // every other test) rather than sleeping for the real cooldown duration.
        TopicReplayService.resetObjectStoreCircuitForTests();

        AtomicInteger calls = new AtomicInteger();
        String result = TopicReplayService.withObjectStoreRetry("second-call", () -> {
            calls.incrementAndGet();
            return "ok";
        }, 0);

        assertThat(result).isEqualTo("ok");
        assertThat(calls.get()).isEqualTo(1);
    }

    /**
     * The failure mode that turned a transient hiccup into an unhandled 500: attempt 1
     * failed with a real {@code IO Error}, that failure aborted a transaction somebody
     * had left open on the shared connection, and attempt 2 then failed with
     * {@code "Current transaction is aborted"} — which did not look like an object-store
     * error, so it was rethrown as a bug instead of retried, and the circuit breaker
     * never got to open.  {@code DuckDbSession} now rolls the aborted transaction back on
     * the way out of attempt 1, so attempt 2 has a clean connection to retry on — but
     * only if the retry loop recognises the message and keeps going.
     */
    @Test
    void retriesAbortedTransaction_becauseTheConnectionIsRolledBackBetweenAttempts() {
        AtomicInteger calls = new AtomicInteger();
        String result = TopicReplayService.withObjectStoreRetry("queryEntityEvents:fixture", () -> {
            int attempt = calls.incrementAndGet();
            if (attempt == 1) throw ioError("Server returned nothing (no headers, no data)");
            if (attempt == 2) throw abortedTransaction();
            return "ok";
        }, 0);

        assertThat(result).isEqualTo("ok");
        assertThat(calls.get()).isEqualTo(3);
    }

    @Test
    void abortedTransactionAloneIsRetried_ratherThanRethrownAsABug() {
        AtomicInteger calls = new AtomicInteger();
        String result = TopicReplayService.withObjectStoreRetry("test", () -> {
            if (calls.incrementAndGet() < 2) throw abortedTransaction();
            return "ok";
        }, 0);

        assertThat(result).isEqualTo("ok");
        assertThat(calls.get()).isEqualTo(2);
    }

    @Test
    void retryAttempts_and_backoff_giveAboutThreeAndAHalfSecondsOfPatience() {
        // Locks in the "more patience" tuning: 4 attempts, exponential backoff
        // 500ms / 1s / 2s between them (~3.5s total) instead of the old
        // 3-attempt / ~0.9s-total budget — interactive reads can afford to wait a
        // few seconds longer than they used to before giving up.
        assertThat(TopicReplayService.OBJECT_STORE_RETRY_ATTEMPTS).isEqualTo(4);
        assertThat(TopicReplayService.computeBackoffMs(500, 1)).isEqualTo(500);
        assertThat(TopicReplayService.computeBackoffMs(500, 2)).isEqualTo(1000);
        assertThat(TopicReplayService.computeBackoffMs(500, 3)).isEqualTo(2000);
    }
}
