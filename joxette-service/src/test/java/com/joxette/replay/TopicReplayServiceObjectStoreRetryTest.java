package com.joxette.replay;

import com.joxette.api.error.UpstreamUnavailableException;
import org.jooq.exception.DataAccessException;
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

    private static DataAccessException ioError(String detail) {
        return new DataAccessException("SQL [...]", new SQLException("IO Error: " + detail));
    }

    private static DataAccessException nonIoError(String detail) {
        return new DataAccessException("SQL [...]", new SQLException("Binder Error: " + detail));
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
}
