package com.joxette.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that the virtual thread spawned by {@link SseReplayHandler#streamSse}
 * terminates and is deregistered from {@code activeThreads} once the record
 * source is exhausted, fails mid-stream, or is interrupted via
 * {@link SseReplayHandler#stop()}.
 *
 * <p>{@link org.springframework.web.servlet.mvc.method.annotation.SseEmitter#onCompletion}
 * requires Spring's async servlet machinery and does not fire in plain unit tests.
 * We therefore drive synchronisation via a per-test {@link CountDownLatch} inside
 * the streamer (to confirm the VT has started / reached a checkpoint), then use
 * Awaitility to poll {@link SseReplayHandler#activeThreadCount()} until it drops to zero.
 */
class SseReplayHandlerLifecycleTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(2);

    private final ObjectMapper mapper = new ObjectMapper();
    private final SseReplayHandler handler = new SseReplayHandler(mapper);

    // -------------------------------------------------------------------------
    // Normal completion
    // -------------------------------------------------------------------------

    @Test
    void vtIsDeregisteredAfterNormalCompletion() throws InterruptedException {
        CountDownLatch streamerDone = new CountDownLatch(1);

        handler.<String>streamSse(sink -> {
            sink.accept("a");
            sink.accept("b");
            streamerDone.countDown();
        });

        assertThat(streamerDone.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .as("streamer should finish within %s", TIMEOUT)
                .isTrue();
        await().atMost(TIMEOUT)
                .untilAsserted(() ->
                        assertThat(handler.activeThreadCount())
                                .as("VT must be deregistered after normal stream completion")
                                .isZero());
    }

    @Test
    void vtIsDeregisteredAfterNormalCompletionWithPreamble() throws InterruptedException {
        CountDownLatch streamerDone = new CountDownLatch(1);

        handler.<String>streamSse("transform", "{}", sink -> {
            sink.accept("x");
            streamerDone.countDown();
        });

        assertThat(streamerDone.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isTrue();
        await().atMost(TIMEOUT)
                .untilAsserted(() -> assertThat(handler.activeThreadCount()).isZero());
    }

    // -------------------------------------------------------------------------
    // Mid-stream failure paths
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "mid-stream {0} → VT deregistered")
    @CsvSource({
        "RuntimeException",
        "SQLException",
    })
    void vtIsDeregisteredAfterMidStreamFailure(String failureKind) throws InterruptedException {
        CountDownLatch failureReached = new CountDownLatch(1);

        SseReplayHandler.RecordStreamer<String> streamer = sink -> {
            sink.accept("one");
            failureReached.countDown();
            if ("SQLException".equals(failureKind)) {
                throw new SQLException("injected");
            } else {
                throw new RuntimeException("injected");
            }
        };

        handler.<String>streamSse(streamer);

        assertThat(failureReached.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .as("streamer should reach the failure point within %s", TIMEOUT)
                .isTrue();
        await().atMost(TIMEOUT)
                .untilAsserted(() ->
                        assertThat(handler.activeThreadCount())
                                .as("VT must be deregistered even when the stream fails mid-way")
                                .isZero());
    }

    // -------------------------------------------------------------------------
    // Interrupted via stop() (shutdown simulation)
    // -------------------------------------------------------------------------

    @Test
    void vtIsDeregisteredAfterShutdownInterrupt() throws InterruptedException {
        CountDownLatch streamingStarted = new CountDownLatch(1);

        // Streamer signals when it has started and then blocks until interrupted.
        SseReplayHandler.RecordStreamer<String> blocking = sink -> {
            sink.accept("first");
            streamingStarted.countDown();
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        handler.<String>streamSse(blocking);

        assertThat(streamingStarted.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .as("VT should reach the blocking sleep within %s", TIMEOUT)
                .isTrue();
        assertThat(handler.activeThreadCount()).isGreaterThanOrEqualTo(1);

        handler.stop();

        await().atMost(TIMEOUT)
                .untilAsserted(() ->
                        assertThat(handler.activeThreadCount())
                                .as("VT must be deregistered after shutdown interrupt")
                                .isZero());
    }
}
