package com.joxette.replay;

import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import tools.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises {@link SseReplayHandler#streamNdjsonFollow}'s line shapes and
 * cleanup contract. No prior test covered this method's NDJSON output
 * directly (only the SSE follow path had coverage) -- this is new coverage
 * added alongside the jox-json migration, not a pre-existing regression net.
 */
class SseReplayHandlerNdjsonFollowTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final SseReplayHandler handler = new SseReplayHandler(mapper);

    private static String render(StreamingResponseBody body) throws IOException {
        var bos = new ByteArrayOutputStream();
        body.writeTo(bos);
        return bos.toString(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Test
    void emitsFollowPreambleThenRecordsThenHeartbeatAndOverflow() throws Exception {
        AtomicBoolean closed = new AtomicBoolean();
        StreamingResponseBody body = handler.<String>streamNdjsonFollow(
                Duration.ofSeconds(5),
                () -> closed.set(true),
                (sink, hooks) -> {
                    hooks.onHistoricalEnd();
                    sink.accept("one");
                    hooks.onHeartbeat();
                    sink.accept("two");
                    hooks.onOverflow();
                });

        String payload = render(body);
        var lines = payload.lines().toList();

        assertThat(lines).hasSize(5);
        assertThat(lines.get(0)).isEqualTo("{\"event\":\"follow\"}");
        assertThat(lines.get(1)).isEqualTo("\"one\"");
        assertThat(lines.get(2)).contains("\"event\":\"heartbeat\"").contains("\"ts\":");
        assertThat(lines.get(3)).isEqualTo("\"two\"");
        assertThat(lines.get(4)).isEqualTo("{\"event\":\"overflow\",\"reason\":\"buffer overflow\"}");
        assertThat(closed).as("onClose must run after the stream ends").isTrue();
    }

    @Test
    void midStreamFailureEmitsErrorFrameAndStillRunsCleanup() throws Exception {
        AtomicBoolean closed = new AtomicBoolean();
        StreamingResponseBody body = handler.<String>streamNdjsonFollow(
                Duration.ofSeconds(5),
                () -> closed.set(true),
                (sink, hooks) -> {
                    sink.accept("one");
                    throw new java.sql.SQLException("boom");
                });

        String payload = render(body);
        var lines = payload.lines().toList();

        assertThat(lines).hasSize(2);
        assertThat(lines.get(0)).isEqualTo("\"one\"");
        assertThat(lines.get(1)).contains("\"_error\"").contains("\"status\":500");
        assertThat(closed).as("onClose must run even after a mid-stream failure").isTrue();
    }
}
