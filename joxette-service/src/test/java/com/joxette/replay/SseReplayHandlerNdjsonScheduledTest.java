package com.joxette.replay;

import com.joxette.config.JoxetteProperties;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import tools.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises {@link SseReplayHandler#streamNdjsonScheduled}'s line shapes.
 * As with the follow variant, no test covered this method's NDJSON output
 * before this migration -- this is new coverage.
 */
class SseReplayHandlerNdjsonScheduledTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final SseReplayHandler handler = new SseReplayHandler(mapper);
    private final ScheduledReplayService schedService = new ScheduledReplayService(new JoxetteProperties());

    private static String render(StreamingResponseBody body) throws IOException {
        var bos = new ByteArrayOutputStream();
        body.writeTo(bos);
        return bos.toString(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Test
    void proceedsImmediatelyWhenScheduledTimeAlreadyPassedThenStreamsRecords() throws Exception {
        String id = schedService.registerTopicReplay(
                "orders.events", Instant.now().minusSeconds(1), null, null, null, null, null);

        StreamingResponseBody body = handler.<String>streamNdjsonScheduled(
                id, Instant.now().minusSeconds(1), schedService,
                sink -> { sink.accept("one"); sink.accept("two"); });

        String payload = render(body);
        var lines = payload.lines().toList();

        assertThat(lines).hasSize(3);
        assertThat(lines.get(0)).contains("\"event\":\"scheduled\"").contains("\"id\":\"" + id + "\"");
        assertThat(lines.get(1)).isEqualTo("\"one\"");
        assertThat(lines.get(2)).isEqualTo("\"two\"");
    }

    @Test
    void cancelledBeforeStartEmitsCancelledLineAndNoRecords() throws Exception {
        String id = schedService.registerTopicReplay(
                "orders.events", Instant.now().plusSeconds(60), null, null, null, null, null);
        schedService.cancel(id);

        StreamingResponseBody body = handler.<String>streamNdjsonScheduled(
                id, Instant.now().plusSeconds(60), schedService,
                sink -> sink.accept("should-not-appear"));

        String payload = render(body);
        var lines = payload.lines().toList();

        assertThat(lines).hasSize(2);
        assertThat(lines.get(0)).contains("\"event\":\"scheduled\"");
        assertThat(lines.get(1)).isEqualTo("{\"event\":\"cancelled\",\"id\":\"" + id + "\"}");
    }

    @Test
    void midStreamFailureEmitsErrorFrame() throws Exception {
        String id = schedService.registerTopicReplay(
                "orders.events", Instant.now().minusSeconds(1), null, null, null, null, null);

        StreamingResponseBody body = handler.<String>streamNdjsonScheduled(
                id, Instant.now().minusSeconds(1), schedService,
                sink -> { sink.accept("one"); throw new RuntimeException("boom"); });

        String payload = render(body);
        var lines = payload.lines().toList();

        assertThat(lines).hasSize(3);
        assertThat(lines.get(1)).isEqualTo("\"one\"");
        assertThat(lines.get(2)).contains("\"_error\"").contains("\"status\":500");
    }
}
