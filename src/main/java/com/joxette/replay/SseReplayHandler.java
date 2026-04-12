package com.joxette.replay;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Handles SSE ({@code text/event-stream}) and NDJSON
 * ({@code application/x-ndjson}) streaming for cassette replay endpoints.
 *
 * <p>Both formats share the same {@link RecordStreamer} abstraction: callers
 * provide a lambda that feeds records to a {@link Consumer} sink; this class
 * wraps the sink with format-specific serialisation.
 */
@Component
public class SseReplayHandler {

    /**
     * Checked streaming callback: implementations call {@code sink.accept(record)}
     * for each record and may throw {@link SQLException}.
     */
    @FunctionalInterface
    public interface RecordStreamer<T> {
        void stream(Consumer<T> sink) throws SQLException;
    }

    private final ObjectMapper objectMapper;

    public SseReplayHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Creates a {@link SseEmitter} that asynchronously streams records as SSE
     * events on a virtual thread. Each record is serialised as a JSON data line.
     */
    public <T> SseEmitter streamSse(RecordStreamer<T> streamer) {
        SseEmitter emitter = new SseEmitter(0L); // no timeout
        Thread.ofVirtual().start(() -> {
            try {
                streamer.stream(record -> {
                    try {
                        emitter.send(SseEmitter.event()
                                .data(objectMapper.writeValueAsString(record))
                                .build());
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                });
                emitter.complete();
            } catch (java.io.UncheckedIOException e) {
                emitter.completeWithError(e.getCause());
            } catch (Exception e) {
                emitter.completeWithError(e);
            }
        });
        return emitter;
    }

    /**
     * Returns a {@link StreamingResponseBody} that writes records as newline-
     * delimited JSON. Each line is flushed immediately so clients receive
     * incremental output.
     */
    public <T> StreamingResponseBody streamNdjson(RecordStreamer<T> streamer) {
        return outputStream -> {
            var writer = new BufferedWriter(
                    new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
            try {
                streamer.stream(record -> {
                    try {
                        writer.write(objectMapper.writeValueAsString(record));
                        writer.newLine();
                        writer.flush();
                    } catch (JsonProcessingException e) {
                        throw new java.io.UncheckedIOException(new IOException(e));
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                });
            } catch (java.io.UncheckedIOException e) {
                throw e.getCause();
            } catch (SQLException e) {
                throw new IOException(e);
            }
        };
    }

    /**
     * Creates a {@link SseEmitter} for a scheduled replay.
     *
     * <p>Immediately sends a {@code scheduled} SSE event with the resolved start time,
     * then blocks until {@code scheduledAt} is reached (or until the replay is cancelled).
     * Once the start time arrives, records are streamed as normal SSE data events.
     * If the replay is cancelled while waiting, a {@code cancelled} event is sent.
     *
     * <p>If the SSE client disconnects before streaming begins,
     * {@link ScheduledReplayService#cancelIfPending(String)} is called automatically.
     */
    public <T> SseEmitter streamSseScheduled(
            String id,
            Instant scheduledAt,
            ScheduledReplayService schedService,
            RecordStreamer<T> streamer) {
        SseEmitter emitter = new SseEmitter(0L);
        emitter.onError(t -> schedService.cancelIfPending(id));
        Thread.ofVirtual().start(() -> {
            try {
                String scheduledJson = objectMapper.writeValueAsString(
                        Map.of("id", id, "scheduledAt", scheduledAt.toString()));
                emitter.send(SseEmitter.event().name("scheduled").data(scheduledJson).build());

                long delayMs = Math.max(0L, scheduledAt.toEpochMilli() - Instant.now().toEpochMilli());
                boolean proceed = schedService.awaitStart(id, delayMs);
                if (!proceed) {
                    try {
                        emitter.send(SseEmitter.event().name("cancelled")
                                .data("{\"id\":\"" + id + "\"}").build());
                    } catch (Exception ignored) { /* client may have disconnected */ }
                    try { emitter.complete(); } catch (Exception ignored) {}
                    return;
                }

                schedService.markStreaming(id);
                streamer.stream(record -> {
                    try {
                        emitter.send(SseEmitter.event()
                                .data(objectMapper.writeValueAsString(record))
                                .build());
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                });
                schedService.markCompleted(id);
                emitter.complete();
            } catch (java.io.UncheckedIOException e) {
                schedService.markFailed(id);
                emitter.completeWithError(e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                schedService.markFailed(id);
                emitter.completeWithError(e);
            } catch (Exception e) {
                schedService.markFailed(id);
                emitter.completeWithError(e);
            }
        });
        return emitter;
    }

    /**
     * Returns a {@link StreamingResponseBody} for a scheduled replay.
     *
     * <p>Immediately writes a {@code scheduled} NDJSON line with the resolved start time,
     * then blocks until {@code scheduledAt} is reached (or until cancelled).
     * Once the start time arrives, records are streamed as normal NDJSON lines.
     * If cancelled while waiting, a {@code cancelled} NDJSON line is written and the stream ends.
     */
    public <T> StreamingResponseBody streamNdjsonScheduled(
            String id,
            Instant scheduledAt,
            ScheduledReplayService schedService,
            RecordStreamer<T> streamer) {
        return outputStream -> {
            var writer = new BufferedWriter(
                    new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
            try {
                String scheduledJson = objectMapper.writeValueAsString(
                        Map.of("event", "scheduled", "id", id, "scheduledAt", scheduledAt.toString()));
                writer.write(scheduledJson);
                writer.newLine();
                writer.flush();

                long delayMs = Math.max(0L, scheduledAt.toEpochMilli() - Instant.now().toEpochMilli());
                boolean proceed = schedService.awaitStart(id, delayMs);
                if (!proceed) {
                    writer.write("{\"event\":\"cancelled\",\"id\":\"" + id + "\"}");
                    writer.newLine();
                    writer.flush();
                    return;
                }

                schedService.markStreaming(id);
                streamer.stream(record -> {
                    try {
                        writer.write(objectMapper.writeValueAsString(record));
                        writer.newLine();
                        writer.flush();
                    } catch (JsonProcessingException e) {
                        throw new java.io.UncheckedIOException(new IOException(e));
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                });
                schedService.markCompleted(id);
            } catch (java.io.UncheckedIOException e) {
                schedService.markFailed(id);
                throw e.getCause();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                schedService.markFailed(id);
                throw new IOException("Interrupted while awaiting scheduled replay start", e);
            } catch (SQLException e) {
                schedService.markFailed(id);
                throw new IOException(e);
            }
        };
    }
}
