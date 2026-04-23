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
import java.time.Duration;
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
        return streamSse(null, null, streamer);
    }

    /**
     * Creates a {@link SseEmitter} that first emits a named preamble event (when
     * {@code preambleEventName} is non-null), then streams all data records.
     *
     * <p>Used to emit a {@code transform} event at stream start when a user pipeline
     * is active, so clients can detect that records have been processed.
     *
     * @param preambleEventName SSE event name for the preamble (e.g. {@code "transform"}),
     *                          or {@code null} to skip the preamble
     * @param preambleData      pre-serialised JSON string for the preamble data field;
     *                          ignored when {@code preambleEventName} is null
     * @param streamer          record source
     */
    public <T> SseEmitter streamSse(
            String preambleEventName, String preambleData, RecordStreamer<T> streamer) {
        SseEmitter emitter = new SseEmitter(0L); // no timeout
        Thread.ofVirtual().start(() -> {
            try {
                if (preambleEventName != null && preambleData != null) {
                    emitter.send(SseEmitter.event()
                            .name(preambleEventName)
                            .data(preambleData)
                            .build());
                }
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
        return streamNdjson(null, streamer);
    }

    /**
     * Returns a {@link StreamingResponseBody} that first writes a preamble line
     * (when non-null), then streams all data records as newline-delimited JSON.
     *
     * <p>Used to emit a {@code {"event":"transform",...}} line at stream start when
     * a user pipeline is active.
     *
     * @param preambleLine pre-serialised JSON string to write as the first line,
     *                     or {@code null} to skip
     * @param streamer     record source
     */
    public <T> StreamingResponseBody streamNdjson(String preambleLine, RecordStreamer<T> streamer) {
        return outputStream -> {
            var writer = new BufferedWriter(
                    new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
            try {
                if (preambleLine != null) {
                    writer.write(preambleLine);
                    writer.newLine();
                    writer.flush();
                }
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
     * SSE streaming variant for {@code follow=true} replays.
     *
     * <p>Invokes {@code streamer} with a sink and a {@link FollowHooks}; the
     * streamer first drains history through the sink, then (via the hooks)
     * emits a {@code follow} preamble event, then enters the live tail loop.
     * During the live loop, idle periods emit a {@code :heartbeat} SSE comment
     * every {@code heartbeat} so intermediaries keep the connection open.
     *
     * <p>On emitter completion / error / timeout the follow subscription is
     * closed via {@code onClose}, mirroring the cancellation pattern used by
     * {@link #streamSseScheduled}.
     *
     * @param heartbeat       heartbeat cadence (SSE comments while idle)
     * @param onClose         called exactly once when the emitter completes,
     *                        errors, or times out — typically
     *                        {@code FollowSubscription::close}
     * @param streamer        invoked with sink + hooks on a virtual thread
     */
    public <T> SseEmitter streamSseFollow(
            Duration heartbeat,
            Runnable onClose,
            FollowRecordStreamer<T> streamer) {
        SseEmitter emitter = new SseEmitter(0L);
        java.util.concurrent.atomic.AtomicBoolean cleanedUp =
                new java.util.concurrent.atomic.AtomicBoolean();
        Runnable cleanup = () -> {
            if (cleanedUp.compareAndSet(false, true)) {
                try { onClose.run(); } catch (Exception ignored) {}
            }
        };
        emitter.onCompletion(cleanup);
        emitter.onError(t -> cleanup.run());
        emitter.onTimeout(cleanup);

        Thread.ofVirtual().start(() -> {
            try {
                FollowHooks<T> hooks = new FollowHooks<>() {
                    @Override public Duration heartbeatInterval() { return heartbeat; }
                    @Override public void onHistoricalEnd() {
                        try {
                            emitter.send(SseEmitter.event()
                                    .name("follow")
                                    .data("{}")
                                    .build());
                        } catch (IOException e) {
                            throw new java.io.UncheckedIOException(e);
                        }
                    }
                    @Override public void onHeartbeat() {
                        try {
                            emitter.send(SseEmitter.event().comment("heartbeat").build());
                        } catch (IOException e) {
                            throw new java.io.UncheckedIOException(e);
                        }
                    }
                    @Override public void onOverflow() {
                        try {
                            emitter.send(SseEmitter.event()
                                    .name("overflow")
                                    .data("{\"reason\":\"buffer overflow\"}")
                                    .build());
                        } catch (IOException e) {
                            throw new java.io.UncheckedIOException(e);
                        }
                    }
                };
                streamer.stream(record -> {
                    try {
                        emitter.send(SseEmitter.event()
                                .data(objectMapper.writeValueAsString(record))
                                .build());
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                }, hooks);
                emitter.complete();
            } catch (java.io.UncheckedIOException e) {
                emitter.completeWithError(e.getCause());
            } catch (Exception e) {
                emitter.completeWithError(e);
            } finally {
                cleanup.run();
            }
        });
        return emitter;
    }

    /**
     * NDJSON streaming variant for {@code follow=true} replays.
     *
     * <p>First streams history as NDJSON lines, then (via hooks) writes a
     * {@code {"event":"follow"}} preamble line and enters the live tail loop.
     * Idle periods emit a {@code {"event":"heartbeat","ts":"..."}} line every
     * {@code heartbeat}.  On overflow a terminal
     * {@code {"event":"overflow",...}} line is written and the stream ends.
     */
    public <T> StreamingResponseBody streamNdjsonFollow(
            Duration heartbeat,
            Runnable onClose,
            FollowRecordStreamer<T> streamer) {
        return outputStream -> {
            var writer = new BufferedWriter(
                    new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
            java.util.concurrent.atomic.AtomicBoolean cleanedUp =
                    new java.util.concurrent.atomic.AtomicBoolean();
            Runnable cleanup = () -> {
                if (cleanedUp.compareAndSet(false, true)) {
                    try { onClose.run(); } catch (Exception ignored) {}
                }
            };
            try {
                FollowHooks<T> hooks = new FollowHooks<>() {
                    @Override public Duration heartbeatInterval() { return heartbeat; }
                    @Override public void onHistoricalEnd() {
                        writeLine(writer, "{\"event\":\"follow\"}");
                    }
                    @Override public void onHeartbeat() {
                        writeLine(writer, "{\"event\":\"heartbeat\",\"ts\":\""
                                + Instant.now() + "\"}");
                    }
                    @Override public void onOverflow() {
                        writeLine(writer, "{\"event\":\"overflow\",\"reason\":\"buffer overflow\"}");
                    }
                };
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
                }, hooks);
            } catch (java.io.UncheckedIOException e) {
                throw e.getCause();
            } catch (SQLException e) {
                throw new IOException(e);
            } finally {
                cleanup.run();
            }
        };
    }

    private static void writeLine(BufferedWriter writer, String line) {
        try {
            writer.write(line);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /**
     * Streamer signature for follow-mode replays: the replay service is invoked
     * with both a record sink and a {@link FollowHooks} callback surface so it
     * can signal historical-end, heartbeat, and overflow transitions.
     */
    @FunctionalInterface
    public interface FollowRecordStreamer<T> {
        void stream(Consumer<T> sink, FollowHooks<T> hooks) throws SQLException;
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
