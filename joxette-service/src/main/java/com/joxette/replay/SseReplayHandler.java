package com.joxette.replay;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.api.error.ErrorCodes;
import com.joxette.api.error.ErrorTypes;
import com.joxette.api.error.JoxetteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import org.springframework.context.SmartLifecycle;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Handles SSE ({@code text/event-stream}) and NDJSON
 * ({@code application/x-ndjson}) streaming for cassette replay endpoints.
 *
 * <p>Both formats share the same {@link RecordStreamer} abstraction: callers
 * provide a lambda that feeds records to a {@link Consumer} sink; this class
 * wraps the sink with format-specific serialisation.
 *
 * <h2>Error semantics</h2>
 *
 * <p>Errors raised <em>before</em> streaming begins (invalid query params,
 * malformed cursors, unknown resources) are thrown by the controller and
 * rendered as {@code application/problem+json} (RFC 7807) by
 * {@link com.joxette.api.error.GlobalExceptionHandler}. Streaming-format
 * handlers here are only invoked after pre-stream validation has succeeded.
 *
 * <p>Errors that surface <em>mid-stream</em> (e.g. DuckLake query failure
 * after records have already been written) cannot change the HTTP status —
 * the response has been committed. To give clients a structured, machine-
 * readable signal, this handler emits a terminal frame that mirrors the
 * ProblemDetail fields emitted by the global exception handler:
 *
 * <dl>
 *   <dt>SSE</dt>
 *   <dd>A final event of the form
 *       {@code event: error\ndata: {"type":"…","title":"…","status":500,"detail":"…","errorCode":"…"}\n\n}
 *       followed by {@code emitter.complete()}.</dd>
 *   <dt>NDJSON</dt>
 *   <dd>A final line of the form
 *       {@code {"_error":{"type":"…","title":"…","status":500,"detail":"…","errorCode":"…"}}}
 *       followed by a normal end-of-stream.</dd>
 * </dl>
 *
 * <p>Clients distinguish a normal end-of-stream from an error termination by
 * the presence of the {@code error} SSE event or the {@code _error} NDJSON
 * key. The underlying exception is always logged at {@code ERROR} level with
 * the full stack trace.
 */
@Component
public class SseReplayHandler implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(SseReplayHandler.class);

    // Tracks all VTs currently running a streaming replay so we can interrupt them on shutdown.
    private final Set<Thread> activeThreads = ConcurrentHashMap.newKeySet();
    private volatile boolean running = true;

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
        Thread vt = Thread.ofVirtual().unstarted(
                () -> runTracked(emitter, () -> runStreamSse(emitter, preambleEventName, preambleData, streamer)));
        vt.start();
        return emitter;
    }

    /**
     * Shared body for {@link #streamSse}. Exposed package-private so tests can run the
     * streaming loop synchronously against a captured emitter without having to wait
     * for a virtual thread to finish.
     */
    <T> void runStreamSse(SseEmitter emitter, String preambleEventName, String preambleData,
                          RecordStreamer<T> streamer) {
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
            if (isClientDisconnect(e.getCause())) {
                log.debug("SSE client disconnected mid-stream: {}", e.getCause().getMessage());
                try { emitter.complete(); } catch (Exception ignored) {}
            } else {
                sendSseError(emitter, e.getCause());
            }
        } catch (IllegalStateException e) {
            // Emitter already completed (e.g. broken-pipe callback raced us) — nothing to do.
            log.debug("SSE emitter already completed: {}", e.getMessage());
        } catch (Exception e) {
            sendSseError(emitter, e);
        }
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
                writeNdjsonError(writer, e.getCause());
            } catch (SQLException | RuntimeException e) {
                writeNdjsonError(writer, e);
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

        Thread vt = Thread.ofVirtual().unstarted(() -> runTracked(emitter, () -> {
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
                if (isClientDisconnect(e.getCause())) {
                    log.debug("SSE follow client disconnected mid-stream: {}", e.getCause().getMessage());
                    try { emitter.complete(); } catch (Exception ignored) {}
                } else {
                    sendSseError(emitter, e.getCause());
                }
            } catch (IllegalStateException e) {
                log.debug("SSE follow emitter already completed: {}", e.getMessage());
            } catch (Exception e) {
                sendSseError(emitter, e);
            } finally {
                cleanup.run();
            }
        }));
        vt.start();
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
                writeNdjsonError(writer, e.getCause());
            } catch (SQLException | RuntimeException e) {
                writeNdjsonError(writer, e);
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
        Thread vt = Thread.ofVirtual().unstarted(() -> runTracked(emitter, () -> {
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
                sendSseError(emitter, e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                schedService.markFailed(id);
                sendSseError(emitter, e);
            } catch (Exception e) {
                schedService.markFailed(id);
                sendSseError(emitter, e);
            }
        }));
        vt.start();
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
                writeNdjsonError(writer, e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                schedService.markFailed(id);
                writeNdjsonError(writer, e);
            } catch (SQLException | RuntimeException e) {
                schedService.markFailed(id);
                writeNdjsonError(writer, e);
            }
        };
    }

    // =========================================================================
    // Shutdown lifecycle — interrupts in-flight VTs before Tomcat closes
    // =========================================================================

    /**
     * Registers the current thread, runs {@code body}, then deregisters.
     * If the thread is interrupted while running (e.g. during shutdown) the
     * emitter is completed so the HTTP response is closed before Tomcat times out.
     */
    private void runTracked(SseEmitter emitter, Runnable body) {
        activeThreads.add(Thread.currentThread());
        try {
            body.run();
        } finally {
            activeThreads.remove(Thread.currentThread());
            // Ensure the emitter is closed so Tomcat sees the request finish.
            if (Thread.currentThread().isInterrupted()) {
                try { emitter.complete(); } catch (Exception ignored) {}
            }
        }
    }

    /** Package-private for tests: number of VTs currently tracked in-flight. */
    int activeThreadCount() {
        return activeThreads.size();
    }

    /**
     * Phase must be higher than Tomcat's graceful-shutdown phase so this runs first
     * and all replay VTs have released their HTTP connections before Tomcat starts its
     * 30 s drain clock.  Spring registers {@code webServerGracefulShutdown} at
     * {@code MAX_VALUE - 1024}; we therefore use {@code MAX_VALUE - 512}.
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 512;
    }

    @Override
    public boolean isAutoStartup() { return true; }

    @Override
    public void start() { running = true; }

    @Override
    public boolean isRunning() { return running; }

    /**
     * Interrupts all active replay VTs and marks this bean stopped so Spring
     * does not call {@link #stop()} a second time.
     */
    @Override
    public void stop() {
        running = false;
        int count = activeThreads.size();
        if (count > 0) {
            log.info("SseReplayHandler: interrupting {} in-flight replay VT(s) for shutdown", count);
            for (Thread t : activeThreads) {
                t.interrupt();
            }
        }
    }

    // =========================================================================
    // Mid-stream error helpers — see class-level Javadoc for the contract.
    // =========================================================================

    /**
     * Maps any throwable to the RFC 7807 ProblemDetail fields shared with
     * {@link com.joxette.api.error.GlobalExceptionHandler}. {@link JoxetteException}
     * instances surface their typed status/type/title/errorCode; everything else
     * is reported as a 500 Internal Server Error with a generic message so we
     * don't leak stack traces or driver internals to clients.
     */
    static Map<String, Object> problemPayload(Throwable t) {
        Map<String, Object> payload = new LinkedHashMap<>();
        if (t instanceof JoxetteException je) {
            payload.put("type", je.type().toString());
            payload.put("title", je.title());
            payload.put("status", je.status().value());
            payload.put("detail", je.detail());
            payload.put("errorCode", je.errorCode());
        } else {
            payload.put("type", ErrorTypes.INTERNAL.toString());
            payload.put("title", "Internal Server Error");
            payload.put("status", HttpStatus.INTERNAL_SERVER_ERROR.value());
            payload.put("detail", "Stream terminated abnormally. See server logs for details.");
            payload.put("errorCode", ErrorCodes.INTERNAL);
        }
        payload.put("timestamp", Instant.now().toString());
        return payload;
    }

    /**
     * Returns true when the exception represents a client-side disconnect:
     * broken pipe, reset by peer, or Spring's AsyncRequestNotUsableException.
     * These are not server errors and should not be logged at ERROR.
     */
    private static boolean isClientDisconnect(Throwable t) {
        if (t == null) return false;
        String name = t.getClass().getName();
        if (name.contains("AsyncRequestNotUsableException")) return true;
        String msg = t.getMessage();
        if (msg == null) return false;
        String lower = msg.toLowerCase();
        return lower.contains("broken pipe")
                || lower.contains("connection reset")
                || lower.contains("closed")
                || lower.contains("reset by peer");
    }

    /** Logs {@code cause} at ERROR, appending SQL error-code and SQLState when available. */
    private void logStreamFailure(String message, Throwable cause) {
        if (cause instanceof SQLException sqle) {
            log.error("{} [sqlErrorCode={}, sqlState={}]", message, sqle.getErrorCode(), sqle.getSQLState(), sqle);
        } else {
            log.error(message, cause);
        }
    }

    /**
     * Emits a terminal {@code event: error} SSE frame carrying a ProblemDetail-shaped
     * payload, then completes the emitter normally. The underlying cause is logged
     * at ERROR. Client-disconnect causes are demoted to DEBUG. Swallows any
     * {@link IOException} or {@link IllegalStateException} from the emitter
     * (the client may have already disconnected).
     */
    private void sendSseError(SseEmitter emitter, Throwable cause) {
        if (isClientDisconnect(cause)) {
            log.debug("SSE client disconnected, stream terminated: {}", cause.getMessage());
        } else {
            logStreamFailure("Mid-stream SSE replay failure", cause);
            try {
                String payload = objectMapper.writeValueAsString(problemPayload(cause));
                emitter.send(SseEmitter.event().name("error").data(payload).build());
            } catch (Exception ignored) {
                // Client may have disconnected; nothing we can do.
            }
        }
        try { emitter.complete(); } catch (Exception ignored) {}
    }

    /**
     * Writes a terminal {@code {"_error":{…}}} NDJSON line carrying a
     * ProblemDetail-shaped payload, flushes, and returns. Logs the underlying
     * cause at ERROR. Swallows any write failure (the client may have
     * disconnected mid-stream).
     */
    private void writeNdjsonError(BufferedWriter writer, Throwable cause) {
        logStreamFailure("Mid-stream NDJSON replay failure", cause);
        try {
            Map<String, Object> wrapper = new LinkedHashMap<>();
            wrapper.put("_error", problemPayload(cause));
            writer.write(objectMapper.writeValueAsString(wrapper));
            writer.newLine();
            writer.flush();
        } catch (Exception ignored) {
            // Client may have disconnected; nothing we can do.
        }
    }
}
