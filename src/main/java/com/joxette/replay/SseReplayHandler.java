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
}
