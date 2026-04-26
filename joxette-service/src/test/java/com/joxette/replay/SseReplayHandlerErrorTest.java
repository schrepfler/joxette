package com.joxette.replay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.api.error.ErrorCodes;
import com.joxette.api.error.ErrorTypes;
import com.joxette.api.error.InvalidCursorException;
import com.joxette.api.error.ResourceNotFoundException;
import com.joxette.api.error.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the mid-stream error contract in {@link SseReplayHandler}.
 *
 * <p>Covers both transports (SSE, NDJSON) and the three flavours of failure the
 * handler must distinguish: typed {@link com.joxette.api.error.JoxetteException}s,
 * {@link SQLException}, and arbitrary {@link RuntimeException}. All non-typed
 * failures must surface as a 500 with a generic detail — verifying we don't leak
 * internals like SQL driver messages to clients.
 */
class SseReplayHandlerErrorTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final SseReplayHandler handler = new SseReplayHandler(mapper);

    // ---------------------------------------------------------------------
    // NDJSON — terminal `_error` line
    // ---------------------------------------------------------------------

    static Stream<Arguments> ndjsonErrorCases() {
        return Stream.of(
                Arguments.of("JoxetteException surfaces typed 400",
                        (Supplier<Exception>) () -> new ValidationException("boom"),
                        400, ErrorTypes.VALIDATION.toString(),
                        "Validation Failed", ErrorCodes.VALIDATION),
                Arguments.of("JoxetteException surfaces typed 404",
                        (Supplier<Exception>) () -> ResourceNotFoundException.topic("missing"),
                        404, ErrorTypes.NOT_FOUND.toString(),
                        "Not Found", ErrorCodes.NOT_FOUND),
                Arguments.of("SQLException maps to internal",
                        (Supplier<Exception>) () -> new SQLException("Table 'lake.main.general_x' missing"),
                        500, ErrorTypes.INTERNAL.toString(),
                        "Internal Server Error", ErrorCodes.INTERNAL),
                Arguments.of("RuntimeException maps to internal",
                        (Supplier<Exception>) () -> new RuntimeException("boom"),
                        500, ErrorTypes.INTERNAL.toString(),
                        "Internal Server Error", ErrorCodes.INTERNAL)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ndjsonErrorCases")
    void ndjsonEmitsTerminalErrorLine(
            String displayName,
            Supplier<Exception> failureFactory,
            int expectedStatus,
            String expectedType,
            String expectedTitle,
            String expectedErrorCode) throws Exception {

        StreamingResponseBody body = handler.<String>streamNdjson(sink -> {
            sink.accept("one");
            sink.accept("two");
            throw sneakyThrow(failureFactory.get());
        });

        String payload = render(body);
        List<String> lines = payload.lines().toList();

        assertThat(lines).hasSize(3);
        assertThat(lines.get(0)).isEqualTo("\"one\"");
        assertThat(lines.get(1)).isEqualTo("\"two\"");

        JsonNode terminal = mapper.readTree(lines.get(2));
        assertThat(terminal.has("_error"))
                .as("last line must be the terminal error wrapper")
                .isTrue();
        JsonNode problem = terminal.get("_error");
        assertThat(problem.get("status").asInt()).isEqualTo(expectedStatus);
        assertThat(problem.get("type").asText()).isEqualTo(expectedType);
        assertThat(problem.get("title").asText()).isEqualTo(expectedTitle);
        assertThat(problem.get("errorCode").asText()).isEqualTo(expectedErrorCode);
        assertThat(problem.get("timestamp").asText()).isNotEmpty();
    }

    @Test
    void ndjsonInternalErrorDoesNotLeakCauseMessage() throws Exception {
        StreamingResponseBody body = handler.<String>streamNdjson(sink -> {
            throw new SQLException("connection refused: super-secret-host:5432");
        });

        String payload = render(body);
        assertThat(payload).doesNotContain("super-secret-host");

        JsonNode problem = mapper.readTree(payload.trim()).get("_error");
        assertThat(problem.get("detail").asText())
                .contains("See server logs")
                .doesNotContain("connection refused");
    }

    @Test
    void ndjsonCompletesCleanlyWithoutError() throws Exception {
        StreamingResponseBody body = handler.<String>streamNdjson(sink -> {
            sink.accept("a");
            sink.accept("b");
        });

        String payload = render(body);
        assertThat(payload).isEqualTo("\"a\"\n\"b\"\n");
        assertThat(payload).doesNotContain("_error");
    }

    // ---------------------------------------------------------------------
    // SSE — terminal `event: error` frame
    // ---------------------------------------------------------------------

    static Stream<Arguments> sseErrorCases() {
        return Stream.of(
                Arguments.of("JoxetteException surfaces 400",
                        (Supplier<Exception>) () -> new InvalidCursorException("bad cursor"),
                        400, ErrorTypes.INVALID_CURSOR.toString(),
                        ErrorCodes.INVALID_CURSOR),
                Arguments.of("RuntimeException maps to 500",
                        (Supplier<Exception>) () -> new RuntimeException("boom"),
                        500, ErrorTypes.INTERNAL.toString(),
                        ErrorCodes.INTERNAL),
                Arguments.of("SQLException maps to 500",
                        (Supplier<Exception>) () -> new SQLException("driver failure"),
                        500, ErrorTypes.INTERNAL.toString(),
                        ErrorCodes.INTERNAL)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("sseErrorCases")
    void sseEmitsTerminalErrorEvent(
            String displayName,
            Supplier<Exception> failureFactory,
            int expectedStatus,
            String expectedType,
            String expectedErrorCode) throws Exception {

        CapturingSseEmitter emitter = new CapturingSseEmitter();
        handler.<String>runStreamSse(emitter, null, null, sink -> {
            sink.accept("one");
            throw sneakyThrow(failureFactory.get());
        });

        List<CapturingSseEmitter.Frame> frames = emitter.frames;
        assertThat(frames)
                .as("expected at least one data frame followed by the error frame")
                .hasSizeGreaterThanOrEqualTo(2);
        CapturingSseEmitter.Frame last = frames.get(frames.size() - 1);
        assertThat(last.eventName).isEqualTo("error");

        JsonNode problem = mapper.readTree(last.data);
        assertThat(problem.get("status").asInt()).isEqualTo(expectedStatus);
        assertThat(problem.get("type").asText()).isEqualTo(expectedType);
        assertThat(problem.get("errorCode").asText()).isEqualTo(expectedErrorCode);
        assertThat(problem.get("timestamp").asText()).isNotEmpty();

        assertThat(emitter.completed.get()).isTrue();
    }

    @Test
    void sseCompletesCleanlyWithoutError() {
        CapturingSseEmitter emitter = new CapturingSseEmitter();
        handler.<String>runStreamSse(emitter, null, null, sink -> {
            sink.accept("x");
        });

        assertThat(emitter.frames).hasSize(1);
        assertThat(emitter.frames.get(0).eventName).isNull();
        assertThat(emitter.frames.get(0).data).isEqualTo("\"x\"");
        assertThat(emitter.completed.get()).isTrue();
        assertThat(emitter.frames.stream().anyMatch(f -> "error".equals(f.eventName))).isFalse();
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static String render(StreamingResponseBody body) throws IOException {
        var bos = new ByteArrayOutputStream();
        body.writeTo(bos);
        return bos.toString(java.nio.charset.StandardCharsets.UTF_8);
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> RuntimeException sneakyThrow(Exception t) throws E {
        throw (E) t;
    }

    /**
     * {@link SseEmitter} stand-in that records every {@code send(SseEventBuilder)} call
     * instead of writing to a servlet response. Lets us assert the exact frames the
     * handler emits under error conditions.
     */
    static final class CapturingSseEmitter extends SseEmitter {
        final List<Frame> frames = new ArrayList<>();
        final AtomicBoolean completed = new AtomicBoolean(false);

        CapturingSseEmitter() { super(0L); }

        static final class Frame {
            final String eventName;
            final String data;
            Frame(String eventName, String data) { this.eventName = eventName; this.data = data; }
        }

        @Override
        public void send(Set<ResponseBodyEmitter.DataWithMediaType> parts) {
            // Production path: handler calls emitter.send(builder.build()), which
            // dispatches to ResponseBodyEmitter.send(Set<DataWithMediaType>).
            // SseEmitter builds a LinkedHashSet whose chunks concatenate to the
            // wire format:  [event:NAME\n][data:PAYLOAD\n][\n]. Parse by scanning
            // the joined string line-by-line rather than matching individual
            // chunks — the chunking is an implementation detail.
            StringBuilder joined = new StringBuilder();
            for (var p : parts) {
                joined.append(String.valueOf(p.getData()));
            }
            String name = null;
            StringBuilder data = new StringBuilder();
            for (String line : joined.toString().split("\n", -1)) {
                if (line.startsWith("event:")) {
                    name = line.substring("event:".length()).trim();
                } else if (line.startsWith("data:")) {
                    if (data.length() > 0) data.append('\n');
                    data.append(line.substring("data:".length()).trim());
                }
            }
            frames.add(new Frame(name, data.toString()));
        }

        @Override
        public void complete() {
            completed.set(true);
        }

        @Override
        public void completeWithError(Throwable t) {
            completed.set(true);
        }
    }
}
