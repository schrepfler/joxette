# SseReplayHandler NDJSON → jox-json Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate `SseReplayHandler`'s three NDJSON methods (`streamNdjson`, `streamNdjsonFollow`, `streamNdjsonScheduled`) from hand-rolled `BufferedWriter` writes to the new `jox-json` module, per the approved design spec.

**Architecture:** Introduce a package-private `NdjsonLine` sealed union type (`RecordLine<T>` / `ControlEvent` / `ErrorFrame`) with one transparent-union serializer mirroring the existing `GuardedStep`/`GuardedStepSerializer` pattern. Each NDJSON method wraps its existing control flow in one `Flows.usingEmit(...)` body run inside `Scopes.supervised(...)` (mirroring `FlowReplayEngine`'s existing pattern exactly), converts every "write bytes" call into an "emit a value" call, translates mid-stream errors into an emitted `ErrorFrame` value instead of a thrown exception, and renders the resulting `Flow<NdjsonLine>` via `JsonFlow.renderNdjson(...)` followed by a manual `runForeach` write with per-chunk flush.

**Tech Stack:** Java 25, Jox 0.6.0 (`flows`, `structured`, new `json` module), Jackson 3 (`tools.jackson.*`), Spring Boot 4.1.1, JUnit 5, AssertJ.

## Global Constraints

- Wire format must not change: every NDJSON line this migration produces must be byte-identical to what the current hand-rolled code produces today. Existing tests that assert on exact response bodies (`SseReplayHandlerErrorTest`, `TimestampSerializationIT`, `CassetteControllerLastNExclusivityTest`, `CassetteControllerTransformValidationTest`) are the ground truth — if a step's output doesn't match them, the step is wrong, not the test.
- Out of scope (per the approved spec, `docs/superpowers/specs/2026-09-15-ndjson-jox-json-migration-design.md`): `SseReplayHandler`'s SSE methods (`streamSse`, `streamSseFollow`, `streamSseScheduled`) and `ExportService.exportNdjson` — neither is touched by this plan.
- `SmartLifecycle`/shutdown mechanics (`activeThreads` tracking, `runTracked`, `stop()`/`getPhase()`) are not touched — they wrap the virtual thread `streamSse*` methods spawn, which this plan doesn't change. `streamNdjson*` methods have never spawned their own thread (Spring's `StreamingResponseBody` is invoked directly by Spring MVC's own request-handling thread), and that stays true after this migration.
- Every task's module tests must be green (`mvn -pl joxette-service -am test`) before moving to the next task. Each task lands as its own commit.

## Task 1: Add the `jox-json` dependency

**Files:**
- Modify: `joxette-service/pom.xml`

**Interfaces:** None — this task only adds a dependency and verifies it resolves.

- [ ] **Step 1: Add the dependency**

In `joxette-service/pom.xml`, find the existing Jox dependency block (`channels`/`flows`/`structured`/`kafka`, all using `${jox.flows.version}`) and add:
```xml
        <!-- Jox JSON: lazy, backpressured NDJSON rendering over Flow/ByteFlow -->
        <dependency>
            <groupId>com.softwaremill.jox</groupId>
            <artifactId>json</artifactId>
            <version>${jox.flows.version}</version>
        </dependency>
```
(`jox.flows.version` is already `0.6.0` in the root `pom.xml`, and `com.softwaremill.jox:json:0.6.0` is released in lockstep with `flows`/`structured`/`kafka` — confirmed via `mcs search com.softwaremill.jox:json`.)

- [ ] **Step 2: Verify dependency resolution and check for version mediation surprises**

Run:
```bash
mvn -pl joxette-service -am dependency:resolve
```
Expected: `BUILD SUCCESS`, no errors.

Then check which `jackson-databind` version actually lands on the classpath — `jox-json`'s own `pom.xml` declares `tools.jackson.core:jackson-databind:3.2.2`, one minor version ahead of the `3.1.5` this project gets from Spring Boot 4.1.1's BOM:
```bash
mvn -pl joxette-service dependency:tree -DoutputFile=/tmp/joxjson-deptree.txt
grep "tools.jackson.core:jackson-databind" /tmp/joxjson-deptree.txt
```
Expected: exactly one version wins (Maven's dependency-management-wins rule should resolve to `3.1.5`, since that's pinned via this project's own `spring-boot-dependencies` BOM import, which takes precedence over a transitive dependency's own unmanaged version). Note whichever version wins — Task 3's test run is the real verification that this version combination works correctly at runtime.

- [ ] **Step 3: Compile to confirm no immediate classpath conflicts**

Run: `mvn -pl joxette-service -am compile`
Expected: `BUILD SUCCESS` (no source changes yet, so this just confirms the dependency addition alone doesn't break anything).

- [ ] **Step 4: Commit**

```bash
git add joxette-service/pom.xml
git commit -m "$(cat <<'EOF'
Add jox-json dependency for NDJSON streaming migration

com.softwaremill.jox:json:0.6.0 (same release train as the flows/
structured/kafka modules already in use). No code changes yet.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: `NdjsonLine` union type

**Files:**
- Create: `joxette-service/src/main/java/com/joxette/replay/NdjsonLine.java`
- Test: `joxette-service/src/test/java/com/joxette/replay/NdjsonLineTest.java`

**Interfaces:**
- Produces: `NdjsonLine` (package-private sealed interface, `com.joxette.replay`), with three variants: `NdjsonLine.RecordLine<T>(T value)`, `NdjsonLine.ControlEvent(Map<String,Object> payload)`, `NdjsonLine.ErrorFrame(Map<String,Object> payload)`. All three serialize (via any `ObjectMapper`/`ObjectWriter`, no module registration required) as their unwrapped payload — `NdjsonLine` itself never appears in the JSON. Task 3/4/5 construct these directly and render a `Flow<NdjsonLine>` via `JsonFlow.renderNdjson(flow, objectMapper.writerFor(NdjsonLine.class))`.

- [ ] **Step 1: Write the failing test**

Create `joxette-service/src/test/java/com/joxette/replay/NdjsonLineTest.java`:
```java
package com.joxette.replay;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NdjsonLineTest {

    private final ObjectMapper mapper = new ObjectMapper();

    record Sample(String name, int count) {}

    @Test
    void recordLineSerializesAsBareValue() {
        NdjsonLine line = new NdjsonLine.RecordLine<>(new Sample("orders", 3));
        String json = mapper.writerFor(NdjsonLine.class).writeValueAsString(line);
        assertThat(json).isEqualTo("{\"name\":\"orders\",\"count\":3}");
    }

    @Test
    void controlEventSerializesAsBareMap() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("event", "heartbeat");
        payload.put("ts", "2026-09-15T10:00:00Z");
        NdjsonLine line = new NdjsonLine.ControlEvent(payload);
        String json = mapper.writerFor(NdjsonLine.class).writeValueAsString(line);
        assertThat(json).isEqualTo("{\"event\":\"heartbeat\",\"ts\":\"2026-09-15T10:00:00Z\"}");
    }

    @Test
    void errorFrameSerializesAsBareMap() {
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("status", 500);
        inner.put("title", "Internal Server Error");
        NdjsonLine line = new NdjsonLine.ErrorFrame(Map.of("_error", inner));
        String json = mapper.writerFor(NdjsonLine.class).writeValueAsString(line);
        assertThat(json).isEqualTo("{\"_error\":{\"status\":500,\"title\":\"Internal Server Error\"}}");
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -pl joxette-service -am test -Dtest=NdjsonLineTest`
Expected: `FAIL` — compile error, `NdjsonLine` does not exist.

- [ ] **Step 3: Create `NdjsonLine.java`**

```java
package com.joxette.replay;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.annotation.JsonSerialize;
import tools.jackson.databind.ser.std.StdSerializer;

import java.util.Map;

/**
 * Internal wire-line union for {@link SseReplayHandler}'s NDJSON streaming methods.
 *
 * <p>Wraps a replay record, a control event (preamble/follow/heartbeat/overflow/
 * scheduled/cancelled), or a mid-stream error frame, so all three can flow through
 * one jox-json {@code Flow<NdjsonLine>} while still rendering as flat, unwrapped
 * JSON lines identical to the hand-written NDJSON output this replaces.
 * {@code NdjsonLine} itself is never visible in the JSON — {@link Serializer}
 * unwraps to whichever variant's payload is present.
 */
@JsonSerialize(using = NdjsonLine.Serializer.class)
sealed interface NdjsonLine {

    /** A normal replay record of type {@code T}. */
    record RecordLine<T>(T value) implements NdjsonLine {}

    /** A non-record control line: preamble, follow, heartbeat, overflow, scheduled, cancelled. */
    record ControlEvent(Map<String, Object> payload) implements NdjsonLine {}

    /** The terminal error frame — {@code payload} is already the {@code {"_error": {...}}} wrapper map. */
    record ErrorFrame(Map<String, Object> payload) implements NdjsonLine {}

    final class Serializer extends StdSerializer<NdjsonLine> {

        Serializer() {
            super(NdjsonLine.class);
        }

        @Override
        public void serialize(NdjsonLine line, JsonGenerator gen, SerializationContext provider) {
            Object payload = switch (line) {
                case RecordLine<?> r -> r.value();
                case ControlEvent c -> c.payload();
                case ErrorFrame e -> e.payload();
            };
            provider.writeTree(gen, provider.valueToTree(payload));
        }
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -pl joxette-service -am test -Dtest=NdjsonLineTest`
Expected: `Tests run: 3, Failures: 0, Errors: 0`

- [ ] **Step 5: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/replay/NdjsonLine.java \
        joxette-service/src/test/java/com/joxette/replay/NdjsonLineTest.java
git commit -m "$(cat <<'EOF'
Add NdjsonLine transparent-union type for jox-json rendering

Sealed RecordLine<T>/ControlEvent/ErrorFrame union with one serializer
that unwraps to the bare payload -- mirrors the GuardedStep/
GuardedStepSerializer pattern already used elsewhere in this package.
Works with any ObjectMapper/ObjectWriter, no module registration
required (annotation-driven, same as GuardedStep).

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Migrate `streamNdjson`

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/replay/SseReplayHandler.java`

**Interfaces:**
- Consumes: `NdjsonLine` (Task 2).
- Produces: a new private helper `renderAndWrite(OutputStream outputStream, Flow<NdjsonLine> flow) throws Exception` that Task 4 and Task 5 also call.
- `streamNdjson(RecordStreamer<T>)` and `streamNdjson(String preambleLine, RecordStreamer<T>)` keep their exact existing public signatures — no caller in `CassetteController.java` needs to change.

- [ ] **Step 1: Confirm the current behavior with existing tests (baseline)**

Run: `mvn -pl joxette-service -am test -Dtest=SseReplayHandlerErrorTest`
Expected: `BUILD SUCCESS`, all tests green (this is the pre-migration baseline; these tests must still pass identically after Step 3 below).

- [ ] **Step 2: Add the new imports and the shared `renderAndWrite` helper**

In `joxette-service/src/main/java/com/joxette/replay/SseReplayHandler.java`, add these imports (alongside the existing ones):
```java
import com.softwaremill.jox.flows.Flow;
import com.softwaremill.jox.flows.Flows;
import com.softwaremill.jox.json.JsonFlow;
import com.softwaremill.jox.structured.Scopes;
```

Add this private helper method (place it near `writeLine`, after the public methods):
```java
    /**
     * Renders {@code flow} as NDJSON and writes each resulting chunk directly to
     * {@code outputStream}, flushing after every chunk. Deliberately not
     * {@code ByteFlow.runToOutputStream} — that closes the stream on any exception
     * (which would fire before an error already emitted into the flow gets written)
     * and never flushes between chunks (which would delay live/follow-mode output).
     */
    private void renderAndWrite(java.io.OutputStream outputStream, Flow<NdjsonLine> flow) throws Exception {
        JsonFlow.renderNdjson(flow, objectMapper.writerFor(NdjsonLine.class))
                .runForeach(chunk -> {
                    outputStream.write(chunk.toArray());
                    outputStream.flush();
                });
    }
```

- [ ] **Step 3: Replace `streamNdjson(String preambleLine, RecordStreamer<T> streamer)`**

Replace the existing method body:
```java
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
                    } catch (JacksonException e) {
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
```
with:
```java
    public <T> StreamingResponseBody streamNdjson(String preambleLine, RecordStreamer<T> streamer) {
        return outputStream -> {
            // preambleLine arrives pre-serialised (built by the caller via
            // objectMapper.writeValueAsString already) -- write it directly rather
            // than round-tripping it back through Jackson.
            if (preambleLine != null) {
                outputStream.write(preambleLine.getBytes(StandardCharsets.UTF_8));
                outputStream.write('\n');
                outputStream.flush();
            }
            try {
                Scopes.<Void>supervised(scope -> {
                    Flow<NdjsonLine> flow = Flows.usingEmit(emit -> {
                        try {
                            streamer.stream(record -> {
                                try {
                                    emit.apply(new NdjsonLine.RecordLine<>(record));
                                } catch (Exception e) {
                                    if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                                    throw new RuntimeException(e);
                                }
                            });
                        } catch (SQLException | RuntimeException e) {
                            logStreamFailure("Mid-stream NDJSON replay failure", e);
                            emit.apply(new NdjsonLine.ErrorFrame(Map.of("_error", problemPayload(e))));
                        }
                    });
                    renderAndWrite(outputStream, flow);
                    return null;
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                // The error frame itself failed to reach the client (e.g. broken pipe
                // surfaced only once we tried to write it) or the Flow machinery hit
                // an unexpected failure. Already logged via logStreamFailure in the
                // common case above; nothing more we can do to a dead connection.
                log.debug("NDJSON stream terminated abnormally: {}", e.getMessage());
            }
        };
    }
```

- [ ] **Step 4: Run `SseReplayHandlerErrorTest` to verify identical behavior**

Run: `mvn -pl joxette-service -am test -Dtest=SseReplayHandlerErrorTest`
Expected: `BUILD SUCCESS` — the NDJSON test cases (`ndjsonEmitsTerminalErrorLine`, `ndjsonInternalErrorDoesNotLeakCauseMessage`, `ndjsonCompletesCleanlyWithoutError`) must still pass with byte-identical assertions (they call `handler.<String>streamNdjson(sink -> {...})` synchronously via `body.writeTo(bos)` and assert on the exact output — no test code changes needed here since the public method signature and behavior are unchanged).

If it fails, the most likely cause is a Java version-pattern-matching or generics issue in `NdjsonLine.Serializer`'s `switch` — re-check `RecordLine<?>` matches a `RecordLine<String>` instance correctly (it should: `RecordLine<?>` in a `switch` pattern matches any parameterization of `RecordLine`).

- [ ] **Step 5: Run the full NDJSON-adjacent test suite**

Run: `mvn -pl joxette-service -am test -Dtest=SseReplayHandlerErrorTest,CassetteControllerLastNExclusivityTest,CassetteControllerTransformValidationTest`
Expected: `BUILD SUCCESS`, all green. These exercise `streamNdjson` through `CassetteController`'s real endpoints (including the preamble-line path via a non-empty transform pipeline).

- [ ] **Step 6: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/replay/SseReplayHandler.java
git commit -m "$(cat <<'EOF'
Migrate streamNdjson to jox-json

Replaces the hand-rolled BufferedWriter + objectMapper.writeValueAsString
loop with Flows.usingEmit + Scopes.supervised (same pattern
FlowReplayEngine already uses) feeding JsonFlow.renderNdjson, terminated
with a manual runForeach write (not runToOutputStream, which doesn't
flush per chunk and closes the stream on any exception). Mid-stream
errors become an emitted NdjsonLine.ErrorFrame value instead of a thrown
exception, so the existing terminal {"_error":{...}} line contract is
preserved without needing runToOutputStream's close-on-error semantics.
Wire format unchanged -- SseReplayHandlerErrorTest passes without
modification.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Migrate `streamNdjsonFollow`

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/replay/SseReplayHandler.java`
- Test: `joxette-service/src/test/java/com/joxette/replay/SseReplayHandlerNdjsonFollowTest.java`

**Interfaces:**
- Consumes: `NdjsonLine`, `renderAndWrite` (Task 2, 3), `FollowHooks<T>` (existing interface in this package: `onHistoricalEnd()`, `onHeartbeat()`, `onOverflow()`, `heartbeatInterval()`), `FollowRecordStreamer<T>` (existing nested interface in `SseReplayHandler`: `void stream(Consumer<T> sink, FollowHooks<T> hooks) throws SQLException`).
- No test exists today for this method's NDJSON-specific output (`FollowModeIntegrationTest` only covers SSE) — Step 1 below adds the first one.

- [ ] **Step 1: Write the failing test**

Create `joxette-service/src/test/java/com/joxette/replay/SseReplayHandlerNdjsonFollowTest.java`:
```java
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -pl joxette-service -am test -Dtest=SseReplayHandlerNdjsonFollowTest`
Expected: `FAIL` — output still matches the OLD hand-written format for these specific assertions only if the current implementation already produces this shape (it does — this test asserts today's existing wire format, so before Step 3 it should actually already pass against the *current* implementation). Confirm this explicitly:
```bash
git stash
mvn -pl joxette-service -am test -Dtest=SseReplayHandlerNdjsonFollowTest
git stash pop
```
Expected: passes against the pre-migration code too — this test is a **characterization test**: it pins today's behavior before the rewrite, so any regression during Task 4's rewrite shows up as a failure. (Unlike Tasks 2/3, there's no "doesn't compile yet" red state here, since nothing about this test depends on `NdjsonLine` being wired into `streamNdjsonFollow` — it calls the existing public method. Proceed to Step 3 regardless.)

- [ ] **Step 3: Replace `streamNdjsonFollow`**

Replace the existing method body:
```java
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
                    } catch (JacksonException e) {
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
```
with:
```java
    public <T> StreamingResponseBody streamNdjsonFollow(
            Duration heartbeat,
            Runnable onClose,
            FollowRecordStreamer<T> streamer) {
        return outputStream -> {
            java.util.concurrent.atomic.AtomicBoolean cleanedUp =
                    new java.util.concurrent.atomic.AtomicBoolean();
            Runnable cleanup = () -> {
                if (cleanedUp.compareAndSet(false, true)) {
                    try { onClose.run(); } catch (Exception ignored) {}
                }
            };
            try {
                try {
                    Scopes.<Void>supervised(scope -> {
                        Flow<NdjsonLine> flow = Flows.usingEmit(emit -> {
                            FollowHooks<T> hooks = new FollowHooks<>() {
                                @Override public Duration heartbeatInterval() { return heartbeat; }
                                @Override public void onHistoricalEnd() {
                                    try {
                                        emit.apply(new NdjsonLine.ControlEvent(Map.of("event", "follow")));
                                    } catch (Exception e) { throw new RuntimeException(e); }
                                }
                                @Override public void onHeartbeat() {
                                    try {
                                        emit.apply(new NdjsonLine.ControlEvent(Map.of(
                                                "event", "heartbeat", "ts", Instant.now().toString())));
                                    } catch (Exception e) { throw new RuntimeException(e); }
                                }
                                @Override public void onOverflow() {
                                    try {
                                        emit.apply(new NdjsonLine.ControlEvent(Map.of(
                                                "event", "overflow", "reason", "buffer overflow")));
                                    } catch (Exception e) { throw new RuntimeException(e); }
                                }
                            };
                            try {
                                streamer.stream(record -> {
                                    try {
                                        emit.apply(new NdjsonLine.RecordLine<>(record));
                                    } catch (Exception e) {
                                        if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                                        throw new RuntimeException(e);
                                    }
                                }, hooks);
                            } catch (SQLException | RuntimeException e) {
                                logStreamFailure("Mid-stream NDJSON replay failure", e);
                                emit.apply(new NdjsonLine.ErrorFrame(Map.of("_error", problemPayload(e))));
                            }
                        });
                        renderAndWrite(outputStream, flow);
                        return null;
                    });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    log.debug("NDJSON follow stream terminated abnormally: {}", e.getMessage());
                }
            } finally {
                cleanup.run();
            }
        };
    }
```

- [ ] **Step 4: Run the new test to verify it passes**

Run: `mvn -pl joxette-service -am test -Dtest=SseReplayHandlerNdjsonFollowTest`
Expected: `Tests run: 2, Failures: 0, Errors: 0`

- [ ] **Step 5: Run the full existing suite touching this class**

Run: `mvn -pl joxette-service -am test -Dtest=SseReplayHandlerErrorTest,SseReplayHandlerNdjsonFollowTest,SseReplayHandlerLifecycleTest`
Expected: `BUILD SUCCESS`, all green — `SseReplayHandlerLifecycleTest` exercises the SSE virtual-thread lifecycle, untouched by this change, but confirms nothing in the shared class broke.

- [ ] **Step 6: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/replay/SseReplayHandler.java \
        joxette-service/src/test/java/com/joxette/replay/SseReplayHandlerNdjsonFollowTest.java
git commit -m "$(cat <<'EOF'
Migrate streamNdjsonFollow to jox-json

Same Flows.usingEmit + Scopes.supervised + renderAndWrite pattern as
streamNdjson. The heartbeat/overflow FollowHooks callbacks now emit
NdjsonLine.ControlEvent values instead of writing bytes directly -- the
idle-detection timing itself (EntityReplayService/TopicReplayService's
follow.awaitNext(heartbeat) poll loop) is untouched, so no Flow.merge or
Flows.tick composition is needed; the hooks and the record sink already
produce one correctly-ordered sequence from one thread.

Adds the first direct test coverage for this method's NDJSON output --
none existed before (FollowModeIntegrationTest only covers SSE).

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Migrate `streamNdjsonScheduled`

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/replay/SseReplayHandler.java`
- Test: `joxette-service/src/test/java/com/joxette/replay/SseReplayHandlerNdjsonScheduledTest.java`

**Interfaces:**
- Consumes: `NdjsonLine`, `renderAndWrite` (Task 2, 3), `ScheduledReplayService` (existing concrete class in this package — real instance used in the test, not mocked, since `awaitStart` with `delayMs <= 0` returns synchronously with no blocking).
- No test exists today for this method either.

- [ ] **Step 1: Write the failing test**

Create `joxette-service/src/test/java/com/joxette/replay/SseReplayHandlerNdjsonScheduledTest.java`:
```java
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -pl joxette-service -am test -Dtest=SseReplayHandlerNdjsonScheduledTest`
Expected: passes already against the current implementation (same characterization-test situation as Task 4 — this pins today's wire format). Confirm, then proceed:
```bash
git stash
mvn -pl joxette-service -am test -Dtest=SseReplayHandlerNdjsonScheduledTest
git stash pop
```

- [ ] **Step 3: Replace `streamNdjsonScheduled`**

Replace the existing method body:
```java
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
                    } catch (JacksonException e) {
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
```
with:
```java
    public <T> StreamingResponseBody streamNdjsonScheduled(
            String id,
            Instant scheduledAt,
            ScheduledReplayService schedService,
            RecordStreamer<T> streamer) {
        return outputStream -> {
            try {
                Scopes.<Void>supervised(scope -> {
                    Flow<NdjsonLine> flow = Flows.usingEmit(emit -> {
                        emit.apply(new NdjsonLine.ControlEvent(Map.of(
                                "event", "scheduled", "id", id, "scheduledAt", scheduledAt.toString())));

                        long delayMs = Math.max(0L, scheduledAt.toEpochMilli() - Instant.now().toEpochMilli());
                        boolean proceed;
                        try {
                            proceed = schedService.awaitStart(id, delayMs);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            schedService.markFailed(id);
                            emit.apply(new NdjsonLine.ErrorFrame(Map.of("_error", problemPayload(e))));
                            return;
                        }
                        if (!proceed) {
                            emit.apply(new NdjsonLine.ControlEvent(Map.of("event", "cancelled", "id", id)));
                            return;
                        }

                        schedService.markStreaming(id);
                        try {
                            streamer.stream(record -> {
                                try {
                                    emit.apply(new NdjsonLine.RecordLine<>(record));
                                } catch (Exception e) {
                                    if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                                    throw new RuntimeException(e);
                                }
                            });
                            schedService.markCompleted(id);
                        } catch (SQLException | RuntimeException e) {
                            schedService.markFailed(id);
                            logStreamFailure("Mid-stream NDJSON replay failure", e);
                            emit.apply(new NdjsonLine.ErrorFrame(Map.of("_error", problemPayload(e))));
                        }
                    });
                    renderAndWrite(outputStream, flow);
                    return null;
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.debug("NDJSON scheduled stream terminated abnormally: {}", e.getMessage());
            }
        };
    }
```

- [ ] **Step 4: Run the new test to verify it passes**

Run: `mvn -pl joxette-service -am test -Dtest=SseReplayHandlerNdjsonScheduledTest`
Expected: `Tests run: 3, Failures: 0, Errors: 0`

- [ ] **Step 5: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/replay/SseReplayHandler.java \
        joxette-service/src/test/java/com/joxette/replay/SseReplayHandlerNdjsonScheduledTest.java
git commit -m "$(cat <<'EOF'
Migrate streamNdjsonScheduled to jox-json

Same pattern as the other two NDJSON methods. The scheduled/cancelled
control lines and the blocking schedService.awaitStart(...) wait both
live inside the same Flows.usingEmit body as the record streaming --
Flows.usingEmit's body is ordinary blocking Java, so the existing
sequential control flow (emit scheduled -> wait -> emit cancelled-and-
return, or proceed to stream records) carries over with each "write"
call becoming an "emit" call.

Adds the first direct test coverage for this method -- none existed
before.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Remove dead code and final verification

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/replay/SseReplayHandler.java`

**Interfaces:** None — cleanup and verification only.

- [ ] **Step 1: Confirm `writeNdjsonError` is now unused**

Run:
```bash
grep -n "writeNdjsonError" joxette-service/src/main/java/com/joxette/replay/SseReplayHandler.java
```
Expected: only the method's own declaration remains — no call sites (all three were replaced in Tasks 3–5).

- [ ] **Step 2: Delete `writeNdjsonError`**

Remove the method entirely:
```java
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
```

- [ ] **Step 3: Remove now-unused imports**

Remove these three (no longer referenced anywhere in the file):
```java
import tools.jackson.core.JacksonException;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
```
Keep `import java.io.IOException;` — still used by the SSE methods (`streamSse`, `streamSseFollow`, `streamSseScheduled`, `sendSseError`).

- [ ] **Step 4: Compile and confirm no unused-import or unused-method warnings remain**

Run: `mvn -pl joxette-service -am compile`
Expected: `BUILD SUCCESS`.

- [ ] **Step 5: Run the full `joxette-service` module test suite**

Run: `mvn -pl joxette-service -am test`
Expected: `BUILD SUCCESS`, all green, including every test touched across Tasks 2–5
(`NdjsonLineTest`, `SseReplayHandlerErrorTest`, `SseReplayHandlerNdjsonFollowTest`,
`SseReplayHandlerNdjsonScheduledTest`, `SseReplayHandlerLifecycleTest`,
`CassetteControllerLastNExclusivityTest`, `CassetteControllerTransformValidationTest`)
plus everything else in the module (the pre-existing regression net).

- [ ] **Step 6: Run the integration test suite**

Run: `mvn -pl joxette-service -am verify`
Expected: `BUILD SUCCESS`. Pay particular attention to `TimestampSerializationIT`'s
NDJSON assertions (`topicReplay_ndjson_timestampFieldsAreIso8601WithTimezone`,
`entityReplay_ndjson_timestampFieldsAreIso8601WithTimezone`) — these hit the real
`streamNdjson` path end-to-end through a running Spring context and Testcontainers
Kafka, and are the strongest evidence the migration preserves the exact wire format
in production conditions, not just in the unit tests added above.

- [ ] **Step 7: Manual smoke check**

If a local Kafka + populated topic is available, start the service and compare:
```bash
curl -s -H "Accept: application/x-ndjson" "http://localhost:<port>/v1/cassettes/topics/<some-topic>?follow=true"
```
against the same request made before this branch's changes (e.g. checked out at
`main` before Task 1). The two outputs should be indistinguishable line-for-line
except for timestamps. If no populated topic is available for a quick manual check,
treat Step 6's `TimestampSerializationIT` run as sufficient evidence and note in the
final commit message that the manual check was skipped in favor of the equivalent
automated IT coverage.

- [ ] **Step 8: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/replay/SseReplayHandler.java
git commit -m "$(cat <<'EOF'
Remove dead NDJSON error-writing code after jox-json migration

writeNdjsonError and its BufferedWriter/OutputStreamWriter/
JacksonException imports are unused now that all three NDJSON methods
route errors through NdjsonLine.ErrorFrame instead. Full joxette-service
test suite and integration suite (including TimestampSerializationIT's
end-to-end NDJSON assertions) are green.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```
