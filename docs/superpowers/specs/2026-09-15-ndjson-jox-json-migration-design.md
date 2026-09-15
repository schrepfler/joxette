# SseReplayHandler NDJSON → jox-json Migration

## Motivation

Jox 0.6.0 (bumped in an earlier commit on this branch) shipped a new
`jox-json` module (`com.softwaremill.jox:json`): lazy, backpressured
NDJSON/JSON-array rendering over Jox `Flow`/`ByteFlow`, Jackson-backed.
This was the original motivating question behind the Jackson 2 → 3
migration (jox-json requires Jackson 3, which the codebase now has).

`SseReplayHandler`'s NDJSON methods (`streamNdjson`,
`streamNdjsonFollow`, `streamNdjsonScheduled`) currently hand-roll NDJSON
by calling `objectMapper.writeValueAsString(record)` per record into a
`BufferedWriter` wrapping the raw HTTP response `OutputStream`
(`StreamingResponseBody`). This design already has correct backpressure
and broken-pipe cancellation for free, because the DB read and the HTTP
write happen synchronously on the same virtual thread — but it
duplicates "serialize + newline + flush + exception mapping" across
three methods and doesn't reuse the Jox `Flow` machinery the recording
pipeline (`TopicRecorder`) and `FlowReplayEngine` already use elsewhere
in this codebase.

## Scope

**In scope:** `joxette-service/src/main/java/com/joxette/replay/SseReplayHandler.java`'s
three NDJSON-producing methods (`streamNdjson`, `streamNdjsonFollow`,
`streamNdjsonScheduled`), and the new small types/serializer they need.

**Out of scope:**
- `SseReplayHandler`'s SSE methods (`streamSse`, `streamSseFollow`,
  `streamSseScheduled`) — they write through `SseEmitter`, not an
  `OutputStream`, so `jox-json`'s `ByteFlow` rendering doesn't apply.
  Untouched.
- `ExportService.exportNdjson` — evaluated and explicitly rejected as a
  jox-json target (see "ExportService was considered and declined"
  below). Its own fix (stream records instead of materializing a
  `List`, and swap the JDBC `PreparedStatement` batch insert for
  DuckDB's `Appender` API) is unrelated to jox-json and Jackson, and is
  tracked as a separate follow-up design, not part of this spec.
- `SmartLifecycle`/shutdown mechanics (`activeThreads` tracking,
  interrupting virtual threads on shutdown) — kept structurally as-is;
  verified to still work correctly with the new `Scopes.supervised`
  wrapper during implementation, not redesigned here.

### ExportService was considered and declined

`ExportService.exportNdjson` doesn't write to an `OutputStream` at all:
it materializes all records into a `List<EntityRecord>`, batch-inserts
each `objectMapper.writeValueAsString(r)` line into a DuckDB temp table
via JDBC, then lets DuckDB's own `COPY ... TO ...` write the file
(local or `s3://`, transparently through the `httpfs` extension).
`jox-json` renders to an `OutputStream`/`ByteFlow`; there's nothing here
for it to attach to without redesigning how exports write files, which
is a different project.

A real benchmark (`jbang`, synthetic records shaped like `EntityRecord`,
DuckDB JDBC 1.5.5.1) found the actual bottleneck has nothing to do with
JSON serialization: at 500k rows, the JDBC `PreparedStatement` batch
`INSERT` step alone takes 21.4s (98.4% of total time), regardless of
chunk size (1k/10k/50k/one-shot all ≈8.4s at 200k rows). DuckDB's native
`Appender` API does the same 200k-row insert in 137ms — 61× faster. A
direct-streaming rewrite (the jox-json-shaped approach) is fastest of
all, but can't reach `s3://` export paths without violating this
project's "no direct S3 SDK usage from Java" rule (object storage stays
inside DuckDB/httpfs). The recommended fix — keep the DuckDB
temp-table-then-`COPY` design (so S3 export keeps working), but stream
records in instead of collecting a `List` first, and use the `Appender`
API instead of `PreparedStatement` batching — is a small, self-contained
DuckDB usage fix, independent of jox-json/Jackson entirely, and belongs
in its own design.

## Target Architecture

### Unified event model

A sealed `NdjsonLine` type with three variants:

- `Record<T>(T value)` — a normal replay record.
- `ControlEvent(Map<String,Object> payload)` — covers every non-record
  line current code writes: the transform preamble, `{"event":"follow"}`,
  `{"event":"heartbeat","ts":...}`, `{"event":"overflow",...}`,
  `{"event":"scheduled",...}`, `{"event":"cancelled",...}`.
- `ErrorFrame(Map<String,Object> payload)` — the mid-stream
  `{"_error":{...}}` frame.

One custom serializer unwraps whichever variant is present and writes
it as a flat JSON value with no wrapper envelope — the same
"transparent union" technique `GuardedStep`/`GuardedStepSerializer`
already use elsewhere in `joxette-service` (`@JsonTypeInfo(use=NONE)`
+ a hand-written `StdSerializer` that fully controls the output shape).
This keeps the wire format byte-identical to today's hand-written
lines; `NdjsonLine` is purely an internal Java-side grouping type, never
visible in the JSON.

### Per-method Flow construction

Each of the three methods keeps its exact current control flow,
re-expressed inside one `Flows.usingEmit(emit -> {...})` body, run
inside `Scopes.supervised(...)` — the same pattern `FlowReplayEngine`
already uses for `replayTopic`. Every place that currently calls
`writer.write(...)` instead calls `emit.apply(new NdjsonLine.X(...))`;
every place that currently reads from the DB (`streamer.stream(sink)`,
`FollowRecordStreamer.stream(sink, hooks)`) is unchanged — only the
sink/hooks implementations passed to it change from "write bytes" to
"emit a value":

- **`streamNdjson(preamble, streamer)`**: emit the preamble
  `ControlEvent` (if present), then `streamer.stream(record ->
  emit.apply(new Record<>(record)))`.
- **`streamNdjsonFollow(heartbeat, onClose, streamer)`**: build a
  `FollowHooks<T>` whose `onHistoricalEnd()`/`onHeartbeat()`/
  `onOverflow()` each `emit.apply(new ControlEvent(...))`, then call
  `streamer.stream(sink, hooks)`. The idle-poll loop that decides *when*
  a heartbeat fires lives inside `EntityReplayService`/
  `TopicReplayService` (`follow.awaitNext(heartbeat)`, a blocking
  poll-with-timeout) and is untouched — this is why no `Flow.merge` or
  `Flows.tick` composition is needed: the current design already
  produces one single, correctly-ordered sequence of record/heartbeat
  events from one thread, just split across two callback interfaces
  (`sink` and `hooks`) instead of one. `Flows.usingEmit`'s body is
  ordinary blocking Java, so both interfaces can emit into the same
  Flow without any additional timing machinery.
- **`streamNdjsonScheduled(...)`**: emit the `scheduled` `ControlEvent`,
  block on `schedService.awaitStart(id, delayMs)` exactly as today,
  then either emit `cancelled` and return, or fall through to the same
  `streamer.stream(sink)` pattern as the base case — all within the same
  `usingEmit` body.

### Error handling & terminal write

Inside each `usingEmit` body, the existing try/catch keeps its current
shape: on a client-disconnect, return quietly (no error element,
matching today). Otherwise, log at ERROR as today, then
`emit.apply(new NdjsonLine.ErrorFrame(problemPayload(cause)))` **instead
of rethrowing**. Because the error becomes a normally-emitted value
rather than a thrown exception, the Flow always completes successfully
from the terminal step's perspective.

This matters because `ByteFlow.runToOutputStream` closes the
`OutputStream` on any exception — which would fire before our own
error-frame writer got a chance to write into it, if we let the
exception propagate. Translating the error into a value *before* it
reaches the Flow boundary sidesteps that entirely.

A second, independent reason to avoid `runToOutputStream`: it never
calls `flush()` between chunks, only at the very end. For live
`follow`-mode tailing, that would delay heartbeats/records reaching the
client. So the terminal write is a manual `runForeach`, not
`runToOutputStream`:

```java
JsonFlow.renderNdjson(flow, ndjsonLineWriter)
    .runForeach(chunk -> {
        for (byte[] arr : chunk.getArrays()) outputStream.write(arr);
        outputStream.flush();
    });
```

This preserves today's per-line flush behavior and today's error-frame
contract (write the frame, then close normally) without needing
`runToOutputStream`'s close-on-error semantics.

### Scope boundaries & testing

- SSE methods (`streamSse`, `streamSseFollow`, `streamSseScheduled`)
  are untouched — no `OutputStream` involved.
- `SmartLifecycle`/shutdown (`activeThreads`, thread interruption on
  `stop()`) stays structurally as-is. Jox structured concurrency is
  itself interruption-based, so interrupting the tracked virtual thread
  should still cancel the `Scopes.supervised` block correctly — this
  must be explicitly verified against `SseReplayHandlerLifecycleTest`
  during implementation, not assumed from this spec alone.
- The wire format does not change (the transparent-union serializer
  produces byte-identical JSON lines to today's hand-written ones), so
  the existing NDJSON-exercising test suite (`SseReplayHandlerLifecycleTest`,
  `SseReplayHandlerErrorTest`, `CassetteControllerLastNExclusivityTest`,
  `CassetteControllerTransformValidationTest`, `TimestampSerializationIT`,
  `TopicReplayServiceTransformTest`, `BatchReplayTest`, and others) is the
  regression net, unchanged in what it asserts. One new unit test is
  added for the `NdjsonLine` serializer's transparent-union behavior
  itself, mirroring the existing `GuardedStep` round-trip tests.

## Risks & Non-Goals

- **Risk:** `Scopes.supervised` + `Flows.usingEmit` introduce Jox
  structured-concurrency machinery into a class that currently has
  none. The design keeps every method's control flow line-for-line
  equivalent to today specifically to minimize what actually changes,
  but the shutdown-interruption interaction (noted above) is new
  surface that needs explicit test coverage, not just code-reading
  confidence.
- **Risk:** `renderNdjson`'s NDJSON framing guarantees (UTF-8, LF after
  every line including the last, rejection of raw CR/LF from a
  pretty-printing writer) must be confirmed to match today's
  `BufferedWriter.newLine()` output byte-for-byte during implementation
  — existing tests that assert on exact response bodies will catch any
  drift.
- **Non-goal:** `ExportService.exportNdjson` — explicitly declined
  above; tracked separately.
- **Non-goal:** Redesigning `SmartLifecycle`/shutdown semantics — kept
  as-is, only verified against the new wrapper.
