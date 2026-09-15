# ExportService NDJSON Appender Fix

## Motivation

While evaluating jox-json for `SseReplayHandler`'s NDJSON streaming
(`docs/superpowers/specs/2026-09-15-ndjson-jox-json-migration-design.md`),
`ExportService.exportNdjson` was benchmarked and found to have a real
performance problem unrelated to Jackson or jox-json: it collects every
record into an in-memory `List<EntityRecord>` first, then bulk-inserts
each serialized line into a DuckDB temp table via JDBC
`PreparedStatement.addBatch()`/`executeBatch()`, then `COPY`s that table
out to the final file (local or `s3://`, via DuckDB's `httpfs`
extension).

A `jbang` benchmark (synthetic records shaped like `EntityRecord`, DuckDB
JDBC 1.5.5.1) found the `PreparedStatement` batch insert step alone takes
21.4s at 500k rows — 98.4% of total export time — and does not improve
with chunk size (1k/10k/50k/one-shot batches all ≈8.4s at 200k rows).
DuckDB's native `Appender` API does the same 200k-row insert in 137ms —
about 61× faster. A second benchmark of the multi-row
`INSERT ... VALUES (?),(?),...` technique `CassetteBatchWriter` already
uses elsewhere in this codebase landed at ≈1.7s for 500k rows (a ~12×
win, using an already-established pattern) — considered as an
alternative, but the `Appender` API was chosen for the larger margin.

This spec covers only the fix; the benchmark and architecture comparison
that motivated it are recorded in the artifact linked from the jox-json
migration spec.

## Scope

**In scope:** `ExportService.exportNdjson` (and a new private helper it
uses) in
`joxette-service/src/main/java/com/joxette/exports/ExportService.java`.

**Out of scope:**
- `exportParquet` and `loadAll` — both untouched. `loadAll` is shared
  between `exportParquet` and `exportNdjson` today; `exportParquet`'s own
  batch-insert step was not part of the benchmark and is not part of
  this fix. `exportNdjson` gets its own new streaming helper instead of
  changing `loadAll`'s contract for `exportParquet`.
- jox-json / Jackson — this is a pure DuckDB usage fix, independent of
  the JSON library question.
- Everything downstream of the temp table (`COPY ... TO ...`, `s3://`
  export via `httpfs`) — unchanged.

## Target Architecture

### Connection handling

DuckDB temp tables are scoped to a single connection. The fix duplicates
the shared `duckDB` connection once at the start of `exportNdjson`
(`duckDB.unwrap(DuckDBConnection.class).duplicate()` — the same pattern
`CassetteBatchWriter` already uses to get an independent connection per
writer) and uses that one duplicated connection consistently for
creating the temp table, appending rows, running `COPY`, and dropping
the table, all inside one try-with-resources block so it's always
closed. This also closes a pre-existing gap: today's `exportNdjson` uses
the shared `duckDB` connection directly with no `synchronized(duckDB)`
wrapping, which the project's threading model requires for that shared
connection (see `CLAUDE.md`'s "DuckDB Connection Model"). Duplicating the
connection sidesteps the need for that lock entirely, for the same
reason `CassetteBatchWriter` does.

### Streaming records into the Appender

`exportNdjson` gets a new private helper (`loadAll` is untouched, still
used by `exportParquet`). The helper loops over `entityIds` the same way
`loadAll` does today, calling the same
`entityReplayService.streamEntityEvents(...)` overload, but the sink
appends each record directly instead of collecting it:
```java
appender.beginRow();
appender.append(objectMapper.writeValueAsString(record));
appender.endRow();
```
`Consumer<EntityRecord>.accept` can't declare the checked `SQLException`
`DuckDBAppender`'s methods throw, so the sink wraps it in a
`RuntimeException` and the call site unwraps/rethrows it — the same
wrap-then-rethrow shape already used for checked exceptions inside
`Consumer`/`FlowEmit`-style callbacks elsewhere in this codebase (e.g.
`FlowReplayEngine`'s `emit.apply` wrapping). A `long` counter incremented
per appended row replaces `records.size()` as the return value.

### Empty-result handling

Today's code checks `records.isEmpty()` before touching the database and
returns `0L` immediately, creating no temp table and no output file.
Streaming can't know the count in advance, so the check moves to after
the appender closes: if the counter is `0`, the (empty) temp table is
dropped and `0L` is returned without running `COPY` — preserving
"no output file when there's nothing to export" exactly, checked at the
other end of the operation instead of the start.

### Method shape

`exportNdjson`'s overall structure becomes:
1. Duplicate the connection (try-with-resources).
2. `CREATE TEMP TABLE` (unchanged SQL).
3. Open a `DuckDBAppender` on that table (try-with-resources, so it's
   flushed/closed before the row count is read).
4. Loop over `entityIds`, calling `streamEntityEvents(...)` per entity
   with a sink that appends directly, incrementing the row counter.
5. If the counter is `0`: `DROP TABLE`, return `0L`.
6. Otherwise: run the existing `COPY (SELECT line FROM ...) TO ...`
   statement, `DROP TABLE`, return the counter.

## Testing

- `ExportControllerTest`/`ExportJobRepositoryTest` (existing) continue to
  exercise the public `submit`/`get`/`list` contract unchanged — this fix
  doesn't touch `ExportService`'s public API or `ExportJob` shape.
- A new unit-level test exercises `exportNdjson` against a real embedded
  DuckDB connection (matching this project's existing DuckDB test
  conventions — no Testcontainers needed, DuckDB runs embedded) with a
  handful of synthetic entity records, asserting: (a) the returned row
  count matches the number of records streamed, (b) the output file
  contains one JSON line per record in the same shape
  `objectMapper.writeValueAsString` would produce today, (c) the
  zero-records case returns `0L` and creates no output file.
- No existing test currently exercises `exportNdjson`'s row-count-return
  value or output file contents directly (confirm during planning) —
  this new test is the first direct coverage of that contract, not a
  replacement for missing coverage the plan should also flag if found.

## Risks & Non-Goals

- **Risk:** `DuckDBAppender.append(String)` must produce byte-identical
  VARCHAR values to `PreparedStatement.setString(1, ...)` for the same
  input — expected (both go through the same DuckDB VARCHAR encoding
  path), but the new test's exact-content assertion is what actually
  confirms it.
- **Risk:** duplicating the connection per export call means concurrent
  exports each get their own connection, which is the intended fix for
  the missing-synchronization gap — but this should be confirmed by
  checking whether `DuckDBConnection.duplicate()` has a meaningful cost
  or limit worth knowing about before assuming it's free for
  short-lived, per-job use (`CassetteBatchWriter` uses it for
  long-lived, per-topic writers instead).
- **Non-goal:** `exportParquet`'s own batch-insert step — not
  benchmarked, not touched.
- **Non-goal:** the multi-row `VALUES` INSERT alternative — evaluated,
  not chosen (see Motivation).
