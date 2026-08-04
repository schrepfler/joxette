# Write Resilience — Object-Store Circuit Breaker

## Problem

When the object store (S3, MinIO, GCS) is temporarily unreachable, DuckLake
attempts a Parquet HTTP PUT that fails with an `IO Error`. Before this feature,
that failure propagated up through the Jox flow, threw an exception in the
`TopicLifecycleActor`, and triggered a Pekko exponential-backoff actor restart
(5 s → 5 min). The Kafka consumer was torn down and re-subscribed on every
transient network blip, causing a recording gap of minutes.

Two additional risks:

- **Data loss**: the in-flight batch was dropped; Kafka offsets had not been
  committed, so messages were re-consumed after restart (at-least-once), but
  the window of exposure was large.
- **`max.poll.interval.ms` breach**: blocking the consumer VT during retries
  could exceed the Kafka broker's liveness timeout, triggering a rebalance.

---

## Solution — Three-State Circuit Breaker

The drain VT in `DuckLakeWriteChannel` implements a circuit breaker with three
states: `HEALTHY` → `DEGRADED` → `FAILED`.

```
Object store fails
       │
       ▼
  DEGRADED ──────────────────────────────────────────────────────────┐
       │                                                              │
  submit() throws SinkDegradedException immediately                  │ drain VT retries
  TopicRecorder pauses assigned partitions                            │ 1s → 2s → 4s → … → 60s
  spin-polls kc.poll() — heartbeats alive, no rebalance              │
       │                                                              │
       │◄─────────────────────── store recovers ─────────────────────┘
       │
  HEALTHY — sink resets, consumers resume, pipeline continues
            (no actor restart needed)

       OR (after writeRetryMaxAttempts failures)

  FAILED → SinkFailed message → TopicLifecycleActor throws
         → Pekko backoff restart → starting() calls resetSinkState()
         → consumers resume after actor comes back
```

---

## Component Responsibilities

### `DuckLakeWriteChannel`

- Owns the `SinkState` enum (`HEALTHY` / `DEGRADED` / `FAILED`).
- On first transient `SQLException` (IO Error, HTTP PUT/GET, connection
  refused/timeout): transitions to `DEGRADED`, logs a warning.
- Retries the same batch with exponential backoff (`writeRetryInitialMs`,
  `writeRetryMultiplier`, `writeRetryMaxMs`).
- On recovery: transitions back to `HEALTHY`, logs the attempt count.
- After `writeRetryMaxAttempts` consecutive failures: transitions to `FAILED`,
  completes the batch exceptionally, and invokes the registered
  `failureCallback` (set by `TopicLifecycleActor`).
- `submit()` throws `SinkDegradedException` immediately if state is
  `DEGRADED` or `FAILED` — producers are never blocked.
- `resetSinkState()` resets `FAILED`/`DEGRADED` → `HEALTHY`; called by
  `TopicLifecycleActor.starting()` on each actor restart.

### `TopicRecorder`

- Before each `submitWriteBatch()` call, checks `writeChannel.isHealthy()`.
- If not healthy, calls `pauseForSinkRecovery(kc, label)`:
  - Pauses all assigned `TopicPartition`s via `kc.pause()`.
  - Spin-polls `kc.poll(POLL_TIMEOUT)` in a loop — keeps consumer alive,
    sends heartbeats, never advances offsets.
  - When `writeChannel.isHealthy()` returns true, calls `kc.resume()` and
    returns to normal pipeline operation.
- Kafka offsets are **never committed** while paused — at-least-once
  delivery is preserved.

### `TopicLifecycleActor`

- Registers `failureCallback` on `writeChannel` after launching recorders
  (in `starting()`). The callback sends a `SinkFailed` message to the actor.
- Handles `SinkFailed` in the `recording` state: increments restart metric,
  throws `RuntimeException` → Pekko backoff restart.
- Calls `writeChannel.resetSinkState()` at the top of every `starting()`
  invocation so the next run starts from `HEALTHY`.

---

## Transient vs Non-Transient Failures

Only `SQLException`s whose message contains one of the following strings are
treated as transient and retried:

| Pattern | Example cause |
|---|---|
| `IO Error` | Generic DuckLake storage error |
| `HTTP PUT` | Parquet flush to object store |
| `HTTP GET` | Parquet read during merge |
| `Could not connect` | Store unreachable / DNS failure |
| `Connection refused` | Store port closed |
| `Connection timed out` | Network timeout |

All other failures (schema errors, constraint violations, null pointer, etc.)
are **not retried** — `processBatch` completes the batch exceptionally
immediately and the sink stays `HEALTHY`.

---

## Quarantine — Giving Up On a Poison Batch

A non-retryable failure is not automatically fatal for the topic: `submit()`
throws back to `TopicRecorder`, the actor restarts (Pekko backoff), and the
same uncommitted Kafka offsets are re-read and re-written on the next
generation. If the failure is truly deterministic (e.g. a schema mismatch, a
`CHECK` constraint the data will never satisfy), this repeats forever —
restart storm, zero progress, and the topic never advances past the poison
offset.

`DuckLakeWriteChannel` tracks consecutive non-retryable failures per batch
**identity** — `(topic, per-partition starting offsets)`, stable across
restarts because a failed write never advances the committed offset (see
`DuckLakeWriteChannel.batchIdentity()`). Once a given identity has failed
`quarantineAfterAttempts` times in a row, the drain VT gives up: it logs an
ERROR with the record count being dropped, increments
`joxette.recording.batches_quarantined` and
`joxette.recording.records_quarantined`, and completes the batch
*successfully* with zero records written instead of exceptionally. The
recorder commits past the poison offset and the topic resumes making
progress. **This is a deliberate, logged data-loss event** — the quarantined
records are gone, not retried again.

### Per-partition splitting

A batch reaching the drain VT is not always single-partition — upstream
`batchWeighted` coalescing can bundle several partitions together when the
writer is the bottleneck. If a multi-partition batch hits a non-retryable
failure, `DuckLakeWriteChannel` does **not** quarantine the whole thing:
`splitAndProcessByPartition` breaks it into one sub-batch per partition and
reprocesses each independently. This means:

- A poison record on partition 0 never holds partition 1's healthy records
  hostage — partition 1's sub-batch writes and commits normally in the same
  `submit()` call.
- Quarantine identity is always computed on a single-partition sub-batch, so
  it stays stable across restarts regardless of which other partitions
  happen to get coalesced alongside the poison one by upstream batching —
  the whole-batch composite identity was unstable for exactly this reason
  and is what motivated the split.

### Why quarantine is a batch-level, not a record-level, decision

`DuckLakeWriteChannel` gives up on the whole (post-split, single-partition)
batch rather than trying to isolate the one poison record inside it. Kafka
offsets are committed once per batch (the highest offset written), not once
per record — an individual record has no independent commit point to skip
past without also resolving every record ahead of it in the same batch. This
mirrors the granularity note in `splitAndProcessByPartition`'s javadoc: the
split goes down to "per partition" (because that's where an independent,
stable commit boundary and a stable quarantine identity both exist), but not
further to "per record" (because there is no such boundary at that level).

Lower `quarantine-after-attempts` to fail fast and stop tolerating a bad
topic sooner; raise it (or accept the restart storm) if you would rather
investigate before any data is dropped.

---

## Configuration

All keys live under `joxette.threading`:

| Key | Default | Description |
|---|---|---|
| `write-retry-initial-ms` | `1000` | First retry delay (ms) |
| `write-retry-multiplier` | `2.0` | Backoff multiplier per attempt |
| `write-retry-max-ms` | `60000` | Cap on retry delay (1 minute) |
| `write-retry-max-attempts` | `10` | Failures before escalating to actor restart; `-1` = unlimited |
| `quarantine-after-attempts` | `5` | Non-retryable failures on the SAME batch identity before giving up and committing past it — **causes record loss**; see "Quarantine" above |

Example override for a slow/flaky store:

```yaml
joxette:
  threading:
    write-retry-initial-ms: 2000
    write-retry-multiplier: 1.5
    write-retry-max-ms: 120000
    write-retry-max-attempts: 20
```

---

## Observability

- **Log level WARN**: `Sink DEGRADED for topic '...'` — first failure detected.
- **Log level INFO**: `Write succeeded after N retries — sink recovering to HEALTHY`.
- **Log level ERROR**: `Sink FAILED after N attempts — signalling supervisor restart`.
- **Micrometer metric**: `joxette_recording_restarts_total{topic=...}` increments
  on both `RecorderFailed` and `SinkFailed` paths.
- **Consumer lag**: rises during the pause window and drains on resume — the
  natural observable indicator of a degraded sink.
- **Log level ERROR**: `Quarantining permanently-failing batch ... N record(s)
  dropped` — a poison batch was given up on; see "Quarantine" above.
- **Micrometer metrics**: `joxette_recording_batches_quarantined_total{topic=...}`
  (batch count) and `joxette_recording_records_quarantined_total{topic=...}`
  (record count — alert on this one; a single batch can carry thousands of
  records) both increment when quarantine fires.

---

## Kafka Offset Safety

Offsets are committed at the **start of the next poll cycle**, after a
successful write. The sequence is:

```
poll() → emit records → batchWeighted → buildWriteBatch
       → pauseForSinkRecovery (if degraded)
       → submitWriteBatch (blocks until write succeeds)
       → pendingCommit.set(offsets)   ← only after success
       → next poll() → commitSync(pendingCommit)
```

During `pauseForSinkRecovery`, `pendingCommit` is null for the current batch
(the previous batch's offsets may be pending but are not advanced). No
commit fires until the write succeeds.

---

## Resilience Coverage Across Other I/O Flows

The same principles (classify transient vs non-transient, retry with backoff,
never silently drop) are applied to all other external I/O paths.

### 1. `KafkaRecordSink` — replay-to-topic Kafka producer

**File:** `joxette-kafka/.../sink/kafka/KafkaRecordSink.java`

**Problem:** `producer.send().get()` on a `RetriableException` (broker restart,
leader election, network blip) threw `SinkException` immediately, permanently
aborting the entire replay job.

**Fix:** Retry loop inside `doSend()` — up to `RETRY_MAX_ATTEMPTS` (5) on any
`RetriableException`, with exponential backoff starting at 500 ms, capped at
30 s. Non-retriable `ExecutionException` causes abort immediately.

### 2. `ExportService` — DuckDB COPY to S3

**File:** `joxette-service/.../exports/ExportService.java`

**Problem:** A single S3 throttle or network blip during `COPY … TO …` caused
`markFailed()` to be called permanently — the user had to re-trigger the export.

**Fix:** Retry loop in `execute()` wraps the entire export attempt. Uses the
same `DuckDbErrors.isTransient()` check as `DuckLakeWriteChannel` (IO
Error / HTTP PUT / HTTP GET / Could not connect / Connection refused / timed
out). Up to 5 attempts with 2 s initial backoff, capped at 60 s.

### 3. `CassetteLifecycleService` — S3 snapshot upload

**File:** `joxette-service/.../replay/CassetteLifecycleService.java`

**Problem:** Uploading a snapshot to S3 iterated all files in a loop with no
retry. A transient S3 error mid-upload left a partial snapshot in object
storage (some files uploaded, some missing) with no recovery path.

**Fix:** Each file is uploaded via `uploadWithRetry(file, key, bucket)`. Uses
`SdkException.retryable()` (the AWS SDK's own signal) to distinguish transient
from permanent failures. Up to 5 attempts per file, 1 s initial backoff, capped
at 30 s. A non-retryable failure throws `UpstreamUnavailableException`, which
aborts the entire snapshot upload cleanly.

### 4. `KnownEntitiesRepository` — entity registry upserts

**File:** `joxette-service/.../replay/KnownEntitiesRepository.java`

**Problem:** `upsertBatch()` threw `SQLException` on failure. The caller
(`TopicRecorder`) caught and logged it at WARN and continued — correct for
best-effort semantics, but silent drift was invisible until it became severe.

**Fix:** Exception handling moved inside `upsertBatch()`. A `consecutiveFailures`
`AtomicInteger` tracks how many batches have failed in a row:

| Failures | Log level |
|---|---|
| 1–2 | WARN — isolated failure |
| 3–9 | WARN — escalating, mentions count |
| 10+ | ERROR — "registry is drifting from reality" |

Counter resets to 0 on any success. `consecutiveFailures()` is exposed as a
public method so health checks or metrics can surface it.

#### Kafka consumer poll interval vs. shared-connection lock contention

`upsertBatch()` deliberately runs on the same shared `synchronized(duckDB)`
connection lock as every other write path (see "DuckDB Connection Model" in
`CLAUDE.md`) rather than a duplicated connection — Task 5's bucket-count-change
guard in `EntityController.updateEntityType` depends on `upsertBatch()`
contending for that *same* monitor to close its check-then-act window. Giving
`KnownEntitiesRepository` its own connection (the way `CassetteBatchWriter`
does for general-cassette writes, below) would silently reopen that race.

The cost of keeping it on the shared connection: `upsertBatch()` runs on the
live Kafka consumer's own virtual thread (via `TopicRecorder.submitWriteBatch`)
for every entity-routed batch, and it now contends for the same lock as
`CompactionService`'s merge, `RetentionService`'s rewrite, and
`CassetteLifecycleService`'s snapshot/restore operations — all of which can
legitimately hold `synchronized(duckDB)` for minutes (compaction's own safety
margin, `lock-ttl-minutes`, defaults to 240 minutes precisely because a merge
can run that long in the worst case; see `docs/clustering-deployment.md` §4.2).
If one of those operations is mid-hold when the consumer thread's
`upsertBatch()` call needs the lock, the consumer's next `poll()` is delayed by
however long it waits — and if that wait exceeds Kafka's
`max.poll.interval.ms`, the broker evicts the consumer and triggers a group
rebalance for an entity-routed topic that was never actually stuck, just
waiting its turn.

Kafka's own default for `max.poll.interval.ms` (5 minutes) is not a safe floor
given that ceiling. `joxette.kafka.max-poll-interval-ms` (see
`JoxetteProperties.Kafka#getMaxPollIntervalMs()`) is therefore raised to
**15 minutes** by default — comfortably above the realistic minutes-scale
duration of a single merge/rewrite/snapshot lock hold, while staying well
short of the 240-minute worst-case TTL, so a consumer that is genuinely stuck
(as opposed to just waiting on a slow-but-legitimate lock holder) still
triggers a rebalance in a reasonable time rather than being masked for hours.
Raise it further if profiling shows realistic lock-hold durations exceeding
that margin for your data volumes.

By contrast, `CassetteBatchWriter`'s general-cassette writes use a
**duplicated** connection specifically to avoid this class of contention
entirely — general-cassette recording has no equivalent cross-request
invariant forcing it onto the shared connection, so it opts out of the
contention instead of just budgeting for it.

### 5. `RetentionService` — per-entity-type progress isolation

**File:** `joxette-service/.../compaction/RetentionService.java`

**Problem:** `enforceEntityRetention()` iterated entity types and threw on any
`SQLException`. A failure on entity type N caused the entire run to be marked
FAILED, discarding the N−1 types already processed. The next scheduled run
restarted from entity type 1, re-processing all prior work.

**Fix:** Each entity type (`deleteFromEntityCassette` + `deleteFromKnownEntities`)
is wrapped in its own try/catch. A failure logs at WARN with "will retry next
schedule" and continues to the next entity type. The run completes with partial
progress recorded rather than fully failing. Same isolation applied to
`enforceTopicRetention()` per-topic.

### 6. `CompactionService` — transient S3 error classification

**File:** `joxette-service/.../compaction/CompactionService.java`

**Problem:** All `SQLException` from `ducklake_merge_adjacent_files` were logged
at the same WARN level. A transient S3 timeout was indistinguishable from a
schema error in the logs.

**Fix:** `DuckDbErrors.isTransient()` used to distinguish S3 failures from
schema errors. Transient errors get a distinct log message:
`"transient S3 failure … will retry next run"`. Non-transient errors keep the
existing `"failed for entity_type='...'"` message. Both return
`CompactionResult.NONE` — compaction is idempotent, so the next scheduled run
re-attempts the same file merge.

### 7. `TopicRecorder` Kafka poll loop

**File:** `joxette-service/.../recording/TopicRecorder.java`

**Problem:** On `RetriableException` (broker restart, network loss), the poll
loop continued immediately with the next `kc.poll()` call (100 ms timeout). A
sustained broker failure produced WARN log entries at 10/s, saturating log
aggregators and obscuring other signals.

**Fix:** `kafkaRetryDelayMs` field (initially `KAFKA_RETRY_INITIAL_MS` = 500 ms)
tracks the current backoff. On each `RetriableException`: sleep `kafkaRetryDelayMs`,
then multiply by `KAFKA_RETRY_MULTIPLIER` (2.0), capped at `KAFKA_RETRY_MAX_MS`
(30 s). On any successful poll that returns records: reset to 500 ms. This gives
one log line per retry window instead of a storm at 100 ms cadence.

---

## Shared Transient Detection — `DuckDbErrors.isTransient()`

**File:** `joxette-service/.../db/DuckDbErrors.java`

All three callers (`DuckLakeWriteChannel`, `ExportService`, `CompactionService`)
delegate to a single shared utility.

### Why message matching?

DuckDB JDBC always returns `null` for `getSQLState()` and `0` for
`getErrorCode()` across every error type — IO errors, schema errors, and
constraint violations all look the same to the JDBC API. There is no exception
subclass hierarchy; every error surfaces as a bare `java.sql.SQLException`
thrown from native JNI code. Message prefix matching against DuckDB's own
error-type labels is the only reliable signal available.

This was verified empirically with a probe against DuckDB JDBC 1.5.4 (`IO Error`,
`Catalog Error`, and `Constraint Error` all returned `getSQLState() == null`).

### Denylist-first strategy

Rather than allowlisting transient patterns (the previous approach), we
**denylist known non-transient ones**. Any `SQLException` that does not match
a non-transient prefix is treated as transient and retried. This is safe
because all Joxette write paths are idempotent:

- A false-positive retry (unknown error retried unnecessarily) wastes time but causes no data corruption.
- A false-negative (transient error treated as permanent) drops data permanently.

**Non-transient prefixes — never retried:**

| Prefix | Meaning |
|---|---|
| `Catalog Error` | Missing table or schema |
| `Binder Error` | Type or name resolution failure |
| `Parser Error` | Malformed SQL |
| `Constraint Error` | PK / FK / unique violation |
| `Invalid Input Error` | Bad function argument |
| `Not implemented Error` | Unsupported feature |
| `Permission Error` | Access control denial |
| `Out of Range Error` | Value overflow |

**Transient prefixes — retried:**

| Pattern | Meaning |
|---|---|
| `IO Error` | Generic DuckLake storage error |
| `HTTP PUT` / `HTTP GET` / `HTTP Error` | Object store request failure |
| `Could not connect` | Store unreachable / DNS failure |
| `Connection refused` / `Connection timed out` | Network timeout |

Everything else (unrecognised prefix, null message) is treated as transient.

### Updating the classification

Edit `DuckDbErrors.java` — it is the single source of truth. To verify the
exact message prefix for a new error type, run:

```java
try (Statement st = conn.createStatement()) {
    st.execute("/* trigger the error */");
} catch (SQLException e) {
    System.out.println(e.getMessage());      // the prefix to match
    System.out.println(e.getSQLState());     // always null in DuckDB JDBC
    System.out.println(e.getErrorCode());    // always 0 in DuckDB JDBC
}
```
