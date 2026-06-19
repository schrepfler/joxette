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

## Configuration

All keys live under `joxette.threading`:

| Key | Default | Description |
|---|---|---|
| `write-retry-initial-ms` | `1000` | First retry delay (ms) |
| `write-retry-multiplier` | `2.0` | Backoff multiplier per attempt |
| `write-retry-max-ms` | `60000` | Cap on retry delay (1 minute) |
| `write-retry-max-attempts` | `10` | Failures before escalating to actor restart; `-1` = unlimited |

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
