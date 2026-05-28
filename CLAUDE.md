# Joxette — Kafka Topic Cassette Recorder

## Overview

Joxette is a service that records Kafka topics into "cassettes" — replayable archives stored in DuckLake backed by object storage. Cassettes can be **general** (raw topic stream in order) or **entity-specific** (messages grouped by a business entity like an order, user, or device across multiple topics).

The service leverages DuckLake's **data inlining** feature to buffer small writes in the catalog database before flushing to Parquet on object storage, reducing S3 PUT costs and avoiding the small files problem.

---

## Tech Stack

| Component | Technology | Version |
|---|---|---|
| Language | Java (pure, no Kotlin) | JDK 25 |
| Framework | Spring Boot | 4.0.5 |
| Build | Maven | latest |
| Concurrency/Flows | Jox (softwaremill) | latest |
| Kafka | Jox Kafka module | latest |
| Structured concurrency | Jox structured concurrency | latest |
| Database | DuckDB JDBC | org.duckdb:duckdb_jdbc:1.5.3.0 |
| Storage format | DuckLake (ducklake extension) | latest |
| Object storage | Delegated entirely to DuckDB/DuckLake layer |
| Testing | Testcontainers (Kafka + DuckDB) | latest |

### Key library documentation
- Jox flows, concurrency, backpressure: https://jox.softwaremill.com/latest/
- Jox structured concurrency: https://jox.softwaremill.com/latest/structured.html
- Jox Kafka module: https://jox.softwaremill.com/latest/kafka.html

---

## Core Concepts

### Cassettes

A **cassette** is a replayable recording of Kafka messages. Two types:

- **General cassette**: raw topic stream, one DuckLake table per topic, messages in original order.
- **Entity cassette**: messages for a specific business entity aggregated across multiple topics, one DuckLake table per entity type.

### Data Inlining

DuckLake buffers small writes in the catalog database (DuckDB file) before flushing to Parquet on object storage. This means:
- Sub-millisecond writes for small batches
- Fewer Parquet files on S3 = fewer PUT requests = lower cost
- Queries transparently read from both inlined data and Parquet files — no application-level UNION needed
- Replay cursors must be logical (timestamp, partition, offset), not physical (file path, row number), because the same row may move from inlined to Parquet between queries

### DuckDB as Catalog

DuckDB is used as the DuckLake catalog database (not PostgreSQL). This means:
- Single process only — no multi-process writes
- All Kafka consumer threads share one DuckDB JDBC connection; DuckDB serializes writes internally
- Multiple concurrent reads are fine (multiple Statement objects from one Connection)
- If multi-process writes are needed later, there is a **three-stage scaling path** (see `docs/catalog-scaling.md`):
  1. **Embedded DuckDB** (current) — single process, zero ops overhead
  2. **Quack server** (DuckDB 1.5.3+, beta) — DuckDB semantics, multi-process, no PostgreSQL needed; evaluate at DuckDB 2.0 GA
  3. **PostgreSQL** — if Quack becomes a bottleneck; DuckLake schema is identical across all backends, only the connection string changes

---

## Schema

### General Cassette Table (one per topic, in DuckLake)

Table name: `lake.cassette_{sanitized_topic}`

```sql
CREATE TABLE IF NOT EXISTS lake.cassette_{sanitized_topic} (
    topic           VARCHAR NOT NULL,
    headers         LIST(STRUCT(key VARCHAR, value BLOB)),
    "timestamp"     TIMESTAMPTZ NOT NULL,
    "partition"     INTEGER NOT NULL,
    "offset"        BIGINT NOT NULL,
    key             VARCHAR,
    value           VARIANT,  -- confirmed working: duckdb_jdbc 1.5.3.0 + ducklake; see SchemaManager.probeVariant()
    recorded_at     TIMESTAMPTZ NOT NULL
);
```

### Entity Cassette Table (one per entity type, in DuckLake)

Table name: `lake.entity_{sanitized_type}`

```sql
CREATE TABLE IF NOT EXISTS lake.entity_{sanitized_type} (
    entity_id       VARCHAR NOT NULL,
    entity_bucket   INTEGER NOT NULL,
    source_topic    VARCHAR NOT NULL,
    headers         LIST(STRUCT(key VARCHAR, value BLOB)),
    "timestamp"     TIMESTAMPTZ NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset   BIGINT NOT NULL,
    key             VARCHAR,
    value           VARIANT,  -- confirmed working: duckdb_jdbc 1.5.3.0 + ducklake; see SchemaManager.probeVariant()
    recorded_at     TIMESTAMPTZ NOT NULL
);
```

Partitioned in DuckLake by `(entity_bucket, date)`. Entity bucket = `hash(entity_type, entity_id) % configured_bucket_count`.

### Known Entities Registry (in DuckLake)

```sql
CREATE TABLE IF NOT EXISTS lake.known_entities (
    entity_type     VARCHAR NOT NULL,
    entity_id       VARCHAR NOT NULL,
    first_seen      TIMESTAMPTZ NOT NULL,
    last_seen       TIMESTAMPTZ NOT NULL,
    message_count   BIGINT NOT NULL,
    source_topics   VARCHAR[] NOT NULL,
    PRIMARY KEY (entity_type, entity_id)
);
```

Maintained during ingest: upsert per entity per batch. Powers entity listing, search, and stats endpoints.

### Configuration Tables (plain DuckDB, NOT DuckLake)

These live in the same DuckDB database file but outside DuckLake — they are operational config, not lakehouse data.

```sql
CREATE TABLE IF NOT EXISTS topic_configs (
    topic           VARCHAR PRIMARY KEY,
    mode            VARCHAR NOT NULL,        -- 'general', 'entity_only', 'both'
    time_partition  VARCHAR DEFAULT 'hour',
    max_file_size_mb INTEGER DEFAULT 256,
    max_records_per_file INTEGER DEFAULT 1000000,
    retention_days  INTEGER DEFAULT 90,
    consumer_group  VARCHAR,
    start_from      VARCHAR DEFAULT 'latest',
    paused          BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS entity_type_configs (
    entity_type     VARCHAR PRIMARY KEY,
    buckets         INTEGER DEFAULT 256,
    retention_days  INTEGER DEFAULT 365,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS entity_source_mappings (
    entity_type     VARCHAR NOT NULL,
    topic           VARCHAR NOT NULL,
    entity_id_source    VARCHAR NOT NULL,     -- 'key', 'value', 'header'
    entity_id_expression VARCHAR NOT NULL,    -- JSONPath or literal
    entity_type_source   VARCHAR,            -- null means literal in entity_type_configs
    entity_type_expression VARCHAR,
    PRIMARY KEY (entity_type, topic)
);

CREATE TABLE IF NOT EXISTS compaction_history (
    id              BIGINT PRIMARY KEY,
    target          VARCHAR NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ,
    status          VARCHAR NOT NULL,
    files_before    INTEGER,
    files_after     INTEGER,
    bytes_reclaimed BIGINT
);
```

### Headers

Kafka headers allow duplicate keys and binary values. We store as `LIST(STRUCT(key VARCHAR, value BLOB))`.

Provide helper functions (registered as DuckDB macros at startup):
- `headers_get(headers, 'key')` → first value for that key
- `headers_get_all(headers, 'key')` → all values for that key as a list
- `headers_put(headers, 'key', value)` → appends a key-value pair
- `headers_to_map(headers)` → lossy conversion to `MAP(VARCHAR, VARCHAR)`, last-write-wins on dupes

### VARIANT vs JSON

**Decision: use VARIANT (confirmed, duckdb_jdbc 1.5.3.0).**

DuckDB 1.5 introduced the VARIANT type (JSON shredded to binary, up to 100× faster analytical queries). We ran a full round-trip probe through DuckLake's Parquet serialisation path — inlining disabled to force the Parquet code path — using 18 test cases spanning four payload classes:

| Test class | What it covers |
|---|---|
| `RoundTripTests` | Core probe + four JSON payload shapes (object, string, nested, array) |
| `DecimalParquetTests` | Small decimal Parquet encoding fix in 1.5.3 (`0.01`, `99.50`, `1234567.89`, `0.001`) |
| `SelectionVectorTests` | Selection-vector indexing fix in 1.5.3 (5 rows read back in order; filtered SELECT returns correct rows) |
| `CleanupTests`, `InliningRestoreTests`, `SchemaManagerInitializeTests` | Lifecycle correctness |

All 18 tests pass. VARIANT is therefore the column type for the `value` / `metadata` column in both general and entity cassette tables. `SchemaManager.probeVariant()` still runs at startup as a safety net — if a future DuckLake build regresses, it falls back to JSON automatically with no schema change needed.

---

## Entity Routing

### Configuration Model

Entity routing is entity-centric, not topic-centric. One entity type can have messages in multiple topics.

```yaml
entities:
  - type: "order"
    buckets: 256
    sources:
      - topic: "orders.events"
        entity_id:
          source: value              # 'key', 'value', or 'header'
          expression: "$.order_id"   # JSONPath
      - topic: "payments.events"
        entity_id:
          source: value
          expression: "$.payment.order_id"
```

Both `entity_type` and `entity_id` can be:
- **literal**: a static string (e.g., entity type is always "order")
- **expression**: extracted from the message via JSONPath on key, value, or header

### Topic Recording Modes

Each topic has a mode:
- `general` — record raw topic stream only, no entity extraction
- `entity_only` — extract entities and write only to entity cassettes, discard unmatched messages
- `both` — record general stream AND route to entity cassettes

### Cross-Topic Entity Ordering

Entity cassettes aggregate messages from multiple topics. Ordering is by `timestamp` (Kafka producer timestamp) as primary, `recorded_at` (service ingestion time) as tiebreaker. Cross-topic ordering is best-effort based on producer timestamps (clock skew is possible).

### Bucketed Entities

Entity IDs are hashed into a configurable number of buckets (e.g., 256). Parquet files are partitioned by `(entity_bucket, date)`. Replaying one entity scans only its bucket's files. File layout:

```
/{topic}/{date_or_hour}/{entity_bucket}_{sequence}.parquet
```

---

## Recording Pipeline (Jox)

```
Jox KafkaSource (per topic)
    │
    ▼
Flow<KafkaMessage>
    │
    ├── .grouped(batchSize, batchTimeout)   ── batching for throughput
    │
    ▼
Flow<List<KafkaMessage>>
    │
    ├── .map(batch -> route(batch))         ── entity extraction + routing
    │
    ▼
Flow<RoutedBatch>
    │
    ├── .map(batch -> write(batch))         ── DuckLake bulk insert
    │
    ▼
Flow<WriteResult>
    │
    ├── .map(result -> commit(result))      ── Kafka offset commit
    │
    ▼
    drain
```

### Structured Concurrency

Each topic recorder runs in a Jox supervised scope. If DuckLake writer fails → scope cancels Kafka source. If Kafka source disconnects → everything shuts down cleanly. `RecordingCoordinator` manages these scopes and can start/stop them dynamically via REST API.

### Backpressure

Flows naturally: DuckLake writes slow down → batching buffer fills → Kafka consumption slows → consumer lag increases. Lag is the pressure valve.

### At-Least-Once Semantics

Kafka delivers at-least-once. Deduplication on read (not write): the `(topic, partition, offset)` tuple is unique, replay queries can use `QUALIFY` or `DISTINCT ON` to filter duplicates. Duplicates in storage are harmless.

### Batching Config

```yaml
joxette:
  recording:
    batch-size: 10000
    batch-timeout-ms: 1000
```

---

## Threading Model

### Virtual Threads (Java 25)

Spring Boot 4 runs on a virtual thread executor by default. Every HTTP request, scheduled task, and Jox-managed fiber runs on a virtual thread. Platform thread pool sizing (`corePoolSize`, `maxPoolSize`) is irrelevant — the JVM schedules virtual threads onto carrier threads automatically. The only tuning knob is the number of carrier threads, which defaults to `Runtime.availableProcessors()` and rarely needs changing.

Virtual threads make blocking I/O cheap. Kafka poll, DuckDB JDBC calls, and object storage reads all block without consuming carrier threads. No reactive programming or async callbacks are required.

### DuckDB Write Serialization

DuckDB enforces single-writer semantics. Rather than relying on JDBC-level synchronization, all write paths funnel through a single bounded `Channel<WriteBatch>` (Jox). One dedicated virtual thread drains this channel and executes every INSERT/UPSERT sequentially.

This mirrors the Akka pinned-dispatcher pattern for blocking I/O: one logical thread owns the resource. The difference is that here no explicit thread pinning is needed — the drain loop is a tight virtual-thread loop that never yields voluntarily between dequeue and execute, so the JVM keeps it on the same carrier for the duration of each write.

```
Kafka VT 1 ──┐
Kafka VT 2 ──┼──▶  Channel<WriteBatch> (bounded, capacity N)  ──▶  drain VT  ──▶  DuckDB
Kafka VT N ──┘
```

Reads (replay API) bypass the channel entirely: each request opens a separate `Statement` on the shared `Connection`. DuckDB permits concurrent reads.

### Thread and Scope Responsibilities

| Subsystem | Concurrency unit | Parallelism | Notes |
|---|---|---|---|
| Kafka consumers | One Jox supervised scope per topic | Dynamic (one VT per source by default) | Managed by `RecordingCoordinator` |
| DuckDB writes | Single VT draining `Channel<WriteBatch>` | 1 always | Serializes all INSERTs; natural backpressure via channel capacity |
| DuckDB reads (replay) | Virtual thread per HTTP request | Unbounded (VT) | Separate `Statement` per request; concurrent reads safe |
| Compaction | Dedicated Jox scope, cron-triggered | 1 (isolated lifecycle) | Reads and writes; holds write channel slot during merge |
| REST API | Spring Boot 4 virtual thread executor | Unbounded (VT) | Default; no configuration needed |

### Dynamic Topology

`RecordingCoordinator` owns a `Map<String, RecorderScope>` keyed by topic name.

- **Add topic**: fork a new child Jox scope under the coordinator's root scope; the scope starts the Kafka source and wires it to the shared write channel.
- **Remove topic**: cancel the scope for that topic; the Kafka source stops, in-flight batches already enqueued on the write channel drain normally before the scope exits.
- **Write channel**: created once at startup, lives for the service lifetime. Adding or removing topics does not recreate or resize it.

### Backpressure

The write channel capacity (`N × batchSize` slots) is the primary backpressure valve. When DuckDB writes are slow, the channel fills, Jox `send` blocks the producing virtual thread, the Kafka poll loop stalls, and consumer lag rises. Lag is the observable downstream indicator of write pressure.

No explicit flow-control protocol is needed — Jox channel semantics provide it for free.

### Per-Topic Write Isolation Tradeoff

The default design uses a single global write channel shared across all topics.

| Approach | Pros | Cons |
|---|---|---|
| Single global channel (default) | Simple; one drain loop; no per-table coordination | A slow table (large entity merge) can delay writes for unrelated topics |
| Per-table channels | Noisy-neighbor isolation; each topic drains independently | N drain loops; N-way coordination on `known_entities` upserts; more complex lifecycle |

Start with the single channel. If noisy-neighbor latency becomes measurable, promote to per-table channels by adding a `Map<String, Channel<WriteBatch>>` in `CassetteBatchWriter` and a drain VT per channel.

### Configuration

```yaml
joxette:
  threading:
    write-channel-capacity: 128          # slots in the global write channel (default 128)
    default-source-parallelism: 1        # VTs per Kafka source (default 1)
    topic-parallelism:
      high-volume-topic: 2               # per-topic override
    compaction-thread-type: virtual      # virtual | platform (default virtual)
```

| Property | Default | Effect |
|---|---|---|
| `write-channel-capacity` | `128` | Bounded channel size; raise if write bursts cause unnecessary consumer lag |
| `default-source-parallelism` | `1` | VTs per Jox KafkaSource; increase only for high-partition topics with CPU-bound routing |
| `topic-parallelism.<topic>` | inherits default | Per-topic parallelism override |
| `compaction-thread-type` | `virtual` | Set to `platform` if a compaction library holds a monitor across I/O and triggers virtual-thread pinning warnings |

---

## Compaction

### What Gets Compacted

- **Inlined data flush** — handled automatically by DuckLake, controlled by inline threshold settings
- **General cassette compaction** — off by default, only needed if service restarts leave undersized files
- **Entity cassette compaction** — most important; merges small files per bucket into larger ones

### Entity Compaction Strategy

1. Scan buckets where file count exceeds threshold (e.g., >10 files per bucket per time window)
2. Read all files in that bucket, merge, re-sort by `(entity_id, timestamp, recorded_at)`
3. Write back as fewer, larger files
4. DuckLake catalog updated to point to new files, old files marked for deletion

### Triggering

- **Cron** — configurable schedule (default: daily at 3am)
- **REST** — `POST /compaction/trigger` with optional body to scope to specific targets
- **Lookback** — only compact data older than N days (default: 30) to avoid churning hot data

### Configuration

```yaml
joxette:
  compaction:
    schedule: "0 3 * * *"
    entity:
      min-files-per-bucket: 10
      target-file-size-mb: 256
      lookback-days: 30
    general:
      enabled: false
      min-files-per-partition: 20
      target-file-size-mb: 256
```

---

## REST API

### Topic Recording Management

| Method | Path | Description |
|---|---|---|
| GET | `/topics` | List all configured topic recordings with mode and status |
| POST | `/topics` | Register a new topic for recording |
| GET | `/topics/{topic}` | Get config and stats for a topic |
| PUT | `/topics/{topic}` | Update topic config |
| DELETE | `/topics/{topic}` | Stop recording (doesn't delete data) |
| POST | `/topics/{topic}/pause` | Pause consumption |
| POST | `/topics/{topic}/resume` | Resume consumption |

### Entity Type Management

| Method | Path | Description |
|---|---|---|
| GET | `/entities` | List all registered entity types with stats |
| POST | `/entities` | Register a new entity type |
| GET | `/entities/{entity_type}` | Get config, source mappings, stats |
| PUT | `/entities/{entity_type}` | Update entity config |
| DELETE | `/entities/{entity_type}` | Remove entity type (doesn't delete data) |
| GET | `/entities/{entity_type}/sources` | List topic→entity mappings |
| POST | `/entities/{entity_type}/sources` | Add a source topic mapping |
| DELETE | `/entities/{entity_type}/sources/{topic}` | Remove a source mapping |

### Cassette Replay

All replay endpoints support three response formats via `Accept` header:
- `application/json` — paginated response (default)
- `text/event-stream` — SSE streaming
- `application/x-ndjson` — NDJSON streaming

All read endpoints also accept `order=asc|desc`, `follow=true` (live-tail after
history drain; SSE/NDJSON; incompatible with `to`/`offset_to`),
`transform`/`transform_preset` (apply a transform pipeline on read), and
`start_at`/`start_delay_ms` (delayed start).

**General cassettes:**

| Method | Path | Query Params | Description |
|---|---|---|---|
| GET | `/cassettes/topics/{topic}` | `from`, `to`, `partition`, `offset_from`, `offset_to`, `limit`, `cursor`, `order`, `follow` | Replay general cassette |

**Entity cassettes:**

| Method | Path | Query Params | Description |
|---|---|---|---|
| GET | `/cassettes/entities/{entity_type}` | `prefix`, `limit`, `cursor`, `active_since` | List known entity IDs |
| GET | `/cassettes/entities/{entity_type}/{entity_id}` | `from`, `to`, `limit`, `cursor`, `order`, `follow`, `last_n`, `dedup`, `message_types`, `output`, `state_fold`, `response_format`, `timeline_bucket`, `sol`, `sol_output` | Replay entity cassette |
| GET | `/cassettes/entities/{entity_type}/{entity_id}/stats` | — | Message count, time range, source topics |
| GET | `/cassettes/entities/{entity_type}/search` | `from`, `to`, `source_topic`, `min_messages`, `limit` | Find entities matching criteria |
| POST | `/cassettes/entities/{entity_type}/batch` | body: `{ids[≤100], from, to, messageTypes, lastN, dedup, output, stateFold, responseFormat, timelineBucket}` | Replay up to 100 entity IDs as one NDJSON stream, grouped by entity |

Entity replay shaping params:
- `output` = `events` (default) \| `state` (fold to current-state JSON; `state_fold` = `merge_patch` \| `last_value` \| `last_per_topic`) \| `diff` (per-event field deltas)
- `response_format` = `events` (default) \| `timeline` (bucketed by `timeline_bucket` = `MINUTE`/`HOUR`/`DAY`, auto) \| `portrait` (compact summary) — JSON only
- `dedup` = `offset` (default) \| `value` \| `none`
- `sol` = SOL query over the full sequence; `sol_output` = `events` \| `annotated` \| `summary`

### Replay to Kafka

| Method | Path | Description |
|---|---|---|
| POST | `/cassettes/topics/{topic}/replay-to-topic` | Immediate general-cassette replay into Kafka |
| POST | `/cassettes/entities/{entity_type}/{entity_id}/replay-to-topic` | Immediate entity-cassette replay into Kafka |
| POST | `/cassettes/{...}/replay` | Same, but schedulable via `start_at`/`start_delay_ms` → `202` + replay id |
| GET | `/cassettes/scheduled` | List pending/running scheduled replays |
| DELETE | `/cassettes/scheduled/{id}` | Cancel a pending scheduled replay |

`?speed=` is a real-time multiplier (1.0 = real-time). The `ReplayToTopicRequest` body:
- `targetTopic` — destination; omit (with no `topicMappings`) for **identity routing** (each record → its original topic)
- `topicMappings` — `{sourceTopic: targetTopic}` per-source overrides; absent topics fall back to `targetTopic` then to the original name
- `partitionStrategy` — `DEFAULT` (Kafka partitioner) \| `PRESERVE` (verbatim source partition; requires equal counts) \| `MODULO` (`source % target_count`)
- `from`/`to`/`partition`/`offsetFrom`/`offsetTo` — record filters (offset/partition filters apply to topic replay only)
- `transforms` — `restamp` + JSONPath `fieldSubstitutions`

Routing and partition resolution are pure functions on `ReplayEngine`
(`resolveTargetTopic`, `resolvePartition`) and the partition count is supplied
via an injected `Function<String,Integer>` lookup, so `joxette-core` stays free
of Spring/Kafka. See [`docs/replay-pipeline.puml`](docs/replay-pipeline.puml).

### SOL & Sequence Matching

| Method | Path | Description |
|---|---|---|
| POST | `/cassettes/topics/{topic}/match-sequences` | NFA-style sequence match over a topic; returns stats + example matches |
| POST | `/cassettes/entities/{entity_type}/match-sequences` | Sequence match over an entity (`entityId` param) |
| POST | `/cassettes/topics/{topic}/sol-match` | Run a SOL query over a topic's messages |
| POST | `/cassettes/entities/{entity_type}/{entity_id}/sol-match` | Run a SOL query over an entity's event sequence |

### Cassette Lifecycle Management

| Method | Path | Description |
|---|---|---|
| GET | `/cassettes/topics/{topic}/stats` | Storage stats: file count, size, inlined vs flushed |
| GET | `/cassettes/entities/{entity_type}/storage` | Storage stats per entity type by bucket |
| POST | `/cassettes/topics/{topic}/compact` | Trigger general cassette compaction |
| POST | `/cassettes/entities/{entity_type}/compact` | Trigger entity cassette compaction (optional body: `{"buckets": [12,45], "before": "2025-01-01"}`) |
| POST | `/cassettes/topics/{topic}/truncate` | Delete data before timestamp |
| POST | `/cassettes/entities/{entity_type}/truncate` | Delete entity data before timestamp |
| POST | `/cassettes/entities/{entity_type}/{entity_id}/delete` | Delete all data for specific entity (GDPR) |
| GET | `/cassettes/snapshots` | List DuckLake snapshots |
| POST | `/cassettes/snapshots/{snapshot_id}/restore` | Restore a snapshot |

### Compaction & Operations

| Method | Path | Description |
|---|---|---|
| GET | `/compaction/status` | Running/idle, last run, next scheduled, stats |
| POST | `/compaction/trigger` | Trigger compaction (optional body: `{"targets": ["orders.events", "entity:order"]}`) |
| GET | `/compaction/history` | Past compaction runs with stats |
| GET | `/health` | Liveness, consumer lag, catalog size, inlined data size |
| GET | `/metrics` | Prometheus-compatible metrics |

### Pagination

Cursor-based pagination. Cursor encodes last message's logical position, stable across physical storage changes.

```json
{
  "messages": [...],
  "cursor": "base64-encoded-position",
  "has_more": true
}
```

General cassette cursor: `(timestamp, partition, offset)`
Entity cassette cursor: `(timestamp, recorded_at, source_topic, source_partition, source_offset)`

### Replay Message Format

```json
{
  "topic": "orders.events",
  "headers": [
    {"key": "correlation-id", "value": "abc-123"},
    {"key": "event-type", "value": "OrderCreated"}
  ],
  "timestamp": "2025-01-15T10:30:00.123Z",
  "partition": 3,
  "offset": 4567,
  "key": "order-789",
  "value": {"order_id": "789", "status": "created", "amount": 99.50},
  "recorded_at": "2025-01-15T10:30:00.456Z"
}
```

Headers with binary values are base64-encoded in JSON responses.

---

## Error Handling

All REST endpoints return errors as **RFC 7807 `application/problem+json`**
bodies with a stable set of extension fields (`errorCode`, `timestamp`,
`path`, and optional `traceId`). Validation failures also carry a per-field
`errors` extension. Streaming responses (SSE, NDJSON) that fail mid-stream
emit a terminal error frame mirroring the same payload shape — an SSE
`event: error` or an NDJSON `{"_error":{…}}` line.

Domain code throws typed subclasses of
`com.joxette.api.error.JoxetteException` (e.g. `ResourceNotFoundException`,
`ConflictException`, `UpstreamUnavailableException`,
`InvalidCursorException`). `com.joxette.api.error.GlobalExceptionHandler`
renders them into ProblemDetail bodies, so controllers never catch and
translate.

For the full contract, the error code catalog, the streaming error format,
and instructions for adding a new typed exception, see
[`docs/error-handling.md`](docs/error-handling.md).

---

## Service Architecture

```
┌─────────────────────────────────────────────────┐
│                  Joxette Service                 │
│                                                  │
│  ┌──────────┐   ┌───────────┐   ┌────────────┐  │
│  │  Kafka    │──▶│  Router   │──▶│  DuckLake  │  │
│  │ Consumer  │   │  (entity  │   │  Writer    │  │
│  │ (per      │   │  extract  │   │  (inline   │  │
│  │  topic)   │   │  + route) │   │  + flush)  │  │
│  └──────────┘   └───────────┘   └─────┬──────┘  │
│                                       │         │
│  ┌──────────┐                   ┌─────▼──────┐  │
│  │  REST    │                   │  DuckDB +  │  │
│  │  API     │──────────────────▶│  DuckLake  │  │
│  │ (replay, │                   │  (catalog  │  │
│  │ manage,  │                   │   + inline │  │
│  │ compact) │                   │   storage) │  │
│  └──────────┘                   └─────┬──────┘  │
│                                       │         │
│  ┌──────────┐                         │         │
│  │  Cron    │─── compact trigger ─────┘         │
│  │ Scheduler│                                   │
│  └──────────┘                                   │
└─────────────────────────────────────────────────┘
          │                          │
          ▼                          ▼
   ┌────────────┐           ┌──────────────┐
   │  DuckDB    │           │   Object     │
   │  Catalog   │           │   Storage    │
   │  (.ducklake)│          │   (S3/GCS/   │
   │            │           │    Azure)    │
   └────────────┘           └──────────────┘
```

Single process. DuckDB embedded. Kafka consumer threads write through router into DuckLake. REST API shares same DuckDB instance. Cron triggers compaction on same instance.

### DuckDB Connection Model

One JDBC connection shared across the application. DuckDB serializes writes internally. Multiple concurrent reads via separate Statement objects. Spring manages connection lifecycle.

---

## Bootstrap Configuration

```yaml
joxette:
  catalog:
    path: "./data/joxette.ducklake"
    object-storage-path: "s3://my-bucket/joxette/data"
  
  inline:
    threshold-mb: 4
    threshold-records: 50000
  
  compaction:
    schedule: "0 3 * * *"
    entity:
      min-files-per-bucket: 10
      target-file-size-mb: 256
      lookback-days: 30
    general:
      enabled: false
  
  kafka:
    bootstrap-servers: "localhost:9092"
  
  recording:
    batch-size: 10000
    batch-timeout-ms: 1000

  bootstrap:
    topics:
      - topic: "orders.events"
        mode: "both"
      - topic: "audit.log"
        mode: "general"
    
    entities:
      - type: "order"
        buckets: 256
        sources:
          - topic: "orders.events"
            entity-id:
              source: value
              expression: "$.order_id"
          - topic: "payments.events"
            entity-id:
              source: value
              expression: "$.payment.order_id"
```

After first start, the REST API is the source of truth for configuration. Bootstrap config is only loaded if the config tables are empty.

---

## Project Structure

```
joxette/
├── pom.xml
├── CLAUDE.md
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/joxette/
│   │   │       ├── JoxetteApplication.java
│   │   │       ├── config/
│   │   │       │   ├── JoxetteProperties.java
│   │   │       │   ├── DuckDBConfig.java
│   │   │       │   ├── KafkaConfig.java
│   │   │       │   └── SchedulingConfig.java
│   │   │       ├── catalog/
│   │   │       │   ├── CatalogManager.java
│   │   │       │   ├── ConfigRepository.java
│   │   │       │   ├── TopicConfig.java
│   │   │       │   ├── EntityTypeConfig.java
│   │   │       │   ├── EntitySourceMapping.java
│   │   │       │   └── KnownEntitiesRepository.java
│   │   │       ├── recording/
│   │   │       │   ├── RecordingCoordinator.java
│   │   │       │   ├── TopicRecorder.java
│   │   │       │   ├── MessageRouter.java
│   │   │       │   ├── EntityIdExtractor.java
│   │   │       │   ├── CassetteBatchWriter.java
│   │   │       │   └── DeduplicationStrategy.java
│   │   │       ├── replay/
│   │   │       │   ├── TopicReplayService.java
│   │   │       │   ├── EntityReplayService.java
│   │   │       │   ├── ReplayCursor.java
│   │   │       │   └── HeadersHelper.java
│   │   │       ├── compaction/
│   │   │       │   ├── CompactionService.java
│   │   │       │   ├── CompactionScheduler.java
│   │   │       │   ├── CompactionStrategy.java
│   │   │       │   └── CompactionHistory.java
│   │   │       ├── api/
│   │   │       │   ├── TopicController.java
│   │   │       │   ├── EntityController.java
│   │   │       │   ├── CassetteController.java
│   │   │       │   ├── CompactionController.java
│   │   │       │   ├── HealthController.java
│   │   │       │   ├── dto/
│   │   │       │   │   ├── MessageResponse.java
│   │   │       │   │   ├── PagedResponse.java
│   │   │       │   │   ├── TopicConfigRequest.java
│   │   │       │   │   ├── EntityTypeRequest.java
│   │   │       │   │   ├── CompactionStatusResponse.java
│   │   │       │   │   └── ...
│   │   │       │   └── sse/
│   │   │       │       └── SseReplayHandler.java
│   │   │       └── storage/
│   │   │           ├── DuckLakeManager.java
│   │   │           └── SchemaManager.java
│   │   └── resources/
│   │       ├── application.yml
│   │       └── db/
│   │           └── init.sql
│   └── test/
│       └── java/
│           └── com/joxette/
│               ├── JoxetteApplicationTests.java
│               ├── recording/
│               │   ├── TopicRecorderTest.java
│               │   ├── MessageRouterTest.java
│               │   └── EntityIdExtractorTest.java
│               ├── replay/
│               │   ├── TopicReplayServiceTest.java
│               │   └── EntityReplayServiceTest.java
│               ├── compaction/
│               │   └── CompactionServiceTest.java
│               ├── api/
│               │   └── ...integration tests...
│               └── testutil/
│                   ├── KafkaTestSupport.java
│                   └── DuckDBTestSupport.java
```

---

## Implementation Order

1. **Project skeleton** — pom.xml, JoxetteApplication.java, config binding
2. **DuckDB + DuckLake setup** — connection management, schema creation, VARIANT confirmed through DuckLake (see `SchemaManager.probeVariant()`)
3. **Config repository** — CRUD for topic/entity configs in plain DuckDB tables
4. **Headers helper** — multimap utility functions registered as DuckDB macros
5. **Recording pipeline** — Jox Kafka source → batching → DuckLake writer, single topic, general cassette only
6. **Entity routing** — expression evaluation, entity cassette writes, known entities registry
7. **Replay API** — paginated + SSE/NDJSON for both general and entity cassettes
8. **Management API** — topic/entity CRUD endpoints, dynamic start/stop of recorders
9. **Compaction** — background job, cron scheduling, REST trigger
10. **Tests** — testcontainers for Kafka + DuckDB integration tests throughout

---

## Important Design Notes

### Unified Read Path
DuckLake transparently reads from both inlined data (in catalog DB) and Parquet files (on object storage) in a single query. No application-level UNION needed. Replay queries "just work" across all storage tiers.

### Deduplication
At-least-once from Kafka means possible duplicates. Deduplicate on read, not write. `(topic, partition, offset)` is the unique key. Use `QUALIFY` or `DISTINCT ON` in replay queries.

### Entity Ordering Across Topics
Order by `timestamp` (Kafka producer timestamp) primary, `recorded_at` (service ingestion time) tiebreaker. Cross-topic ordering is best-effort due to possible clock skew.

### Config Lifecycle
YAML bootstrap config is loaded only on first start (when config tables are empty). After that, the REST API is the source of truth. Config lives in plain DuckDB tables, not DuckLake.

### Catalog Migration Path
Three-stage scaling path — see [`docs/catalog-scaling.md`](docs/catalog-scaling.md) for the full runbook.

| Stage | Catalog backend | When to use |
|---|---|---|
| 1 (current) | Embedded DuckDB | Single Joxette process; zero ops overhead |
| 2 | Quack server (DuckDB 1.5.3+, **beta**) | Multi-process needed; want DuckDB semantics without PostgreSQL; evaluate at DuckDB 2.0 GA |
| 3 | PostgreSQL | Quack throughput or HA not sufficient |

DuckLake catalog schema is **identical across all three backends** — only the JDBC connection string and DuckLake `catalog_connection` property change. No data migration, no schema rewrite.

### Storage Delegation
All object storage interaction (S3, GCS, Azure) is delegated to DuckDB/DuckLake via the httpfs extension and DuckLake's built-in storage management. No direct S3 SDK usage from Java.
