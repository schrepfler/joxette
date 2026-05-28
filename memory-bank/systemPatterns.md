# System Patterns

## Architecture Overview

```
Kafka Topics
    │
    ▼
TopicRecorder (one per topic, Jox structured concurrency)
    │  Jox Flow: poll → groupedWithin(batch) → route → write → commit
    │
    ├──► CassetteBatchWriter         → lake.main.general_{topic}   (DuckLake / Parquet)
    └──► EntityCassetteBatchWriter   → lake.main.entity_{type}     (DuckLake / Parquet)
              │
              └──► KnownEntitiesRepository → known_entities        (plain DuckDB)

REST API (Spring Boot)
    ├── TopicController        /topics/**
    ├── EntityController       /entities/**
    ├── CassetteController     /cassettes/**
    ├── CompactionController   /compaction/**
    └── HealthController       /health

DuckDB (embedded, single connection)
    ├── main schema            plain tables: topic_configs, entity_type_configs,
    │                          entity_source_mappings, entity_source_matchers,
    │                          known_entities, compaction_history, snapshots
    └── lake catalog (DuckLake)
        └── main schema        DuckLake tables: general_{topic}, entity_{type}
                               Parquet on S3; inline buffer in .ducklake file
```

## Key Design Patterns

### 1. DuckDB Connection Model
- One JDBC Connection shared across the whole application (injected as a Spring bean)
- Each `CassetteBatchWriter` / `EntityCassetteBatchWriter` calls `duckConn.duplicate()` to get its own virtual connection — DuckDB serialises writes internally
- All `synchronized(duckDB)` blocks protect multi-statement sequences from interleaving

### 2. MessageRouter — DB-backed, atomically reloadable
- `MessageRouter` holds a `volatile RoutingTables` record (topic modes + entity buckets + source entries)
- Loaded from `ConfigRepository` at startup via `reload()`
- `EntityController` calls `reload()` after every mutating operation (create/update/delete entity type or source)
- Thread-safe: snapshot swap is atomic (volatile field + immutable record)

### 3. Recording Pipeline (Jox)
```
KafkaConsumer.poll() → emit → groupedWithin(batchSize, batchTimeout)
    → writeBatch(generalWriter, entityWriter)
    → pendingCommit.set(offsets)    // committed next poll loop
```
- At-least-once semantics; deduplication on read via `QUALIFY ROW_NUMBER() OVER (PARTITION BY partition, offset ORDER BY recorded_at DESC) = 1`
- Batch commit is deferred one poll cycle to avoid committing before the DuckDB write is confirmed

### 4. Headers Storage Pattern
- Headers are `STRUCT(key VARCHAR, value VARCHAR)[]` in DuckLake — **both key and value are plain strings**
- Header values are decoded as UTF-8 on write; non-UTF-8 binary values are base64-encoded so no data is lost (`CassetteBatchWriter.decodeHeaderValue()`)
- `CassetteBatchWriter` builds `[{'key': '...', 'value': '...'}]` string literals inlined in the SQL (cannot use JDBC `setObject` for struct-array columns — type binding doesn't work through the JDBC driver)
- `EntityCassetteBatchWriter` delegates to `CassetteBatchWriter.headersToStructLiteral(message.headers())` — same approach, actual headers stored (was hardcoded `[]`, now fixed)
- On read, `TopicReplayService.mapHeaders()` casts `struct.get("value")` directly to `String` — no BLOB unwrapping or base64 encoding needed
- The `headers_get(headers, 'key')` DuckDB macro returns `VARCHAR` directly — fully queryable without casts
- Rationale: 99%+ of Kafka header values in practice are UTF-8 strings; BLOB made them unqueryable and unreadable in the UI

### 5. Cursor-based Pagination
- General cassette cursor: `(timestamp, partition, offset)` — base64-encoded JSON
- Entity cassette cursor: `(timestamp, recorded_at, source_topic, source_partition, source_offset)`
- Known-entities cursor: plain base64-encoded `entity_id`
- All use jOOQ `seekAfter(...)` for keyset pagination — stable across physical file changes

### 6. DuckLake Table Naming
- General cassette: `lake.main.general_{normalized_topic}` — topic normalized to `[a-z0-9_]`
- Entity cassette: `lake.main.entity_{type}` — type must match `[a-z][a-z0-9_]*`
- Tables created at startup by `SchemaManager` from bootstrap config; also created dynamically via `EntityController.createEntityType()`

### 7. Config Lifecycle
- Bootstrap YAML → seeded into DuckDB config tables on first start (idempotent, `ON CONFLICT DO NOTHING`)
- REST API is source of truth after first start
- `MessageRouter` must be reloaded after any REST API config change to pick up new routing rules

### 8. Replay Source & Sink SPIs
- `ReplayEngine` lives in `joxette-core` and is Spring-free. It depends on three core SPIs — `CassetteSource`, `EntityCassetteSource`, `RecordSink` — and nothing else, so the same engine can run inside the service or from a standalone test-kit.
- `CassetteSource.streamAll(topic, from, to, partition, offsetFrom, offsetTo, sink)` and `EntityCassetteSource.streamEntityEvents(entityType, entityId, from, to, sink)` are callback-style: callers push records into a `Consumer<CassetteRecord>` / `Consumer<EntityRecord>`. In the service, `TopicReplayService` / `EntityReplayService` implement them backed by jOOQ/DuckLake; in the test-kit, `InMemoryCassetteSource` / `InMemoryEntityCassetteSource` implement them against pre-sorted lists.
- `RecordSink.send(...)` is **blocking**. On virtual threads this is cheap; we deliberately avoid `Future`/`CompletableFuture` in the SPI. Permanent failures throw `SinkException` so the engine can flip the run to `status=failed`.
- `KafkaRecordSink` (in `joxette-kafka`) wraps a `Producer` and owns byte/header/timestamp encoding but does not own the `Producer`. `KafkaRecordSinkFactory` (in `joxette-service`) is the only place that knows about multi-broker routing: it caches one `KafkaProducer` per broker id and closes all on `@PreDestroy`.
- `CassetteController` builds a per-request engine via `engineFor(brokerId)` — the engine itself is single-destination and broker-agnostic.

### 9. Module Boundaries
- `joxette-core` — pure Java, no Spring / DuckDB / jOOQ / Kafka. Holds DTOs (`CassetteRecord`, `EntityRecord`, `ReplayProgress`, `ReplayToTopicRequest`, `ReplayTransformConfig`, `FieldSubstitution`, `PagedResponse`), the `MessageTransformer` (restamp + JSONPath substitution), the three SPIs (`CassetteSource`, `EntityCassetteSource`, `sink.RecordSink` + `SinkException`), and `ReplayEngine`.
- `joxette-kafka` — depends on core + `kafka-clients` only. Holds `KafkaRecordSink` so test-kit consumers that want a real Kafka sink don't need Spring.
- `joxette-service` — Spring Boot app. Depends on core + kafka modules. Implements the SPIs against DuckLake/jOOQ; owns the `KafkaRecordSinkFactory` producer cache and the Spring controllers. `TransformPipeline` (which uses `SqlPushdownAnalyzer` and is therefore jOOQ-bound) stays here; only the simpler `MessageTransformer` was moved to core.
- `joxette-test-kit` — depends on core + kafka modules. No DuckDB, no Spring. Ships `InMemoryCassetteSource`, `InMemoryEntityCassetteSource`, `CapturingRecordSink`, and a fluent `ReplayEngineBuilder` so external consumers can drive the engine in-process.

### 9. Disaster Recovery
- **Export to object store**: `EXPORT DATABASE 's3://.../snapshots/<name>/'` — writes entire catalog (config tables, known_entities, DuckLake metadata) to S3 directly via httpfs. No local disk used.
- **Rebuild known_entities**: scans `lake.main.entity_*` tables, computes `(entity_type, entity_id, MIN(recorded_at), MAX(recorded_at))` per entity, upserts into `known_entities`. Used after catalog file loss when Parquet data is still on S3.

## Compaction
- Entity compaction: copy cold rows (older than `lookback-days`) per bucket to temp table sorted by `(entity_id, kafka_timestamp, recorded_at)`, delete originals, re-insert → DuckLake writes consolidated Parquet files
- General cassette compaction: same strategy per `(topic, kafka_partition)` slice, sorted by `(kafka_timestamp, kafka_partition, kafka_offset)`
- Guarded by `AtomicBoolean running` to prevent overlapping runs
- `ducklake_flush_inlined_data('lake')` called after compaction to push inline buffer to S3; then `CHECKPOINT` to persist catalog

### 10. Catalog Backend Detection
- `CatalogBackend` enum (`EMBEDDED_DUCKDB`, `QUACK`, `POSTGRES`) derived from `catalog.path` URI prefix at startup in `DuckLakeManager`
- `CatalogHealthIndicator` exposes the active backend type and path via Spring Boot Actuator `/actuator/health`
- DuckLake `ATTACH` statement template is identical across all three backends — only the connection string differs
- See `docs/catalog-scaling.md` for the three-stage migration runbook

### 11. Instance Roles (`joxette.roles`)
- `InstanceRoles` record holds `recorder`, `replay`, `compaction` booleans
- `@ConditionalOnRole("recorder")` / `@ConditionalOnRole("replay")` / `@ConditionalOnRole("compaction")` — custom Spring `@Conditional` skips bean registration when the role is disabled
- Applied to: `RecordingStartupRunner`, `MessageRouter`, `CompactionScheduler`, `RetentionScheduler`, `CompactionController`, `CassetteController`
- Enables dedicated recorder vs. replay vs. compaction node topology

### 12. Cluster Instance Registry
- `InstanceRegistry` writes a row to `joxette_instances` on startup (instance_id UUID, host, roles JSON, started_at)
- Sends a heartbeat every 30 s (configurable) via a scheduled virtual thread
- `reapStaleInstances()` removes rows whose `last_heartbeat` is older than `staleness-threshold` (default 2 min)
- `GET /instances` — returns all non-stale rows with `status=alive`
- `GET /health` — includes `cluster.instanceCount` and `cluster.aliveCount` from the registry

### 13. Distributed Compaction Lock
- `CompactionLockManager` uses the `compaction_locks` DuckDB table as a mutex: `INSERT … ON CONFLICT DO NOTHING` + row count check
- Lock scoped to `(instance_id, target)` with `expires_at` TTL
- `cleanExpiredLocks()` runs at the start of each compaction trigger to remove stale locks
- `releaseOwnLocks()` called at startup to clean up locks from a previous crash
- Other instances skip (not fail) when the lock is held; they log at DEBUG and move on

### 14. BackgroundTaskRegistry — unified ad-hoc VT lifecycle

**File:** `com.joxette.lifecycle.BackgroundTaskRegistry` (`SmartLifecycle`, phase `Integer.MAX_VALUE - 2048`)

Provides a single `submit(name, task)` entry-point for ad-hoc virtual threads that need ordered shutdown:

- `submit(name, task)` wraps the runnable in `try/finally { tasks.remove(id) }` so normally-completing tasks self-clean; each VT is stored in a `ConcurrentHashMap<UUID, TaskHandle>`.
- `stop()` sets `accepting=false` (closes the submission window), interrupts all tracked VTs, then joins them against a **single shared 30 s deadline** (not per-task).
- `getRunningTasks()` returns a snapshot — used by `GET /health` to expose a `backgroundTasks` summary (`running`, `names`).
- Phase `MAX_VALUE - 2048` fires between `SseReplayHandler` (`MAX_VALUE - 1024`) and Pekko `@PreDestroy`, so exported jobs, live-metrics SSE streams, and manual retention VTs are always interrupted before the actor system shuts down.

Migrated sites:
- `ExportService` — export job VTs
- `CompactionController` — manual retention VTs
- `InstanceController` — live-metrics SSE VTs
- `SseReplayHandler` keeps its own tracking (pre-existing pattern, not merged).

### 15. Pull-based eviction in ActiveReplayTracker

`ActiveReplayTracker` used to spawn a cleanup VT (`Thread.ofVirtual().name("replay-tracker-cleanup-" + id)`) to remove completed entries after a 30 s sleep. This thread is now eliminated entirely:

- `Entry` gained a `volatile Instant completedAt` field.
- `Handle.close()` sets `entry.completedAt = Instant.now()` — no thread spawn.
- `listActive()` uses an `Iterator`-based inline scan: entries where `completedAt != null && completedAt.isBefore(Instant.now().minusSeconds(30))` are removed via `it.remove()`.

Same 30 s visibility window; zero background threads.

### 16. KIP-848 Cooperative Rebalance
- `TopicRecorder` handles `onPartitionsRevoked` by draining only the revoked partitions through the write channel before incrementing the epoch
- Non-revoked partitions continue recording without pause — critical for Kafka 4.x where the broker can assign/revoke individual partitions independently
- `DuckLakeWriteChannel` extracted as an explicit type to carry partition metadata alongside write batches
- `WriteBatch` carries a `revocationEpoch` so the drain loop can discard stale batches after epoch change

## Data Durability Split
| Storage | Location | Lost if .ducklake deleted? |
|---|---|---|
| General cassette rows | S3 Parquet | No (files remain) |
| Entity cassette rows | S3 Parquet | No (files remain) |
| known_entities | plain DuckDB in .ducklake | **Yes** |
| DuckLake catalog/manifest | .ducklake | **Yes** (orphans S3 files) |
| Config tables | plain DuckDB in .ducklake | **Yes** |
