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

### 8. Replay Sink SPI
- `ReplayEngine` is Spring-free. It depends on `TopicReplayService`, `EntityReplayService`, and a `RecordSink` — nothing else — so the same engine can run inside the service or from a standalone test-kit.
- `RecordSink.send(...)` is **blocking**. On virtual threads this is cheap; we deliberately avoid `Future`/`CompletableFuture` in the SPI. Permanent failures throw `SinkException` so the engine can flip the run to `status=failed`.
- `KafkaRecordSinkFactory` is the only place that knows about multi-broker routing. It caches one `KafkaProducer` per broker id and hands the sink a `Producer` it does **not** own (factory closes it on `@PreDestroy`).
- `CassetteController` builds a per-request engine via `engineFor(brokerId)` — the engine itself is single-destination and broker-agnostic.

### 9. Disaster Recovery
- **Export to object store**: `EXPORT DATABASE 's3://.../snapshots/<name>/'` — writes entire catalog (config tables, known_entities, DuckLake metadata) to S3 directly via httpfs. No local disk used.
- **Rebuild known_entities**: scans `lake.main.entity_*` tables, computes `(entity_type, entity_id, MIN(recorded_at), MAX(recorded_at))` per entity, upserts into `known_entities`. Used after catalog file loss when Parquet data is still on S3.

## Compaction
- Entity compaction: copy cold rows (older than `lookback-days`) per bucket to temp table sorted by `(entity_id, kafka_timestamp, recorded_at)`, delete originals, re-insert → DuckLake writes consolidated Parquet files
- General cassette compaction: same strategy per `(topic, kafka_partition)` slice, sorted by `(kafka_timestamp, kafka_partition, kafka_offset)`
- Guarded by `AtomicBoolean running` to prevent overlapping runs
- `ducklake_flush_inlined_data('lake')` called after compaction to push inline buffer to S3; then `CHECKPOINT` to persist catalog

## Data Durability Split
| Storage | Location | Lost if .ducklake deleted? |
|---|---|---|
| General cassette rows | S3 Parquet | No (files remain) |
| Entity cassette rows | S3 Parquet | No (files remain) |
| known_entities | plain DuckDB in .ducklake | **Yes** |
| DuckLake catalog/manifest | .ducklake | **Yes** (orphans S3 files) |
| Config tables | plain DuckDB in .ducklake | **Yes** |
