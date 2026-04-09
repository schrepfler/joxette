# Progress

## What Works

### Recording Pipeline
- [x] Kafka consumer (Jox KafkaSource) per topic, managed by `RecordingCoordinator`
- [x] Batch accumulation via `groupedWithin(batchSize, batchTimeout)`
- [x] General cassette writes to `lake.main.general_{topic}` via `CassetteBatchWriter`
- [x] Entity routing via `MessageRouter` (DB-backed, reloadable)
- [x] Entity cassette writes to `lake.main.entity_{type}` via `EntityCassetteBatchWriter`
- [x] `known_entities` registry upserted per batch via `KnownEntitiesRepository`
- [x] Kafka offset commit after successful DuckDB write
- [x] Headers stored as `STRUCT(key VARCHAR, value VARCHAR)[]` in general cassettes — UTF-8 decoded on write, base64 fallback for binary values
- [x] Headers stored as `STRUCT(key VARCHAR, value VARCHAR)[]` in entity cassettes — was hardcoded `[]`, now stores real headers

### Replay API
- [x] `GET /cassettes/topics/{topic}` — paginated, SSE, NDJSON
- [x] `GET /cassettes/entities/{type}` — list known entities (cursor-paginated)
- [x] `GET /cassettes/entities/{type}/search` — substring search on entity ID
- [x] `GET /cassettes/entities/{type}/{id}` — entity event replay (paginated, SSE, NDJSON)
- [x] `GET /cassettes/entities/{type}/{id}/stats` — aggregate stats
- [x] Deduplication via `QUALIFY ROW_NUMBER() OVER (PARTITION BY partition, offset ORDER BY recorded_at DESC) = 1`
- [x] Cursor-based pagination (keyset, stable across storage changes)

### Management API
- [x] `GET/POST/PUT/DELETE /topics/**` — topic config CRUD
- [x] `POST /topics/{topic}/pause` + `/resume`
- [x] `GET/POST/PUT/DELETE /entities/**` — entity type + source CRUD
- [x] `EntityController` calls `MessageRouter.reload()` after every mutation

### Lifecycle / Operations
- [x] `GET /cassettes/topics/{topic}/stats` — row count + estimated size
- [x] `POST /cassettes/topics/{topic}/compact` — DuckLake flush + CHECKPOINT
- [x] `POST /cassettes/topics/{topic}/truncate`
- [x] `DELETE /cassettes/entities/{type}/{id}` — GDPR erasure
- [x] `GET/POST /cassettes/snapshots` — local EXPORT DATABASE snapshots
- [x] `POST /cassettes/snapshots/{name}/restore` — IMPORT DATABASE
- [x] `POST /cassettes/snapshots/export-to-object-store` — EXPORT DATABASE to S3
- [x] `POST /cassettes/entities/rebuild-known-entities` — rebuild registry from Parquet

### Compaction
- [x] `CompactionService` — entity bucket compaction + general cassette compaction
- [x] Cron scheduler (`CompactionScheduler`) — configurable cron, default 03:00 daily
- [x] `POST /compaction/trigger` — manual trigger, async, 202 response
- [x] `GET /compaction/status` + `GET /compaction/history`
- [x] `compaction_history` table tracks all runs with status, counts, error messages

### UI
- [x] Topics list → topic detail with replay records, stats, filter, pagination
- [x] Entities list (with Rebuild Known Entities button) → entity type detail (sources, known entities) → entity replay
- [x] Compaction page (status, trigger, history)
- [x] Snapshots page (list, create local, export to object store, restore)
- [x] Health page
- [x] Dark/light theme toggle
- [x] Toast notifications for all mutations
- [x] Confirm dialogs for destructive actions

### Schema
- [x] `SchemaManager` creates all tables at startup (idempotent)
- [x] VARIANT/JSON probe for flexible column type
- [x] Config tables in plain DuckDB `main` schema
- [x] DuckLake tables in `lake.main` schema
- [x] `compaction_history` migration path for columns added after initial schema

### Tests
- [x] `MessageRouterTest` — full routing logic, DB-backed stub, reload test
- [x] `TopicReplayServiceTest`
- [x] `EntityReplayServiceTest`
- [x] `EntityIdExtractorTest`
- [x] `HeadersHelperTest`
- [x] `CompactionServiceTest`
- [x] `TopicRecorderTest`
- [x] `RecordReplayRoundTripIT` — integration test (Testcontainers)
- [x] `SpringDocIT` — verifies OpenAPI spec loads

## What's Left / Known Gaps

### Functional gaps
- [ ] `startFrom: earliest` not fully implemented — `RecordingStartupRunner` hardcodes `latest`
- [ ] Retention policy enforcement — `retention_days` stored in config but no cron job to delete old data
- [ ] `TopicController` does not trigger `MessageRouter.reload()` when topic modes change
- [ ] `entity_source_matchers.id_source` check constraint uses `'key', 'value', 'headers'` but `MessageRouter` / `EntityIdExtractor` use `'key', 'value', 'header'` (singular) — potential mismatch to verify

### Testing gaps
- [x] Integration test for `CassetteBatchWriter` headers struct-array write — covered by `TopicRecorderTest.recorder_handlesHeadersAndNullKey`
- [ ] Integration test for `exportSnapshotToObjectStore`
- [ ] Integration test for `rebuildKnownEntities` — scenario: write entity events to object store, wipe `known_entities`, call rebuild, verify all entities are restored with correct `first_seen`/`last_seen`
- [ ] `TopicRecorderTest` does not test the full batch → DuckDB write path

### Replay-to-Kafka features (not yet started)
- [ ] **Replay-to-topic** — produce recorded messages back onto a Kafka topic, preserving partitioning key logic and maintaining linearization order (`kafka_timestamp` ordering, cross-topic merge for entity cassettes)
- [ ] **Speed multiplier** — replay at x1.5, x2, etc. by proportionally compressing inter-message delays (wall-clock time between messages scaled by the multiplier)
- [ ] **Message transformation** — before producing, allow optional transforms: (a) restamp timestamps to wall clock time (shift all `kafka_timestamp` values so the first message = now), (b) replace field values via JSONPath expressions (e.g. replace IDs in message bodies with new generated IDs, useful for re-running scenarios against a fresh environment)

### UI gaps
- [ ] SSE/NDJSON streaming not exposed in UI (only paginated JSON used)
- [ ] No progress indicator for long-running operations (compaction, rebuild)
- [ ] Entity detail page sources table: `idSource`/`idExpression` fields from API don't match `AddSourceRequest` (UI sends `idSource`/`idExpression` but backend `EntitySourceConfig.MatcherConfig` uses `idSource`/`idExpression` — verify mapping end-to-end)

## Evolution of Key Decisions

| Decision | Original | Current | Reason |
|---|---|---|---|
| MessageRouter config source | YAML bootstrap only | ConfigRepository (DuckDB) | REST-registered entities were invisible to the router |
| Headers storage | VARCHAR JSON string | `STRUCT(key VARCHAR, value BLOB)[]` literal | Type mismatch caused all batch INSERTs to fail |
| known_entities location | DuckLake | Plain DuckDB | `ON CONFLICT DO UPDATE` requires PK enforcement |
| Compaction sort column | `timestamp` | `kafka_timestamp` | Wrong column name caused compaction SQL errors |
| Snapshot storage | Local disk only | Local disk + S3 export | Disaster recovery — local catalog file is a SPOF |
