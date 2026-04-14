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
- [x] `startFrom: earliest` / `startFrom: <ISO timestamp>` — `TopicRecorder` has `seekToEarliest` + `seekToTimestamp` fields; `RecordingStartupRunner` passes `tc.startFrom()` through; `TopicController.createTopic()` defaults to `"latest"` when unset

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
- [x] `TopicController` calls `MessageRouter.reload()` after every mutating operation (create, update, delete, add/delete matcher) via `reloadRouter()`
- [x] `GET/POST/DELETE /topics/{topic}/matchers` — message-type matcher CRUD for general cassettes

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

### Retention
- [x] `RetentionService` — deletes rows older than `retention_days` from `lake.main.general_{topic}`, `lake.main.entity_{type}`, and `known_entities`
- [x] `RetentionScheduler` — cron-driven scheduler (configurable via `joxette.retention.schedule`)
- [x] `retention_history` table tracks all retention runs with status, row counts, error messages
- [x] `RetentionRun` / `RetentionStatus` records for API responses
- [x] Topics or entity types with `retention_days IS NULL` are silently skipped
- [x] `PUT /topics/{topic}/retention` — set or clear retention days

### Replay-to-Topic
- [x] `KafkaProducerService` — wraps `KafkaSink`/`KafkaProducer` for sending records back to Kafka
- [x] `ReplayToTopicService` — general cassette replay + entity cassette replay (merge-sorted by `kafka_timestamp`)
- [x] Speed multiplier — inter-message delays scaled by `speedMultiplier` (x0.5 slow-down, x1 real-time, x2+ fast-forward)
- [x] Progress reporting — `ReplayProgress` events emitted every 100 records, plus final `completed`/`failed`
- [x] `ReplayToTopicRequest` — request DTO with `targetTopic`, time/offset filters, speed, transforms
- [x] `ScheduledReplayService` — in-memory registry for pending/streaming scheduled replays (SSE-driven, cancel support)
- [x] `ScheduledReplay` / `ScheduledReplayResponse` records

### Message Transformation Pipeline
- [x] `MessageTransformer` — stateful per-replay transformer (restamp + field substitutions via JSONPath)
- [x] Restamp — shifts all `kafka_timestamp` values so first message = now, preserving relative gaps
- [x] Field substitution — replaces JSON field values at JSONPath locations with literal strings or new UUID4s
- [x] `FieldSubstitution` record (path + optional literal value)
- [x] `ReplayTransformConfig` record (`restamp` flag + `fieldSubstitutions` list); `isIdentity()` short-circuits no-op
- [x] `TransformPresetsController` — CRUD for saved transform pipeline presets
- [x] `ConfigController` / `RuntimeConfigResponse` — runtime config introspection endpoint
- [x] `replay/transform/` package — transform step definitions
- [x] UI: `TransformPipelineBuilder`, `NestedPipelineBuilder`, `StepCard`, `StepConfigForm`, `StepPicker`, `PredicateBuilder`, `PresetManager`, `PipelineDryRun` components

### UI
- [x] Topics list → topic detail with replay records, stats, filter, pagination
- [x] Entities list (with Rebuild Known Entities button) → entity type detail (sources, known entities) → entity replay
- [x] Compaction page (status, trigger, history)
- [x] Snapshots page (list, create local, export to object store, restore)
- [x] Health page
- [x] Settings page (`ui/src/routes/settings/index.tsx`, `appStore.ts`)
- [x] About page
- [x] Dark/light theme toggle
- [x] Toast notifications for all mutations
- [x] Confirm dialogs for destructive actions
- [x] `ReplayToTopicPanel` — UI panel for configuring and triggering replay-to-topic
- [x] Timeline views — `CassetteTimeline.tsx` (canvas-based, pan/zoom/keyboard nav, proportional timestamp spacing, progressive page loading); exposed at `$topic_.timeline.tsx` and `$entityType/$entityId_.timeline.tsx`
- [x] `TruncateDialog` component
- [x] `useDebounce` hook

### Schema
- [x] `SchemaManager` creates all tables at startup (idempotent)
- [x] VARIANT/JSON probe for flexible column type
- [x] Config tables in plain DuckDB `main` schema
- [x] DuckLake tables in `lake.main` schema
- [x] `compaction_history` migration path for columns added after initial schema
- [x] `retention_history` table

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
- [ ] `entity_source_matchers.id_source` check constraint uses `'key', 'value', 'headers'` (plural) but `MessageRouter` / `EntityIdExtractor` use `'key', 'value', 'header'` (singular) — potential mismatch to verify and fix

### Testing gaps
- [x] Integration test for `CassetteBatchWriter` headers struct-array write — covered by `TopicRecorderTest.recorder_handlesHeadersAndNullKey`
- [ ] Integration test for `exportSnapshotToObjectStore`
- [ ] Integration test for `rebuildKnownEntities` — scenario: write entity events to object store, wipe `known_entities`, call rebuild, verify all entities are restored with correct `first_seen`/`last_seen`
- [ ] `TopicRecorderTest` does not test the full batch → DuckDB write path

### UI gaps
- [ ] SSE/NDJSON streaming not exposed in UI (only paginated JSON used)
- [ ] No progress indicator for long-running operations (compaction, rebuild)
- [ ] Entity detail page sources table: `idSource`/`idExpression` fields from API don't match `AddSourceRequest` (UI sends `idSource`/`idExpression` but backend `EntitySourceConfig.MatcherConfig` uses `idSource`/`idExpression` — verify mapping end-to-end)

## Evolution of Key Decisions

| Decision | Original | Current | Reason |
|---|---|---|---|
| MessageRouter config source | YAML bootstrap only | ConfigRepository (DuckDB) | REST-registered entities were invisible to the router |
| Headers storage | VARCHAR JSON string | `STRUCT(key VARCHAR, value VARCHAR)[]` literal | Type mismatch caused all batch INSERTs to fail; later simplified from BLOB to VARCHAR |
| known_entities location | DuckLake | Plain DuckDB | `ON CONFLICT DO UPDATE` requires PK enforcement |
| Compaction sort column | `timestamp` | `kafka_timestamp` | Wrong column name caused compaction SQL errors |
| Snapshot storage | Local disk only | Local disk + S3 export | Disaster recovery — local catalog file is a SPOF |
| TopicController router reload | Not implemented | `reloadRouter()` called on all mutations | Topic mode/matcher changes were not picked up by router |
| startFrom: earliest | Hardcoded `latest` | Fully wired: DB → `TopicRecorder.seekToEarliest` / `seekToTimestamp` | RecordingStartupRunner passes `tc.startFrom()` through |
| Retention enforcement | Config stored, no cron | `RetentionService` + `RetentionScheduler` + `retention_history` | Retention days are now enforced automatically |
| Replay-to-Kafka | Not started | `ReplayToTopicService` + `KafkaProducerService` + speed multiplier + transforms | Core replay use-case for integration testing |
