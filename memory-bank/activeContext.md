# Active Context

## Current Work Focus
Memory-bank sync. All previously-listed "not yet started" features (retention, replay-to-topic, speed multiplier, message transformation pipeline, scheduled replay, timeline UI) are confirmed implemented in source. `progress.md` has been updated to reflect actual state.

## What Was Already Done (previously undocumented)

### Retention enforcement
`RetentionService` + `RetentionScheduler` + `RetentionRun` / `RetentionStatus` — fully implemented. Deletes rows older than `retention_days` from general cassettes, entity cassettes, and `known_entities`. Cron-driven, guarded by `AtomicBoolean`, logged to `retention_history`.

### Replay-to-Topic
`ReplayToTopicService` orchestrates reading from DuckLake and producing to Kafka via `KafkaProducerService` / `KafkaSink`. Both general cassette and entity cassette paths are implemented. Speed multiplier (inter-message delay scaling) and `ReplayProgress` progress events are included.

### Scheduled Replay
`ScheduledReplayService` — in-memory registry for pending/streaming scheduled replays. Supports register, await-start (cancellable latch), cancel, and status queries. Not persisted across restarts.

### Message Transformation Pipeline
`MessageTransformer` — per-replay-invocation stateful transformer. Supports restamp (shift all `kafka_timestamp` values so first message = now) and field substitution (JSONPath-based replacement with literal or UUID4). `ReplayTransformConfig` + `FieldSubstitution` records. `TransformPresetsController` for saved presets. Full UI pipeline builder.

### TopicController reload
`TopicController` already calls `reloadRouter()` (via `MessageRouter.reload()`) on every mutating operation: create, update, delete topic, add/delete matcher.

### startFrom wiring
`RecordingStartupRunner` passes `tc.startFrom()` through to `coordinator.startTopic()`. `TopicRecorder` has `seekToEarliest` and `seekToTimestamp` fields. `TopicController.createTopic()` defaults to `"latest"` but accepts `"earliest"` or an ISO timestamp.

### Timeline UI
`CassetteTimeline.tsx` — canvas-based two-panel viewer (JSON inspector + horizontal timeline). Features: proportional timestamp spacing, click-to-select, keyboard nav (←/→), pan (drag), zoom (scroll/pinch), fit-to-window, colour coding by partition/topic, progressive page loading. Exposed at `topics/$topic_.timeline.tsx` and `entities/$entityType/$entityId_.timeline.tsx`.

### Settings page
`ui/src/routes/settings/index.tsx` + `appStore.ts` (Zustand store for UI settings).

## Active Decisions & Considerations

### known_entities remains plain DuckDB (not DuckLake)
Intentional: `ON CONFLICT ... DO UPDATE` works correctly in plain DuckDB but not in DuckLake (no PK enforcement). Moving it to DuckLake would require deduplication-on-read which adds complexity. The rebuild operation compensates for the durability gap.

### MessageRouter reload is synchronous
`reload()` is called synchronously in `EntityController` and `TopicController` before returning the HTTP response. This is fine because reload is fast (simple DB queries). If routing tables grow very large, this could be made async.

### Headers stored as VARCHAR (not BLOB)
Both key and value in the `STRUCT(key VARCHAR, value VARCHAR)[]` are plain strings. Non-UTF-8 binary header values are base64-encoded on write so no data is lost. This makes headers fully queryable via the `headers_get` DuckDB macro and readable in the UI without any cast.

### ScheduledReplayService is in-memory only
Scheduled replays are not persisted to DuckDB. A service restart silently drops all pending/streaming replays. Acceptable for the current use-case (test fixtures); could be persisted if needed.

### Bootstrap YAML still needed for table creation
`SchemaManager.createLakeTables()` reads from `JoxetteProperties.getBootstrap()` to create the initial DuckLake tables at startup. Entity types added via REST API are created dynamically by `EntityController.createEntityType()`. The bootstrap config seeds DuckDB config tables; `MessageRouter` reads those tables rather than the YAML directly.

## Next Steps (suggested)
- [ ] **Verify `entity_source_matchers.id_source` constraint** — check constraint uses `'headers'` (plural) but `EntityIdExtractor` uses `'header'` (singular). Fix the constraint or the extractor to align.
- [ ] **Integration test: `rebuildKnownEntities` from object storage** — write entity events to DuckLake (Testcontainers MinIO), wipe `known_entities`, call `CassetteLifecycleService.rebuildKnownEntities()`, assert all `(entity_type, entity_id)` rows are restored with correct `first_seen`/`last_seen` timestamps
- [ ] **Integration test: `exportSnapshotToObjectStore`** — verify EXPORT DATABASE to S3 registers snapshot in local `snapshots` table
- [ ] **`TopicRecorderTest` full batch→DuckDB path** — current test does not exercise the write path end-to-end
- [ ] **UI: SSE/NDJSON streaming** — expose streaming replay in the UI (currently only paginated JSON is used)
- [ ] **UI: progress indicators** — add progress feedback for long-running operations (compaction, rebuild)
- [ ] **UI: verify entity source mapping** — confirm `idSource`/`idExpression` field names match between frontend `AddSourceRequest` and backend `EntitySourceConfig.MatcherConfig`
- [ ] **Replay-to-topic UI integration** — wire `ReplayToTopicPanel` to scheduled replay endpoints; add cancel button
- [ ] **Retention UI** — add retention configuration and history pages (backend API exists, UI not yet built)
