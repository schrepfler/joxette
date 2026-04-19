# Active Context

## Current Work Focus — Maven module split (joxette-core / joxette-kafka / joxette-service / joxette-test-kit)
Split the single-module project into four Maven modules so the replay engine and its SPIs
can be consumed outside the Spring Boot service. Core has no Spring / DuckDB / Kafka on its
classpath; test-kit has no DuckDB.

**What changed:**
- Parent `pom.xml` rewritten: `<modules>` declares the four modules, all third-party deps lifted
  into `dependencyManagement`, plugins into `pluginManagement`. New property
  `swagger-annotations.version=2.2.43` + explicit dep entry — the Spring Boot BOM ships the old
  non-jakarta variant, but core DTOs use `@Schema`, so we need the jakarta JAR.
- **`joxette-core`** — pure Java. Holds the replay engine, DTOs, and SPIs:
  `CassetteSource`, `EntityCassetteSource`, `sink.RecordSink`, `SinkException`, plus
  `ReplayEngine`, `MessageTransformer`, `CassetteRecord`, `EntityRecord`, `PagedResponse`,
  `ReplayProgress`, `ReplayToTopicRequest`, `ReplayTransformConfig`, `FieldSubstitution`.
  Deps: slf4j-api, jackson-annotations, swagger-annotations-jakarta, json-path, junit (test).
- **`joxette-kafka`** — `KafkaRecordSink` (impl `RecordSink`). Deps: joxette-core + kafka-clients.
- **`joxette-service`** — Spring Boot app. `TopicReplayService implements CassetteSource`,
  `EntityReplayService implements EntityCassetteSource`. `KafkaRecordSinkFactory` lives here
  (it's the only piece that knows about per-broker routing). jOOQ codegen + PlantUML exec
  plugin moved here (PlantUML path now `../docs`).
- **`joxette-test-kit`** — new module. `InMemoryCassetteSource`, `InMemoryEntityCassetteSource`,
  `CapturingRecordSink`, and a fluent `ReplayEngineBuilder`. `ReplayEngineBuilderTest` proves
  the kit drives a `ReplayEngine` with zero Joxette-service classes — no DuckDB, no Spring,
  no Kafka broker; asserts ordering and honoured `speed=2.0` inter-message delay.
- `ReplayEngine` updated to depend on the core SPIs rather than the concrete DuckLake-backed
  services. Complex `TransformPipeline` (jOOQ-bound via `SqlPushdownAnalyzer`) stays in
  service; the simpler `MessageTransformer` used by the engine moves to core cleanly.
- Diagrams & docs refreshed: `docs/architecture.puml` shows the four module rectangles and
  the `joxette-test-kit` consumer path.

## Previous Focus — RecordSink SPI extraction
Extracted a reusable `RecordSink` SPI for the replay-to-topic path so the same replay engine can power the service and a future test-kit without dragging in Spring or Kafka imports.

**What changed:**
- New package `com.joxette.replay.sink` holds the SPI:
  - `RecordSink` — blocking `send(targetTopic, CassetteRecord|EntityRecord)` that returns a `SendResult` or throws `SinkException`. Virtual-thread friendly; no `Future`/`CompletableFuture` in the interface.
  - `SinkException` — unchecked, signals permanent transport failure so the engine aborts the run and reports `status=failed`.
- Kafka impl lives in `com.joxette.replay.sink.kafka`:
  - `KafkaRecordSink` — blocks on `producer.send(rec).get()`, owns byte/header/timestamp encoding (UTF-8 keys/headers, base64url value decode, source timestamp on the `ProducerRecord`). Does **not** own the `Producer`.
  - `KafkaRecordSinkFactory` — `@Component` that caches one `KafkaProducer` per broker id (`null` → `BrokerConfig.DEFAULT_BROKER_ID`) and closes all in `@PreDestroy`.
- `ReplayToTopicService` renamed to `ReplayEngine` and stripped of Spring. Takes `TopicReplayService`, `EntityReplayService`, and any `RecordSink`. `doSend` no longer plumbs `Future.get()` since the sink blocks. `replayTopicToKafka` / `replayEntityToKafka` → `replayTopic` / `replayEntity`.
- `KafkaProducerService` deleted; `CassetteController` now builds a per-request `ReplayEngine` via `engineFor(brokerId)` that resolves the broker through `KafkaRecordSinkFactory`.
- New `ReplayToTopicIT` (Testcontainers) seeds a 3-record cassette, POSTs `/cassettes/topics/{topic}/replay-to-topic?speed=2.0`, and asserts order, keys/values, timestamps, and honoured inter-message delay.
- PlantUML updated: `docs/replay-pipeline.puml` splits the produce path into `ReplayEngine` + `KafkaRecordSink`; `docs/architecture.puml` shows the new trio (`ReplayEngine`, `KafkaRecordSinkFactory`, `KafkaRecordSink`). Diagrams regenerated.

## Older Focus — `com.softwaremill.jox:kafka:0.5.3` migration
Migrated from hand-rolled `com.joxette.kafka` shim to the real `com.softwaremill.jox:kafka:0.5.3` module (published to Maven Central 18 Mar 2026).

**What changed:**
- `pom.xml`: replaced commented-out block + direct `kafka-clients` dep with `com.softwaremill.jox:kafka:0.5.3` (kafka-clients is now transitive)
- Deleted `src/main/java/com/joxette/kafka/` (4 files: `ConsumerSettings`, `ProducerSettings`, `KafkaSource`, `KafkaSink`)
- `BrokerConnectionFactory`: now uses jox fluent builder API (`ConsumerSettings.defaults(groupId).bootstrapServers(...).keyDeserializer(...).property(...)`) and `ProducerSettings.defaults().bootstrapServers(...)...`; security props still applied per-broker; `AdminClient` still built from a raw `Map` (no jox wrapper for admin)
- `KafkaConfig`: import updated to `com.softwaremill.jox.kafka.ConsumerSettings`
- `TopicRecorder`: `KafkaSource` replaced with inline `Flows.usingEmit` + `settings.toConsumer()` — identical poll/seek/commit logic, no `KafkaFlow` actor (we need `ConsumerRebalanceListener` for seek which jox's `KafkaFlow` does not expose)
- `RecordingCoordinator`: import updated
- `KafkaProducerService`: `KafkaSink` replaced with `KafkaProducer` obtained via `producerSettings.toProducer()`
- `BrokerController`: peek endpoint uses `ConsumerSettings.defaults(...).bootstrapServers(...).groupId(...).toConsumer()` instead of raw Map + `ConsumerSettings.create()`
- `TopicRecorderTest`: helper methods rewritten to use fluent API; `ConsumerSettings.create()` factory removed

**Note on jox `KafkaFlow`:** Not used for the recording path because it wraps the consumer in an actor (`KafkaConsumerWrapper`) that doesn't expose `ConsumerRebalanceListener` — required for our seek-to-beginning / seek-to-timestamp functionality. We use `Flows.usingEmit` with a raw `KafkaConsumer` directly, which is the same pattern jox uses internally.

## What Was Already Done (previously undocumented)

### Retention enforcement
`RetentionService` + `RetentionScheduler` + `RetentionRun` / `RetentionStatus` — fully implemented. Deletes rows older than `retention_days` from general cassettes, entity cassettes, and `known_entities`. Cron-driven, guarded by `AtomicBoolean`, logged to `retention_history`.

### Fixed `rebuildKnownEntities` — four bugs resolved after DuckLake catalog loss:
1. `executeUpdate()` returns 0 for `INSERT…SELECT…ON CONFLICT` with bound `?` in SELECT; replaced with `execute()` + `SELECT COUNT`.
2. DuckLake catalog reset left `lake.main.entity_*` empty; `resolveEntityDataSource()` falls back to `read_parquet(glob, union_by_name=true)` when catalog table COUNT = 0.
3. Bound `?` in `SELECT` list of `INSERT…SELECT…GROUP BY` collapses all rows to one group (DuckDB planner bug with table functions); entity_type inlined as string literal.
4. After catalog-loss recovery, `resolveEntityDataSource()` also re-inserts the orphaned Parquet data back into the DuckLake catalog table so that all downstream replay/stats queries work normally without any code changes.

### Replay-to-Topic
`ReplayEngine` orchestrates reading from DuckLake and writing to any `RecordSink`. The Kafka implementation (`KafkaRecordSink` + `KafkaRecordSinkFactory`) handles broker routing and producer caching. Both general cassette and entity cassette paths are implemented. Speed multiplier (inter-message delay scaling) and `ReplayProgress` progress events are included. `ReplayToTopicIT` covers the HTTP → engine → Kafka round-trip.

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
- [x] **Integration test: `rebuildKnownEntities` from object storage** — `RebuildKnownEntitiesIT` exists with full MinIO Testcontainers coverage.
- [ ] **Integration test: `exportSnapshotToObjectStore`** — verify EXPORT DATABASE to S3 registers snapshot in local `snapshots` table
- [ ] **`TopicRecorderTest` full batch→DuckDB path** — current test does not exercise the write path end-to-end
- [ ] **UI: SSE/NDJSON streaming** — expose streaming replay in the UI (currently only paginated JSON is used)
- [ ] **UI: progress indicators** — add progress feedback for long-running operations (compaction, rebuild)
- [ ] **UI: verify entity source mapping** — confirm `idSource`/`idExpression` field names match between frontend `AddSourceRequest` and backend `EntitySourceConfig.MatcherConfig`
- [ ] **Replay-to-topic UI integration** — wire `ReplayToTopicPanel` to scheduled replay endpoints; add cancel button
- [ ] **Retention UI** — add retention configuration and history pages (backend API exists, UI not yet built)
