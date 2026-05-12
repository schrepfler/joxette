# Progress

## What Works

### Maven module layout
- [x] Four Maven modules: `joxette-core`, `joxette-kafka`, `joxette-service`, `joxette-test-kit`
- [x] `joxette-core` — replay engine + DTOs + SPIs (`CassetteSource`, `EntityCassetteSource`, `RecordSink`); no Spring / DuckDB / Kafka on the classpath
- [x] `joxette-kafka` — `KafkaRecordSink` (depends on core + kafka-clients only)
- [x] `joxette-service` — Spring Boot app; `TopicReplayService`/`EntityReplayService` implement the core SPIs; `KafkaRecordSinkFactory` owns per-broker producer cache
- [x] `joxette-test-kit` — `InMemoryCassetteSource`, `InMemoryEntityCassetteSource`, `CapturingRecordSink`, `ReplayEngineBuilder` — drives a `ReplayEngine` in-process with no DuckDB / Spring / Kafka broker
- [x] `ReplayEngineBuilderTest` proves the kit can replay a cassette into a capturing sink at `speed=2.0`
- [x] `swagger-annotations-jakarta` pinned to `2.2.43` in parent `dependencyManagement` (Spring Boot BOM ships the old non-jakarta variant)

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
- [x] `RecordSink` SPI — blocking, virtual-thread friendly; `SinkException` signals permanent transport failure
- [x] `KafkaRecordSink` — `RecordSink` impl; owns byte/header/timestamp encoding, blocks on `producer.send(rec).get()`
- [x] `KafkaRecordSinkFactory` — `@Component`; caches one `KafkaProducer` per broker id, closes all on shutdown
- [x] `ReplayEngine` — pure (no Spring); general + entity replay (merge-sorted by `kafka_timestamp`); reusable outside the service
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
- [x] Retention page (`/retention`) — status, trigger, history table (last 20 runs); `g r` hotkey
- [x] Snapshots page (list, create local, export to object store, restore)
- [x] Health page
- [x] Settings page (`ui/src/routes/settings/index.tsx`, `appStore.ts`)
- [x] About page
- [x] Dark/light theme toggle
- [x] Toast notifications for all mutations
- [x] Confirm dialogs for destructive actions
- [x] `ReplayToTopicPanel` — configuring, triggering, progress bar; Stop button; start-delay input (passes `start_delay_ms`)
- [x] Timeline views — `CassetteTimeline.tsx` (canvas-based, pan/zoom/keyboard nav, proportional timestamp spacing, progressive page loading); exposed at `$topic_.timeline.tsx` and `$entityType/$entityId_.timeline.tsx`
- [x] `TruncateDialog` component
- [x] `useDebounce` hook
- [x] `ViewModeBar<T>` — segmented view-mode switcher
- [x] `SolQueryPanel` — SOL query editor with recipes, run, result table
- [x] `SequenceBarcodeView` + `BarcodeLegend` — horizontal scrollable barcode chart
- [x] Entity detail: [Records][SOL][Timeline][Barcode] view modes
- [x] Topic detail: [Records][SOL][Timeline][Barcode] view modes
- [x] Entity type detail: Known Entities [List][Barcode][Sunburst] view modes
- [x] `SolEditor` — CodeMirror 6 with SOL syntax highlighting + autocomplete (event vocab, Ctrl+Space, ⌘↵ to run)
- [x] `SolSequenceInspector` — tag coverage bars (position + width from span indices); clickable rows filter event table; hint strip + clear button
- [x] `SequenceBarcodeView` — `'numeric'` colour mode (diverging red→white→green); `extractNumeric` dot-path extractor; `NumericLegend`; `buildTagMap` helper; `tagColor` palette
- [x] `MultiEntityBarcodePanel` — SOL overlay strip (parallel `solMatchEntity`, tag colour mode, legend); "Colour by field" numeric strip
- [x] `SunburstChart` — D3 partition + sqrt radii; zoom on double-click; breadcrumb trail; `onArcRightClick` + `collectSubtreeSeqIds`; right-click hint in stats line
- [x] `SunburstDistributionPanel` — fetches ≤20 sequences for arc subtree, extracts numeric field, 10-bucket histogram with min/max/mean
- [x] `SunburstPanel` — SOL pre-filter bar (server-side filter, match count, clear)
- [x] Known entities sort controls — A–Z / Last Active / Most Messages; compound keyset cursors (epoch-ms + entity_id); `EntitySortBy` enum
- [x] `known_entities` enrichment — `message_count`, `source_topics`, `last_message_type` columns; `SchemaManager` migration; `KnownEntitiesRepository` upsert; UI columns in entity list

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
- [x] `ReplayToTopicIT` — Testcontainers IT: seeds a 3-record general cassette, POSTs replay-to-topic at `speed=2.0`, asserts order, payload fidelity, and inter-message delay
- [x] `SpringDocIT` — verifies OpenAPI spec loads
- [x] `SolMatchIT` — 6 parameterised SOL match scenarios (entity cassette end-to-end)
- [x] `NflDriveSolTest` (sol module, 17 tests) — real CHI@GB 2019 drive data; covers funnel, three-and-out, sack, filter, match split + combine, all-pass, timeout, no-run, field goal; 42 total sol tests green

## What's Left / Known Gaps

### `sol` module — standalone library (no Joxette deps)
- [x] `sol/pom.xml` — slf4j-api + Jackson only; no Spring, no DuckDB, no Kafka
- [x] `Event`, `Sequence`, `Tag` model classes
- [x] `SolOperation` sealed interface: `MatchOp`, `MatchSplitOp`, `FilterOp`, `SetOp`, `ReplaceOp`, `CombineOp`
- [x] `SolParser` — recursive-descent parser; duration tokens, quantifiers, expression precedence
- [x] `ExpressionEvaluator` — full function library; lazy `broadcast`; short-circuit `if`/`coalesce`
- [x] `MatchEngine` — greedy/lazy matching with backtracking; all quantifiers
- [x] `SolEngine` — full pipeline executor
- [x] `SolResult` + `UnexpectedNull`
- [x] 25 unit tests: `SolParserTest` (15), `MatchEngineTest` (5), `SolEngineTest` (5)

### `joxette-sol` module — adapter layer (depends on `sol` + `joxette-core`)
- [x] `EntityRecordAdapter` — `List<EntityRecord>` → `Sequence`
- [x] `SolResultMapper` — `SolResult` → matched `List<EntityRecord>`

### SOL match endpoint (`joxette-service`)
- [x] `joxette-sol` added to `joxette-service` pom
- [x] `SolMatchService` — loads entity sequence, runs `SolEngine`, maps result
- [x] `POST /cassettes/entities/{type}/{id}/sol-match` in `CassetteController`
- [x] `POST /cassettes/topics/{topic}/sol-match` in `CassetteController`
- [x] `SolMatchIT` — 6 parameterised scenarios (simple match, wildcard, filter, duration constraint); all green

### SOL query UI (`joxette-ui`) — from Motif + Observable analysis
> Reference docs: `docs/motif-ui-reference.md`, `docs/sunburst-sequence-reference.md`
- [x] `ViewModeBar<T>` — generic segmented control: [Records][SOL][Timeline][Barcode] on entity/topic pages, [List][Barcode] on entity type page
- [x] `SolQueryPanel` — SOL textarea, ⌘↵ shortcut, 10-recipe dropdown, run button, status bar (matched/count/nulls), result table with colour-hashed event pills + inline JSON expand
- [x] `SequenceBarcodeView` — SVG barcode: proportional-width rects (time mode) or fixed-width (index mode), colour by messageType or SOL tag; hover tooltip; `BarcodeLegend`
- [x] `MultiEntityBarcodePanel` — parallel useQueries for ≤20 entities, stacked barcode rows
- [x] `cassettesApi.solMatchEntity/solMatchTopic` + `SolMatchResponse` type in `client.ts`
- [x] Entity detail: [Records][SOL][Timeline][Barcode] view modes
- [x] Topic detail: [Records][SOL][Timeline][Barcode] view modes
- [x] Entity type detail: Known Entities [List][Barcode] switcher
- [x] `SolQueryPanel` CodeMirror upgrade — syntax highlighting + autocomplete (event vocab, Ctrl+Space, ⌘↵)
- [x] Autocomplete from entity `messageType` vocab — `GET /cassettes/entities/{type}/fields`
- [x] `SolSequenceInspector` — tag coverage bars; clickable rows filter event table
- [x] Barcode: colour by numeric field (diverging red→green, `extractNumeric` dot-path)
- [x] SOL tag overlay on multi-entity barcode — `buildTagMap`, tag colour mode, legend

### Sunburst sequence chart — from Observable NFL notebook analysis
> Reference doc: `docs/sunburst-sequence-reference.md`
- [x] Backend: `POST /cassettes/entities/{type}/sunburst` — prefix-tree hierarchy, optional `solQuery` pre-filter
- [x] D3 sunburst component — sqrt radii, arcVisible filter, golden-angle hue hash
- [x] Zoom — double-click to zoom in, double-click centre to zoom out
- [x] Breadcrumb trail — pentagon chevrons, count + % in stats line
- [x] Entity type page: [List][Barcode][Sunburst] view modes
- [x] Property distribution panel — right-click arc → 10-bucket histogram + min/max/mean
- [x] SOL pre-filter — server-side filter before prefix tree construction

### Stability backlog — COMPLETED
- [x] **Tests**: `EntityReplayService` sort/cursor — 5 new tests (lastActive, mostMessages, tie-break, pagination with compound cursor)
- [x] **Tests**: `SunburstService` SOL pre-filter — 9 new Mockito unit tests (all-match, none-match, partial-match, blank query, empty-sequence skip, maxEntities cap)
- [x] **Error surfacing**: `MultiEntityBarcodePanel` SOL error count + hover tooltip
- [x] **Error surfacing**: `SunburstDistributionPanel` fetch error inline message
- [x] **TS cleanup**: zero errors — `CassetteRecord.messageType` added, `includeInternal` bool→string, `@types/d3-hierarchy` installed, unused imports/vars removed, `JsonValue` → `unknown`
- [x] **Performance**: `extractNumeric`, `rowsWithTags`, `buildNumericDomain` wrapped in `useMemo`
- [x] **Bug**: topic SOL match resolved event names from value JSON via `typeField` param
- [x] **Bug**: Mac autocomplete/autocorrect suppressed on all code/field inputs
- [ ] **Kafka**: verify short-lived AdminClient fix silences `adminclient-1` reconnect noise (needs runtime observation)

### Next phase roadmap

#### Integration test gaps
- [ ] `typeField` extraction in topic SOL match — IT scenario: topic records with null message_type, typeField resolves event names
- [ ] Sort cursor correctness — IT: page through `lastActive` / `mostMessages` across multiple pages with real DuckDB data
- [ ] `SolMatchIT` coverage: `match split` + `combine`, `replace`, `set` operations; tag span assertions

#### UI polish
- [ ] Barcode `xMode` toggle exposed in UI — switch between time-proportional and fixed-width index modes
- [ ] `SequenceQueryPanel` component — check if it still exists as dead code; remove if unused
- [ ] Sunburst zoom animation — `animating`/`prevRef` stubs left in place; wire D3 tween if desired
- [ ] `SolQueryPanel` recipe hints — show the recipe description on hover in the dropdown

#### Production readiness
- [ ] DuckLake VARIANT probe — verify VARIANT type works end-to-end through DuckLake Parquet serialisation; fall back to JSON if not
- [ ] Object storage config walkthrough — document S3/GCS/Azure setup in README
- [ ] Snapshot restore path — integration test for `POST /cassettes/snapshots/{name}/restore`
- [ ] Multi-topic entity ordering — document clock-skew caveat; add warning to entity cassette replay docs

#### `sol` library
- [ ] Group ID rename `com.joxette → com.sol` — deferred until ready to publish independently
- [ ] `SolEngine` edge-case tests: tag span correctness after `replace`, `combine` output dims

### Kafka resilience
- [x] `reconnect.backoff.ms` raised 50ms → 1s, `reconnect.backoff.max.ms` = 30s in `BrokerConnectionFactory`
- [x] `resilience4j-spring-boot4:2.4.0` — recorder restart with exponential-random backoff (5s → ×2 → 5min, unlimited retries, stops on `isStopped()`)
- [x] `joxette.recording.retry-*` properties tunable in `application.yml`

### Functional gaps
- [x] `entity_source_matchers.id_source` — verified consistent: `init.sql`, `DuckDBTestSupport`, `ConfigRepository.VALID_ID_SOURCES`, and `EntityIdExtractor` all use `'header'` (singular). `EntityIdExtractorTest` covers the header path. No fix needed.

### Testing gaps
- [x] Integration test for `CassetteBatchWriter` headers struct-array write — covered by `TopicRecorderTest.recorder_handlesHeadersAndNullKey`
- [x] Integration test for `exportSnapshotToObjectStore` — `ExportSnapshotToObjectStoreIT` (1 test, green)
- [x] Integration test for `rebuildKnownEntities` — `RebuildKnownEntitiesIT` exists with full MinIO Testcontainers coverage
- [x] `TopicRecorderTest` full batch → DuckDB write path — covered by `recorder_writesAllFieldValuesCorrectlyIncludingHeaders` (all 9 columns verified) plus 4 other scenarios; 5/5 green

### UI gaps
- [x] SSE/NDJSON streaming — both topics and entity pages have full Paged/SSE/NDJSON toggle with follow-live and StreamStatusBadge (already done)
- [x] Progress indicators — compaction polls every 2s + spinner banner; entities page shows rebuild banner (already done)
- [x] Entity detail sources `idSource`/`idExpression` — AddSourceModal and SourceCard both use `idSource`/`idExpression`, dropdown includes `header` singular (already done)
- [x] Replay-to-topic cancel button — Stop button in ReplayToTopicPanel wired to AbortController (already done)
- [x] Replay-to-topic scheduled replay — `startDelayMs` input added to ReplayToTopicPanel; passed as `start_delay_ms` query param to both `streamTopicReplay` and `streamEntityReplay`
- [x] Retention UI — `retentionApi` (getStatus, getHistory, trigger) added to client.ts; `/retention` route with status card, trigger button, history table (last 20 runs); nav entry + `g r` hotkey; backend: `GET /compaction/retention-history` and `POST /compaction/trigger-retention` added to CompactionController

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
| Replay-to-topic shape | `ReplayToTopicService` directly calling `KafkaProducerService` | `ReplayEngine` + pluggable `RecordSink` SPI (`KafkaRecordSink`, factory caches Producer per broker) | Makes the replay path reusable outside Spring (future test-kit) while keeping the engine broker-agnostic |
| Project layout | Single Maven module | Four modules (`joxette-core` / `joxette-kafka` / `joxette-service` / `joxette-test-kit`) | Test-kit consumers must be able to depend on the replay engine without pulling in Spring, DuckDB, or jOOQ |
| Replay source coupling | Engine took concrete `TopicReplayService` / `EntityReplayService` | Engine takes `CassetteSource` / `EntityCassetteSource` SPIs (core) | Lets in-memory sources drive the engine for tests; DuckLake-backed services just `implements` the SPIs |
