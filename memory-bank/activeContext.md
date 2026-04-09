# Active Context

## Current Work Focus
Stabilisation and data-durability features. The recording pipeline, replay API, compaction, and UI were all in place but had several bugs that prevented data from actually appearing in the UI. All three root-cause bugs were fixed in this session, plus disaster-recovery features were added.

## Recent Changes (this session)

### Bug Fixes
1. **`CassetteBatchWriter` headers type mismatch** — headers column is `STRUCT(key VARCHAR, value BLOB)[]` but the old code called `ps.setString()` with a JSON string. This caused every general-cassette batch INSERT to fail, so the topics page showed 0 replay records. Fixed by building DuckDB struct-array literals (`[{'key': '...', 'value': '\xNN'::BLOB}]`) inlined in the SQL.

2. **`MessageRouter` only read YAML bootstrap** — entity types registered via the REST API were never loaded into the router, so no entity routing happened and `known_entities` stayed empty. Fixed by refactoring `MessageRouter` to load from `ConfigRepository` (DuckDB) at startup and on every `reload()` call. `EntityController` now calls `reload()` after every mutating operation.

3. **`CompactionService` wrong column name** — `compactEntityBucket()` sorted by `timestamp` but the actual column is `kafka_timestamp`. This would cause every entity compaction run to fail. Fixed to use `kafka_timestamp`.

### New Features
4. **Export catalog to object storage** — `POST /cassettes/snapshots/export-to-object-store` runs `EXPORT DATABASE 's3://...'` directly via DuckDB httpfs. No local disk used. Snapshot registered in local `snapshots` table. UI button added to Snapshots page.

5. **Rebuild known_entities from cassette data** — `POST /cassettes/entities/rebuild-known-entities` wipes `known_entities` and re-scans all `lake.main.entity_*` tables for `(entity_id, MIN(recorded_at), MAX(recorded_at))`. Used for disaster recovery after catalog file loss. UI button added to Entities index page.

### Test Update
6. **`MessageRouterTest`** — rewrote to use `StubConfigRepository` (inner class, no DuckDB needed). Added `reload_picksUpNewEntitySource` test to verify live-reload works.

## Active Decisions & Considerations

### known_entities remains plain DuckDB (not DuckLake)
Intentional: `ON CONFLICT ... DO UPDATE` works correctly in plain DuckDB but not in DuckLake (no PK enforcement). Moving it to DuckLake would require deduplication-on-read which adds complexity. The rebuild operation compensates for the durability gap.

### MessageRouter reload is synchronous
`reload()` is called synchronously in `EntityController` before returning the HTTP response. This is fine because reload is fast (simple DB queries). If routing tables grow very large, this could be made async.

### Headers not stored for entity cassettes
`EntityCassetteBatchWriter` inserts `[]` for headers. This is intentional for now — entity cassettes store the full message value which is the primary concern. Headers can be added later if needed.

### Bootstrap YAML still needed for table creation
`SchemaManager.createLakeTables()` reads from `JoxetteProperties.getBootstrap()` to create the initial DuckLake tables at startup. If an entity type is added via REST API, `EntityController.createEntityType()` calls `SchemaManager.createEntityTable()` dynamically. The bootstrap config seeds DuckDB config tables; the `MessageRouter` now reads those tables rather than the YAML directly.

## Next Steps (suggested)
- [ ] **Integration test: `rebuildKnownEntities` from object storage** — write entity events to DuckLake (Testcontainers MinIO), wipe `known_entities`, call `CassetteLifecycleService.rebuildKnownEntities()`, assert all `(entity_type, entity_id)` rows are restored with correct `first_seen`/`last_seen` timestamps
- [ ] Verify headers write works end-to-end with real Kafka data (the struct literal approach needs integration testing)
- [ ] Add `startFrom: earliest` support so newly registered topics can backfill
- [ ] Consider periodic background `MessageRouter.reload()` for topics added via REST API (currently only entity config triggers reload, not topic config changes)
- [ ] Add `TopicController` reload hook to `MessageRouter` when topic modes change
- [ ] Add SSE/NDJSON streaming to entity replay in the UI (currently only paginated JSON is used)
- [ ] Retention policy enforcement (delete records older than `retention_days`)
- [ ] **Replay-to-topic** — produce cassette records back to Kafka, respecting partition key logic and `kafka_timestamp` ordering. Entity cassette replay should merge-sort across source topics by timestamp. Needs a new `KafkaProducerService` + `ReplayToTopicService`. Consider SSE progress stream.
- [ ] **Speed multiplier for replay-to-topic** — scale inter-message delays: `delay = (next_timestamp - prev_timestamp) / speedMultiplier`. Support x0.5 (slow down), x1 (real-time), x2, x5 etc. API param: `?speed=2.0`
- [ ] **Message transformation pipeline for replay-to-topic** — pluggable transforms applied before producing: (a) **restamp** — shift all timestamps so the first message = now (preserves relative timing); (b) **field substitution** — replace values at JSONPath locations with new values or auto-generated IDs (useful for replaying against a fresh environment without ID collisions)
