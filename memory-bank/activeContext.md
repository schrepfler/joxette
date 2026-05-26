# Active Context

## Current Focus — Multi-instance / cluster hardening complete

The cluster hardening sprint is complete. Joxette can now run as multiple coordinated
instances with role-based subsystem gating, distributed compaction locking, shared
instance observability, KIP-848 cooperative rebalance support, and an upgraded
duckdb_jdbc (1.5.3.0) that confirmed VARIANT round-trips through DuckLake Parquet.

### Completed this sprint (cluster hardening + DuckDB 1.5.3 upgrade)

**DuckDB 1.5.3.0 upgrade**
- `VariantProbeTest` — 18 tests covering 4 payload classes; decimal encoding fix + selection-vector indexing fix both verified; all pass
- `value` column in all cassette tables switched from JSON → VARIANT (confirmed working)
- `write_buffer_row_group_memory_limit` applied before entity merges; tunable via `joxette.compaction.entity.row-group-memory-limit-mb`
- `TimestampSerializationIT` — 6 tests confirming TIMESTAMPTZ ISO-8601 (Z or ±HH:MM) across all 3 response formats for both cassette types

**Catalog backend detection**
- `CatalogBackend` enum + `CatalogHealthIndicator`; backend selected from `catalog.path` URI prefix
- `docs/catalog-scaling.md` rewritten with URI detection table and per-stage migration procedures

**Instance roles**
- `InstanceRoles` + `@ConditionalOnRole` + `OnRoleCondition`; gating wired to recorder, replay, compaction, retention
- `InstanceRolesTest` — 171 lines covering all combinations

**Cluster instance registry**
- `InstanceRegistry` — `joxette_instances` table, heartbeat scheduler, stale-row reaper
- `GET /instances`, `/health` cluster counts; `InstanceRegistryIT` — 12 tests

**Distributed compaction lock**
- `CompactionLockManager` — `compaction_locks` table; tryAcquire/release/cleanExpiredLocks/releaseOwnLocks
- `CompactionDistributedLockTest` — full two-service integration scenario

**KIP-848 cooperative rebalance**
- `TopicRecorder` scoped partition drain on revocation; non-revoked partitions uninterrupted
- `RebalanceIntegrationTest` — classic + KIP-848 scenarios on `apache/kafka-native:4.0.2`

**Object storage docs**
- `docs/object-storage.md` — EKS/IRSA guide (DuckDB ≥ 1.5.3 web-identity chain), HTTP_PROXY/HTTPS_PROXY/NO_PROXY support, 4 new troubleshooting rows

---

## Next phase options

### 1. Integration test gaps (actionable, no design needed)
- Topic SOL `typeField` extraction — IT: null message_type resolved from JSON value
- Sort cursor correctness — IT: page through `lastActive` / `mostMessages` across real DuckDB
- `SolMatchIT` expansion: `match split` + `combine`, `replace`, `set`, tag span assertions
- Snapshot restore IT — `POST /cassettes/snapshots/{name}/restore`

### 2. UI polish (quick wins, user-facing)
- Barcode `xMode` toggle — time-proportional vs fixed-width index modes not exposed in UI
- `SequenceQueryPanel` — may still exist as dead code; verify and remove
- Sunburst zoom animation — wire D3 tween on double-click (stubs already in place)
- UI for `GET /instances` — cluster topology panel (could live under `/health`)

### 3. Production readiness
- Multi-topic entity ordering — document clock-skew caveat; add warning to entity cassette replay docs
- Quack server integration test (stub exists; activate when DuckDB 2.0 GA)

### 4. `sol` library
- Group ID rename `com.joxette → com.sol` (deferred)
- `SolEngine` edge-case tests: tag spans after `replace` / `combine`

---

## Active Decisions

### known_entities remains plain DuckDB (not DuckLake)
`ON CONFLICT ... DO UPDATE` requires PK enforcement, which DuckLake doesn't provide.

### MessageRouter reload is synchronous
Fast enough for current table sizes. Make async if routing tables grow large.

### Headers stored as VARCHAR (not BLOB)
Non-UTF-8 binary values are base64-encoded on write. Fully queryable via `headers_get` macro.

### ScheduledReplayService is in-memory only
Not persisted across restarts. Acceptable for test fixtures.

### `sol` group ID rename deferred
Keeping `com.sol` package names as-is; group ID rename (`com.joxette → com.sol`) deferred
until the library is ready to publish independently.

### Topic SOL event name resolution
General cassette records may have `message_type=NULL` when no matchers are configured.
The `typeField` param on `SolMatchRequest` lets the user specify a JSON path to extract
the event name from the value payload at query time, without requiring permanent matcher setup.

### Catalog backend auto-detected from URI prefix
`catalog.path` URI prefix drives `CatalogBackend` selection. No separate config property needed. Embedded DuckDB is default (bare path). Quack and PostgreSQL backends need only the URI to change — no Java code or schema edits.

### Distributed lock is advisory, not blocking
When a compaction lock is held by another instance, the challenger skips rather than queues. This avoids lock contention pileup; the next cron cycle will retry. `cleanExpiredLocks()` guards against crash-orphaned locks.

### Roles default to all-active
All three roles (`recorder`, `replay`, `compaction`) default to `true`. Disabling a role is opt-in. This means a single-instance deployment needs zero config changes.
