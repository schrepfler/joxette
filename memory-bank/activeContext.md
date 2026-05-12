# Active Context

## Current Focus — Next phase planning

Stability sprint is complete. The system has a clean TypeScript build (zero errors),
error surfacing in all async panels, memoized render-path computations, two new test
suites, and two bug fixes (topic SOL event names, Mac autocomplete). The only open
stability item is runtime observation of the Kafka AdminClient reconnect noise.

### Completed this session (stability sprint)

**Bug fixes**
- Topic SOL match: `message_type=NULL` general cassette records now resolve event names
  from a `typeField` JSON path extracted from the value payload (`SolMatchRequest.typeField`,
  `cassetteToEntity` fallback, "type field" input in topic `SolQueryPanel`)
- Mac autocomplete suppressed: `autoComplete/autoCorrect/autoCapitalize/spellCheck` on all
  code/field inputs (`SolQueryPanel`, `SunburstPanel`, `MultiEntityBarcodePanel`,
  `SunburstDistributionPanel`)

**Tests**
- `EntityReplayServiceTest`: 5 new tests — `lastActive` / `mostMessages` sort order,
  compound cursor pagination across 2 pages, tie-break by entity_id
- `SunburstServiceTest` (new): 9 Mockito unit tests — prefix-tree structure, SOL filter
  all-match / none-match / partial-match / blank query, empty-sequence skip, maxEntities cap

**Error surfacing**
- `MultiEntityBarcodePanel`: per-row SOL error count + hover tooltip with first error message
- `SunburstDistributionPanel`: fetch error shown inline when any sequence load fails

**TypeScript — zero errors build**
- `CassetteRecord.messageType` added (was missing from interface)
- `includeInternal` bool → string in `QueryParams`-typed call
- `@types/d3-hierarchy` installed
- Unused imports, `JsonValue` → `unknown`, implicit `any` D3 params, `setAnimating`/`prevRef` removed

**Performance**
- `extractNumeric`, `rowsWithTags`, `buildNumericDomain` wrapped in `useMemo`
  in `MultiEntityBarcodePanel`

---

## Next phase options

### 1. Integration test gaps (actionable, no design needed)
- Topic SOL `typeField` extraction — IT: null message_type resolved from JSON value
- Sort cursor correctness — IT: page through `lastActive` / `mostMessages` across real DuckDB
- `SolMatchIT` expansion: `match split` + `combine`, `replace`, `set`, tag span assertions

### 2. UI polish (quick wins, user-facing)
- Barcode `xMode` toggle — time-proportional vs fixed-width index modes not exposed in UI
- `SequenceQueryPanel` — may still exist as dead code; verify and remove
- Sunburst zoom animation — wire D3 tween on double-click (stubs already in place)

### 3. Production readiness (before first real deployment)
- DuckLake VARIANT probe — verify end-to-end through Parquet; fall back to JSON
- Object storage config — S3/GCS/Azure setup documentation
- Snapshot restore IT
- Multi-topic entity ordering caveat documentation

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
