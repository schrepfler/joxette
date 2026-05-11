# Active Context

## Current Focus — Stability & quality

Having shipped all planned visualisation features, the focus shifts to making
the system robust: test coverage for new code, surfacing errors properly,
and closing known rough edges.

### Recently completed (visualisation sprint)

**SOL engine & endpoints**
- `sol` standalone module (zero Joxette deps): model, parser, engine, expression evaluator
- `joxette-sol` adapter: `EntityRecordAdapter`, `SolResultMapper`
- `POST .../sol-match` (entity + topic), `SolMatchService`, `SolMatchIT`

**UI visualisations**
- `ViewModeBar<T>` — generic segmented control across all pages
- `SolEditor` — CodeMirror 6 with SOL syntax highlighting + autocomplete (event vocab)
- `SolQueryPanel` — recipe dropdown, run, status bar, result table; `SolSequenceInspector` tag coverage bars; clickable tag rows filter the event table
- `SequenceBarcodeView` — time/index modes; colour by type / SOL tag / numeric field (diverging red→green); `buildTagMap`, `extractNumeric`, `NumericLegend`
- `MultiEntityBarcodePanel` — SOL overlay strip (parallel entity matches, tag colour mode); "Colour by field" numeric strip
- `SunburstChart` — D3 partition, sqrt radii, zoom, breadcrumb trail, right-click → distribution panel; `onArcRightClick`, `collectSubtreeSeqIds`
- `SunburstDistributionPanel` — fetches sequences for arc's subtree, extracts numeric field, 10-bucket histogram
- `SunburstPanel` — SOL pre-filter bar (server-side, skips non-matching sequences in prefix tree)
- Entity type detail: [List][Barcode][Sunburst] view modes with sort controls (A–Z / Last Active / Most Messages)
- Entity detail + Topic detail: [Records][SOL][Timeline][Barcode] view modes

**Backend**
- `SunburstService` — prefix-tree builder, SOL pre-filter wired
- `EntityReplayService` — `EntitySortBy` enum (id/lastActive/mostMessages), compound keyset cursors
- `known_entities` enriched: `message_count`, `source_topics`, `last_message_type`

---

## Stability plan

### 1. Test coverage (highest priority)
- `EntityReplayService` sort + compound cursor logic (unit tests with in-memory jOOQ or SQL assertions)
- `SunburstService` SOL pre-filter (unit: mock entity records, assert filtered sequences)
- `SolSequenceInspector` tag-row filtering logic (pure function — easy to unit test)
- `SolEngine` edge cases from production usage (tag spans, combine, replace)
- UI: error states in `MultiEntityBarcodePanel` (failed solMatchEntity), `SunburstDistributionPanel` (no values found)

### 2. Error surfacing
- SOL parse errors in `SolQueryPanel` show as inline error (already done for mutation errors; check parse-only path)
- `MultiEntityBarcodePanel` — individual `solMatchEntity` failures currently swallowed by `useQueries`; surface a per-row error badge
- `SunburstDistributionPanel` — network errors currently uncaught; add error state

### 3. Known TS pre-existing errors
- `client.ts:984` — `includeInternal: boolean` incompatible with `QueryParams` index signature
- `SunburstChart.tsx` — unused `setAnimating`/`prevRef`, implicit `any` params from D3 generics
- `$entityId.tsx` / `$topic.tsx` — `SequenceQueryPanel` unused import, `JsonValue` undefined

### 4. Kafka reconnect noise
- `adminclient-1` still logs aggressive reconnect attempts when broker is down
- Current fix: short-lived AdminClient per health check (15s cache); verify it's working in practice
- Fallback: suppress the log level for the specific logger if reconnect noise is still present

### 5. Performance / edge cases
- Sunburst with large entity counts (>500) — `SunburstService` pages all entities; check memory pressure
- Barcode numeric extraction — `extractNumeric` called per-record per-render (no memo); move to useMemo
- `buildTagMap` called inside render loops in `MultiEntityBarcodePanel` — already in useMemo via `rowsWithTags`

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
