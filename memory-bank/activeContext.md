# Active Context

## Current Work Focus — UI visualisation (view modes, SOL panel, barcode, sunburst)

Adding multi-mode visualisations to every cassette page, informed by the Motif Analytics
and Observable sequences collection analysis (docs/motif-ui-reference.md,
docs/sunburst-sequence-reference.md, docs/observable-sequences-reference.md).

### Completed this session

**Infrastructure**
- `ViewModeBar<T>` — generic segmented control used across all three pages
- `SolQueryPanel` — SOL textarea editor + recipe dropdown (10 recipes) + result table with
  colour-hashed event pills and inline JSON expand; calls `POST .../sol-match`
- `SequenceBarcodeView` — SVG barcode: one row per entity, rect width ∝ timestamp gap,
  colour by messageType (golden-angle hash) or SOL tag highlight; hover tooltip
- `MultiEntityBarcodePanel` — parallel `useQueries` fetcher for ≤20 entities, stacked rows
- `cassettesApi.solMatchEntity/solMatchTopic` + `SolMatchResponse` type in client.ts
- `NflDriveSolTest` (17 tests) — real CHI@GB 2019 drive data as SOL unit tests; 42 total green

**Page updates**
- Entity detail: 2-tab → 4-mode [Records][SOL][Timeline][Barcode]
- Topic detail: 2-tab → 4-mode [Records][SOL][Timeline][Barcode]
- Entity type detail: Known Entities gains [List][Barcode] switcher

**Documentation**
- `docs/motif-ui-reference.md` — full Motif Analytics UI reverse-engineering (ASCII art,
  all components, colour system, Joxette mapping)
- `docs/sunburst-sequence-reference.md` — Observable NFL sunburst + sequence query language
- `docs/observable-sequences-reference.md` — barcode chart, tennis sunburst, data schemas
- `docs/sol-specification.md` — SOL language reference from Motif wayback archive

### Next up

1. **Sunburst chart** — D3 prefix-tree chart for entity type pages
   - Backend `POST /cassettes/entities/{type}/sunburst`
   - TypeScript hierarchy builder + D3 arc geometry (sqrt radii, arcVisible)
   - Zoom (double-click), breadcrumb trail, colour modes
   - Integrate into entity type page as a 3rd view mode [List][Barcode][Sunburst]

2. **SOL panel polish**
   - CodeMirror syntax highlighting (replace plain textarea)
   - Autocomplete from entity `messageType` vocab (GET /cassettes/entities/{type}/fields)
   - Sequence inspector (tag coverage bars below editor)

3. **Barcode enhancements**
   - Colour by numeric field (diverging red→green, like EPA in NFL chart)
   - SOL-tag overlay on multi-entity barcode

---

## Previous Focus — SOL engine + match endpoints

`sol` module (standalone, zero Joxette deps) + `joxette-sol` adapter +
`POST .../sol-match` endpoints + `SolMatchIT`. All complete and green.
See progress.md for full checklist.

## Previous Focus — Kafka exponential backoff

`resilience4j-spring-boot4:2.4.0` added. `BrokerConnectionFactory` raises Kafka
reconnect backoff from 50ms → 1s (max 30s). `RecordingCoordinator.runInSupervisedScope`
wraps recorder in Resilience4j `Retry` with exponential-random backoff (5s → ×2 → 5min cap).

## Previous Focus — UI retention page + scheduled replay

Retention page (`/retention`) with status, trigger, history table.
`ReplayToTopicPanel` gained `startDelayMs` input passed as `start_delay_ms` query param.
`GET /compaction/retention-history` + `POST /compaction/trigger-retention` added to backend.

## Previous Focus — Maven module split (joxette-core / joxette-kafka / joxette-service / joxette-test-kit)

Four-module layout. See progress.md for full details.

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

### Bootstrap YAML still needed for table creation
`SchemaManager` reads from `JoxetteProperties.getBootstrap()` at startup.
