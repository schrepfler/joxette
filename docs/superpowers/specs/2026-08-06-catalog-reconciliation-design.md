# Catalog/Object-Storage Reconciliation — Design

**Date:** 2026-08-06
**Status:** Approved, ready for implementation planning

## Background

`GET /health`'s `inlinedDataSizeBytes` field always reported `0` due to a
wrong `schema_name` predicate (fixed separately — see commit `c22e693`).
Investigating that surfaced two gaps in the existing storage-observability
surface:

1. Neither `/cassettes/topics/{topic}/stats` nor
   `/cassettes/entities/{entity_type}/storage` report actual
   Parquet-on-object-storage bytes — both use the same inlined-only
   `duckdb_tables().estimated_size` statistic, despite their doc comments
   claiming otherwise.
2. There is no way to check whether the DuckLake catalog's file manifest
   actually matches what's sitting in object storage, and the only existing
   recovery mechanism (`POST /cassettes/entities/rebuild-known-entities
   ?recoverOrphanedFiles=true`) is entity-cassette-only, opt-in, and bundled
   into a different endpoint's side effect rather than a first-class
   operation.

This spec covers closing gap 2: a read-only drift audit across **all**
cassette tables (general and entity), plus opt-in recovery for one
direction of drift. It does not cover gap 1 (a `flushedSizeBytes` field on
the existing stats endpoints) — that's a smaller, separable follow-up that
can reuse this spec's `ducklake_list_files` groundwork.

**Explicitly out of scope:** full catalog rebuild when the catalog database
itself is destroyed and no snapshot exists. That failure mode needs a
from-scratch bootstrap path driven by `application.yml` config rather than
by scanning an existing (functioning) catalog, and gets its own spec later.
It will reuse this spec's per-table `ducklake_add_data_files` recovery
primitive.

## Terminology

- **Orphaned file** — a Parquet file present in object storage that the
  DuckLake catalog does not track (in any live or historical snapshot).
  Wastes storage; may also indicate the catalog lost track of real data.
- **Missing file** — a Parquet file the catalog's current snapshot
  references that is no longer present in object storage. Replay queries
  touching it will fail or silently under-return; this is the higher-severity
  direction.
- **Drift** — either condition, generically.

## Backend Design

### New package: `com.joxette.reconciliation`

Mirrors the existing `com.joxette.compaction` package shape exactly
(`CompactionService`/`CompactionScheduler`/`CompactionLockManager`), since
this is the same kind of scheduled, lockable, catalog-maintenance operation.

- **`ReconciliationService`** — `AtomicBoolean running` guard.
  `beginRun(TriggerSource, List<String> targets, boolean
  recoverOrphanedFiles)` inserts a `running` row into
  `reconciliation_history` and returns immediately with the run id.
  `executeRun(runId, targets, recoverOrphanedFiles)` does the scan (and
  optional recovery), then `updateRunRecord` on completion/failure —
  identical control flow to `CompactionService`.

- **`ReconciliationScheduler`** — `@Component` +
  `@ConditionalOnProperty(name = "joxette.reconciliation.enabled",
  matchIfMissing = true)`. `@Scheduled(cron =
  "${joxette.reconciliation.schedule}")` (default `"0 0 4 * * *"` — after
  compaction's 3am and retention's 1am, avoiding overlap). Scheduled runs
  **always** pass `recoverOrphanedFiles=false`; only a manual REST trigger
  can set it `true`. This is deliberate: audit is safe to automate,
  mutating the catalog is not (mirrors the existing
  `recoverOrphanedFiles` opt-in precedent and its documented residual
  risk in `resolveEntityDataSource()`'s javadoc).

- **Locking** — reuse `CompactionLockManager` with a new lock target
  constant (e.g. `"reconciliation"`) rather than building a second lock
  manager. A run scans the whole catalog in one pass (not parallelized
  per-table the way compaction is), so a single lock is sufficient.

### Scan mechanism

Grounded in DuckLake's actual primitives (confirmed via current docs, not
assumed):

1. **Orphaned-in-storage** — one call:
   ```sql
   CALL ducklake_delete_orphaned_files('lake', cleanup_all => true, dry_run => true);
   ```
   **`cleanup_all => true` is required alongside `dry_run => true`** —
   verified directly against a real local DuckLake catalog while writing
   this spec: `dry_run => true` on its own silently returns zero rows
   even when a genuinely untracked file sits in the table's directory
   (the exact same silent-zero failure mode as the `inlinedDataSizeBytes`
   bug that started this investigation). With `cleanup_all => true`
   added, a manually-planted stray file was correctly reported, and
   after registering it via `ducklake_add_data_files` (see below) a
   repeat dry-run correctly reported zero orphans. Catalog-wide,
   snapshot-aware (correctly excludes files still referenced by
   un-expired historical snapshots — a naive bucket-listing diff would
   false-positive on those). Each reported path is attributed to its
   owning table via the existing `main/{tableName}/…` path convention
   already used in `resolveEntityDataSource`.

2. **Missing-from-storage** — per table (all `lake.main.general_*` and
   `lake.main.entity_*` tables, discovered via `duckdb_tables()` where
   `database_name = 'lake' AND schema_name = 'main'`):
   ```sql
   SELECT data_file FROM ducklake_list_files('lake', tableName)
   -- anti-joined against:
   SELECT file FROM glob('{objectStoragePath}/main/{tableName}/**/*.parquet')
   ```
   No native DuckLake primitive exists for this direction. Scoped to the
   table's current snapshot only for v1 — historical-snapshot file loss is
   a known, documented limitation, not handled here (time-travel queries
   are a much rarer path than current-snapshot replay).

3. **Recovery** (only when `recoverOrphanedFiles=true` on the trigger
   call) — for each orphaned file found:
   ```sql
   CALL ducklake_add_data_files('lake', tableName, filePath,
        schema => 'main', ignore_extra_columns => true);
   ```
   Zero-copy registration — the file becomes a real tracked DuckLake
   object (participates in compaction, time travel, etc.), unlike the
   existing entity-only recovery code's `read_parquet(glob) → INSERT`,
   which physically duplicates data into new files and loses the
   original file's identity. **Missing files are never auto-recovered** —
   there is nothing to register; remediation is either the existing
   snapshot-restore feature (if a backup exists) or the future full-rebuild
   spec.

### REST API

Added to `CompactionController`, following the naming precedent retention
already established (`/compaction/retention-status`,
`/compaction/retention-history`, `/compaction/trigger-retention`) — that
precedent shows catalog-maintenance jobs share the `/compaction/*`
namespace and controller regardless of not being compaction itself:

| Method | Path | Description |
|---|---|---|
| GET | `/compaction/reconciliation-status` | Last run summary (status, tables scanned, orphaned/missing counts+bytes) + next scheduled time |
| GET | `/compaction/reconciliation-history?limit=20` | Past runs |
| POST | `/compaction/trigger-reconciliation` | Body `{targets?: List<String>, recoverOrphanedFiles?: boolean}` → 202 + run info; 409 (`ConflictException`) if already running |

`targets` reuses compaction's exact scoping format — a mix of bare topic
names (general cassette table) and `entity:{type}` (entity cassette
table), e.g. `["orders.events", "entity:order"]`. Omitted or empty means
scan every `lake.main.*` table.

No separate synchronous "instant report" endpoint — the scan can be slow
over a large catalog (same reasoning as compaction/retention being async),
so `POST trigger` + `GET status` covers both scheduled and on-demand needs
without a fourth redundant endpoint.

### Data model

```sql
CREATE SEQUENCE IF NOT EXISTS seq_reconciliation_history START 1;
CREATE TABLE IF NOT EXISTS reconciliation_history (
    id                 INTEGER PRIMARY KEY DEFAULT nextval('seq_reconciliation_history'),
    started_at         TIMESTAMPTZ NOT NULL,
    completed_at       TIMESTAMPTZ,
    status             VARCHAR NOT NULL CHECK (status IN ('running','completed','failed')),
    triggered_by       VARCHAR NOT NULL,
    targets            VARCHAR[],
    tables_scanned     INTEGER NOT NULL DEFAULT 0,
    orphaned_files     INTEGER NOT NULL DEFAULT 0,
    orphaned_bytes     BIGINT  NOT NULL DEFAULT 0,
    missing_files      INTEGER NOT NULL DEFAULT 0,
    missing_bytes      BIGINT  NOT NULL DEFAULT 0,  -- from ducklake_list_files'
                                                      -- data_file_size_bytes, i.e. the
                                                      -- catalog's last-known size of a
                                                      -- file that's now gone
    recovered_files    INTEGER NOT NULL DEFAULT 0,
    recovery_requested BOOLEAN NOT NULL DEFAULT false,
    details            JSON,      -- {orphanedFiles:[...], missingFiles:[...]}
                                   -- capped at 500 entries per direction, with
                                   -- a `truncated: true` flag when the cap is
                                   -- hit — never silently dropped
    error_message      VARCHAR
);
```
Created in `SchemaManager`, placed right after `retention_history`, same
sequence-then-table skeleton as `compaction_history`/`retention_history`.

### Configuration

New `JoxetteProperties.Reconciliation` nested class (template:
`.Retention`, `JoxetteProperties.java:379-402`), added as its own field —
not sharing `joxette.compaction.enabled` the way retention does; that
shared-flag quirk in the existing code isn't worth propagating to a third
feature.

```yaml
joxette:
  reconciliation:
    enabled: true
    schedule: "0 0 4 * * *"
```

### Metrics

Two new gauges in `JoxetteMetrics`, same retained-supplier pattern as the
existing gauges (see `project_micrometer_gauge_weakref` — an unretained
`Supplier` silently goes NaN after GC):

- `registerReconciliationOrphanedFilesGauge(Supplier<Integer>)`
- `registerReconciliationMissingFilesGauge(Supplier<Integer>)`

Both reflect the most recently **completed** run's counts, held in memory
by `ReconciliationService`; `0` before the first run ever completes.

### Testing

`ducklake_delete_orphaned_files`, `ducklake_add_data_files`, and
`ducklake_list_files` are real DuckLake catalog functions — they do not
work against `DuckDBTestSupport`'s fast unit harness (a plain `ATTACH
':memory:' AS lake`, not a real `ducklake:` attach). Scan and recovery
logic must be `@SpringBootTest` + Testcontainers-MinIO integration tests
(`com.joxette.it` package), following `RebuildKnownEntitiesIT`'s pattern:
real DuckLake catalog, real cassette writes through the production write
path, real MinIO-backed Parquet flush, then deliberately deleting/adding
objects in MinIO before a reconciliation run to engineer both drift
directions. Scheduler/locking/history-bookkeeping (nothing touching
DuckLake internals) can stay fast unit tests, mirroring
`CompactionServiceTest`/`CompactionDistributedLockTest`.

## UI Design

### Route: `ui/src/routes/reconciliation/index.tsx`

Structurally mirrors `ui/src/routes/compaction/index.tsx` /
`retention/index.tsx` almost line-for-line — same `useQuery`
(status, conditional `refetchInterval`: 2s while running else 10–30s),
`useQuery` (history), `useMutation` (trigger, invalidates the
`['reconciliation']` query key prefix, toasts via `useToast()`), and the
`useRef`+`useEffect` pattern that watches `status.running` transitioning
true→false to fire a completion/failure toast.

**Layout** (per the approved mockup — severity-first, with status/tables
folded into the stat row):

1. **Severity banner** (new component — none of the existing pages have
   one to reuse) — shown only when drift exists:
   - `missing_files > 0` → error tone (`--signal-error`/`--signal-error-ink`,
     `#fef2f2` background): *"N files referenced by the catalog are missing
     from object storage — possible data loss. Last checked {time}."*
   - else if `orphaned_files > 0` → warn tone
     (`--signal-warn`/`--signal-warn-ink`): *"N orphaned files found in
     object storage, not tracked by the catalog."*
   - Hidden entirely when both are zero.

2. **Stat row** (4 cards, `jx-card` style, `grid-template-columns:
   repeat(4,1fr)`):
   - Status — `jx-badge` (`jx-badge-live`/`jx-badge-error`/`jx-badge-accent`
     for completed/failed/running)
   - Tables scanned — plain count
   - Orphaned files — count + total bytes, thin progress-bar underneath
     (`StorageMetric`-style, from `health/index.tsx`)
   - Missing files — count + total bytes (from the catalog's last-known
     size, since the files themselves are gone), same progress-bar
     treatment, error-toned

3. **History table** (`@tanstack/react-table`, same `tableStyle`/
   `thStyle`/`tdStyle` from `styles/shared.ts` as compaction/retention):
   columns Started, Status, Tables, Orphaned, Missing, Recovered.

4. **Trigger control** — "Run reconciliation" button (`primaryBtnStyle`),
   plus a "Recover orphaned files" checkbox next to it, unchecked by
   default. When checked, the button label changes to "Run + recover
   orphaned files" and adopts the warn-tone accent, signaling it will
   mutate the catalog — no confirmation dialog, consistent with the
   existing compaction/retention trigger convention (neither has one
   today).

### Navigation

One entry added to `NAV_LINKS` in `Layout.tsx` (`{ to: '/reconciliation',
label: 'Reconciliation', shortcut: 'g o' }` — `o` for "storage", avoiding
collision with `g c`/`g r` already used by compaction/retention), plus the
matching line in the `useHotkeySequences` block.

### API client

`ui/src/api/client.ts` gets a `reconciliationApi` object (`getStatus`,
`getHistory`, `trigger`) plus `ReconciliationRun`/`ReconciliationStatus`
TS interfaces, placed near and shaped like the existing
`CompactionRun`/`CompactionStatus` types (`client.ts:267-302`) — no
generated OpenAPI client exists in this codebase; every domain API is a
hand-written object of `request()` wrapper calls, and this follows that
convention exactly.

## Error Handling

- Scan failures (e.g. DuckLake attach lost mid-run, S3 auth failure) mark
  the `reconciliation_history` row `failed` with `error_message` set —
  same as compaction/retention, no exception surfaced to a scheduled
  caller.
- `POST /compaction/trigger-reconciliation` while a run is already active
  → `409` via `ConflictException`, same pattern as
  `ConflictException.compactionAlreadyRunning()`.
- Recovery partial failure (some files register via
  `ducklake_add_data_files`, one fails) — continue processing remaining
  files, log each failure, report `recovered_files` as the actual
  succeeded count (not the attempted count), surface failures in
  `error_message` as a summary. Does not abort the whole run.

## Open Questions / Follow-ups (not blocking this spec)

- `flushedSizeBytes` field on the existing `/cassettes/*/stats` endpoints
  (gap 1 from Background) — separate, smaller follow-up.
- Full catalog rebuild from object storage when the catalog itself is
  destroyed — separate future spec, reuses `ducklake_add_data_files` from
  this spec's recovery path.
- ~~Whether `ducklake_add_data_files` accepts a glob/list of files in one
  call~~ — confirmed by direct testing: it takes a single file path per
  call. Recovery loops once per orphaned file.
