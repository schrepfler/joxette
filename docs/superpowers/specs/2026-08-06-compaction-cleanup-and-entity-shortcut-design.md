# Compaction Disk-Space Cleanup + Entity-Page Compact Shortcut — Design

## Goal

Fix a real disk-space leak in the existing compaction feature (superseded files
from every merge are never deleted), and add a convenience "compact this
entity type" button on the entity detail page.

## Background

This originated from a UI request ("show how many physical files hold one
entity's data, with a compact-to-single-file button"). Investigation found:

- Entity/general cassette tables are flat, unpartitioned DuckLake tables —
  there is no physical grouping by entity or bucket, so "files for one
  entity" isn't a answerable/actionable number today. Real per-entity/
  per-bucket file isolation would require `ALTER TABLE ... SET PARTITIONED
  BY (bucket)`, a separate, larger project intentionally out of scope here
  (bucketing itself — the logical `bucket` column and its cardinality — is
  fine as-is and not changing).
- `CompactionService` already merges small files via
  `ducklake_merge_adjacent_files`, but **never** calls
  `ducklake_expire_snapshots` or `ducklake_cleanup_old_files` afterward.
  Per DuckLake's own docs: *"Calling `ducklake_merge_adjacent_files` does
  not immediately delete the old files... DuckLake does not automatically
  delete old data files to support time travel; expired snapshots must be
  handled to trigger file cleanup."* Every compaction run — scheduled or
  manual, since the feature was first built — has therefore been creating
  new merged files without ever deleting the small ones they replaced,
  silently doubling storage for whatever got compacted. This is very
  likely the direct cause of the reported disk-space shortage, independent
  of the original entity-view UI question.
- `ducklake_cleanup_old_files` (deletes *tracked-but-superseded* files) is
  distinct from the already-used `ducklake_delete_orphaned_files` (deletes
  *untracked* files from failed writes, used by the reconciliation
  feature) — different problem, both needed, neither substitutes for the
  other.

## Scope

1. **Backend**: after each compaction run's merges complete, expire old
   snapshots (configurable retention) and clean up now-unreferenced files.
   Applies uniformly to scheduled and manual (`POST /compaction/trigger`)
   runs, since both funnel through `CompactionService.executeRun`.
2. **UI**: a "Compact Entity Type" button on the entity detail page,
   scoped to the current entity's type (compaction is table-wide — there
   is no narrower scope available, per Background above). Reuses the
   existing `POST /compaction/trigger` endpoint and `compactionApi.trigger`
   client function verbatim — no new endpoint.

Explicitly out of scope: physical per-entity/per-bucket file partitioning;
any UI display of "file count for this entity" (not a knowable number
without the partitioning project above).

## Design

### 1. New config: snapshot retention

`JoxetteProperties.Compaction` gets one new field, alongside the existing
`enabled`/`schedule`/`lockTtlMinutes`:

```java
/**
 * How long an old (superseded-by-merge) snapshot is kept before it's
 * expired and its files become eligible for cleanup, in hours. Trades off
 * disk space (higher = more retained history, more space held by
 * superseded files) against time-travel/restore window (lower = less
 * history available via GET /cassettes/snapshots restore).
 */
private int snapshotRetentionHours = 24;
```

Getter/setter follow the existing pattern (`getSnapshotRetentionHours()` /
`setSnapshotRetentionHours(int)`). YAML key:
`joxette.compaction.snapshot-retention-hours` (default `24`).

### 2. `CompactionService`: expire + cleanup after merges

New private method, called once per run (not per table — `cleanup_all`
operates catalog-wide, so running it once per table in a multi-target run
would be redundant work and redundant locking):

```java
/**
 * Expires snapshots older than {@code snapshot-retention-hours} and
 * deletes the now-unreferenced files those snapshots were the last
 * reference to. Must run once per run, after every
 * ducklake_merge_adjacent_files call — merging alone never deletes the
 * files it replaces; DuckLake keeps them for time travel until their
 * snapshot is explicitly expired.
 */
private void expireSnapshotsAndCleanup() throws SQLException {
    int retentionHours = props.getCompaction().getSnapshotRetentionHours();
    synchronized (duckDB) {
        try (Statement st = duckDB.createStatement()) {
            st.execute("CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL '"
                    + retentionHours + "' HOUR)");
            st.execute("CALL ducklake_cleanup_old_files('lake', cleanup_all => true)");
        }
    }
}
```

Called from `executeRun`, after both `compactEntityTypes`/
`compactGeneralIfEnabled` and before `checkpoint()` — mirroring how
`checkpoint()` itself already runs once per whole run, not per target.
Failure handling matches the existing pattern used for
`ducklake_merge_adjacent_files` failures elsewhere in this class (log a
WARN with `DuckDbErrors.isTransient()` classification, don't fail the
whole run — a missed cleanup this run just means the space is reclaimed
next run instead).

`max_file_size` for "single file where possible" needs no code change —
it is already the existing `joxette.compaction.entity.target-file-size-mb`
/ `general.target-file-size-mb` config (default 256 MB each); raising it
is a config change, not a feature.

### 3. UI: entity-page compact button

`ui/src/routes/entities/$entityType/$entityId.tsx`, header action row
(next to the existing Timeline/Delete buttons, `:331-355`): a new
"Compact Entity Type" button that calls the existing
`compactionApi.trigger({ targets: [`entity:${entityType}`] })` — no new
API function needed, `compactionApi.trigger` and its `TriggerRequest`
type already support this shape.

Since this compacts the whole entity *type* (every entity sharing it, not
just the one being viewed — there is no narrower scope, per Background),
the button label and a tooltip/confirmation must say so explicitly, so it
is never misread as "compact just this entity." Given compaction is an
async background operation (202-style trigger, same as the existing
Compaction page), show a toast/inline status on trigger rather than
implying immediate completion.

## Testing

- `CompactionService`: unit test (fast DuckDB harness) asserting
  `expireSnapshotsAndCleanup` is invoked once per `executeRun` call
  (not per target) and that its two SQL calls use the configured
  retention hours. A focused test on the two-statement SQL shape itself
  (mirroring existing `merge_adjacent_files` tests' style) rather than a
  full DuckLake snapshot-lifecycle integration test — verifying DuckLake's
  own snapshot-expiry semantics is DuckLake's job, not ours.
- `JoxetteProperties`: default-value test for `snapshotRetentionHours`
  (24), following the existing pattern
  (`ReconciliationSchemaAndConfigTest`-style).
- UI: browser verification (per project convention — no page-level
  component tests exist) that the button appears, is clearly labelled as
  type-wide, and successfully calls the trigger endpoint.
