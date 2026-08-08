# Entity Bucket Partitioning — Design

## Goal

Physically partition entity cassette tables by their existing `bucket` column,
giving real per-bucket subdirectories in object storage, and use that
partitioning to replace the file-count feature's slow catalog-scan fallback
with a cheap, targeted directory glob.

## Background

This originated from a UI/UX observation after the entity file-count feature
shipped (see [`2026-08-07-entity-file-count-and-storage-link-design.md`](2026-08-07-entity-file-count-and-storage-link-design.md)):
the "Object Store Files" count felt slow, and browsing the actual bucket in
RustFS showed every entity of a type dumped into one flat directory
(`main/entity_fixture/`) — not organized by entity at all.

Both observations point at the same root cause: entity/general cassette
tables are flat, unpartitioned DuckLake tables (confirmed as far back as the
[compaction cleanup spec](2026-08-06-compaction-cleanup-and-entity-shortcut-design.md),
which explicitly deferred physical partitioning as "a separate, larger
project"). This spec is that project, scoped to what's now concretely needed:
partition by the entity's `bucket` value, not by raw `entity_id` — a folder
per distinct entity ID would explode into millions of tiny directories for a
high-cardinality entity type and reintroduce the small-files problem
compaction already exists to solve. Partitioning by `bucket` groups many
entities per directory (256 by default, already configurable per entity type
via `entity_type_configs.buckets`), so entity *type* (its own table already)
and *bucket* (a config-tunable subdivision within it) together already form
the right hierarchy — no new concept needed.

Everything below was verified empirically against a real local DuckLake
catalog before writing this spec (not assumed from documentation):

- `ALTER TABLE tbl SET PARTITIONED BY (bucket)` creates real Hive-style
  physical subdirectories for *new* writes (verified: `bucket=1/`, `bucket=2/`
  appeared with the file inside).
- It is safe to re-issue on an already-partitioned table — no error, pure
  no-op (verified).
- `ducklake_merge_adjacent_files` does **not** migrate pre-existing
  unpartitioned files into the new layout — a probe with one old flat file
  and one new partitioned file returned `files_processed: 0` on merge. Plain
  compaction alone will never clean this up.
- An explicit delete-then-reinsert of a row correctly re-writes it into its
  partitioned directory (verified: the row moved into `bucket=1/`), leaving
  the original flat file's row soft-deleted via a `-delete.parquet` marker —
  i.e. the migration mechanism is a full-table rewrite, not a targeted patch.
- Files still needing migration are cheaply and exactly identifiable via a
  pure catalog-metadata query (no data scan) — verified:
  ```sql
  SELECT df.data_file_id, df.path
  FROM __ducklake_metadata_lake.ducklake_data_file df
  JOIN __ducklake_metadata_lake.ducklake_table t ON t.table_id = df.table_id
  WHERE t.table_name = ? AND t.end_snapshot IS NULL
    AND df.end_snapshot IS NULL
    AND NOT EXISTS (
      SELECT 1 FROM __ducklake_metadata_lake.ducklake_file_partition_value fpv
      WHERE fpv.data_file_id = df.data_file_id
    );
  ```
  A file appears here iff no row exists for it in `ducklake_file_partition_value`
  — old flat files have none; anything written after `SET PARTITIONED BY`
  always does.
- There is no queryable "which physical file did this row come from" filter
  usable in a `WHERE` clause on a DuckLake table directly (same limitation
  already documented in the file-count spec) — so the migration cannot be
  scoped to "just this one legacy file's rows" without an expensive scan.
  The migration is therefore a **whole-table** rewrite, not incremental.

## Scope

1. **`SchemaManager`**: declare `bucket` partitioning on every entity table,
   new and pre-existing, idempotently, on every startup.
2. **`CompactionService`**: detect entity tables with leftover unpartitioned
   files and migrate them via a one-time full-table rewrite, before the
   existing merge step. Old flat files are left soft-deleted and get
   physically reclaimed by the already-existing `expireSnapshotsAndCleanup`
   step later in the same run.
3. **`EntityReplayService`**: fast path in the file-count logic — once a
   table has no pending legacy files, skip the catalog-metadata prune
   entirely and glob the entity's own bucket directory directly.

Explicitly out of scope:
- Partitioning by anything other than `bucket` (e.g. raw `entity_id`, date,
  tenant) — rejected per Background above.
- Bounding/chunking the migration rewrite into smaller batches. Verified
  there's no clean way to scope it below whole-table without an expensive
  scan; accepted as a one-time, per-entity-type cost (see Design §2).
- Async/lazy-loading the file-count field on the entity page. Was
  reconsidered given the earlier slowness complaint, but a decision was made
  that a migrated table's bucket-glob is cheap enough for the existing
  synchronous `statsQuery` — no new endpoint or lazy field.
- General/topic cassette tables. This is entity-only; general cassettes have
  no `bucket` column and no per-entity lookup need.

## Design

### 1. `SchemaManager`: declare partitioning on every startup

`migrateEntityCassetteTables` (`SchemaManager.java:862-880`) already runs
unconditionally on every boot, queries `duckdb_tables()` for every
`entity_*` table regardless of how/when it was created, and applies
idempotent per-table schema fixes (currently `dropColumnIfExists` for a
legacy column). This is the existing, correct hook.

There's already an exact template for "idempotent `ALTER TABLE ... SET ...`,
safe to repeat every startup, warn-and-swallow on failure so an
unsupported/older DuckLake can't block boot" — `ensureTableSorted`
(`SchemaManager.java:918-930`), currently used for `SET SORTED BY` right
after table creation:

```java
static void ensureTableSorted(Connection conn, String catalog,
                               String tableName, String sortedBy) {
    String sql = "ALTER TABLE " + catalog + ".main." + tableName
                 + " SET SORTED BY " + sortedBy;
    try (Statement stmt = conn.createStatement()) {
        stmt.execute(sql);
        log.debug("SORTED BY applied to {}.main.{}: {}", catalog, tableName, sortedBy);
    } catch (SQLException e) {
        log.warn("Could not apply SORTED BY to {}.main.{} ({}); " +
                 "DuckLake will not enforce sort order automatically for this table",
                 catalog, tableName, e.getMessage());
    }
}
```

Add a sibling, `ensureTablePartitioned(Connection conn, String catalog,
String tableName)`, issuing `ALTER TABLE {catalog}.main.{tableName} SET
PARTITIONED BY (bucket)` in the same try/warn-and-swallow shape (idempotency
on repeat verified empirically in Background — DuckLake treats a repeated
`SET PARTITIONED BY` with the same column as a no-op, no error). Call it from
`migrateEntityCassetteTables`'s existing per-table loop, alongside
`dropColumnIfExists`:

```java
for (String tableName : tableNames) {
    dropColumnIfExists(conn, catalog + ".main." + tableName, "kafka_value_str");
    ensureTablePartitioned(conn, catalog, tableName);
}
```

This makes every entity table declare bucket partitioning before any other
code runs, on every boot — new tables never accumulate a single unpartitioned
file; only tables that already existed before this change ships have any
legacy files at all, and only until compaction cleans them up (§2).

### 2. `CompactionService`: one-time migration before merge

New step inside `doCompactEntityType` (`CompactionService.java:407`), inside
the existing `synchronized (duckDB)` block, before the current
`ducklake_merge_adjacent_files` call (line 426):

1. **Detect.** Run the verified `NOT EXISTS` query above, scoped to
   `entity_{type}`. If it returns zero rows, skip straight to the existing
   merge logic — this is the common case for every run after the one-time
   migration completes.
2. **Migrate.** If any legacy files are found, do the full-table rewrite,
   wrapped in an explicit transaction (verified in the probe this way) so a
   crash mid-rewrite can't leave the table permanently empty — DuckDB
   auto-commits each statement individually outside an explicit transaction,
   and the `DELETE` and `INSERT` are two separate statements here:
   ```sql
   BEGIN TRANSACTION;
   CREATE TEMP TABLE tmp_migrate_{type} AS SELECT * FROM lake.main.entity_{type};
   DELETE FROM lake.main.entity_{type};
   INSERT INTO lake.main.entity_{type} SELECT * FROM tmp_migrate_{type};
   COMMIT;
   ```
   (The temp table is session-scoped and drops itself; no explicit `DROP
   TABLE` needed.) Every row gets re-written through DuckLake's now-
   partition-aware writer, landing in its correct `bucket=N/` file. The
   original flat file(s) end up with every row soft-deleted (matches the
   verified probe behavior). This entire step runs under the same
   `synchronized(duckDB)` lock already guarding every other query in this
   codebase, so there's no window where a concurrent reader could observe
   the transiently-empty table between `DELETE` and `INSERT` regardless of
   the transaction — the transaction's purpose here is purely crash-safety
   across a process restart, not concurrency control.
3. Continue into the existing merge call unchanged — it now operates
   cleanly within partitions (any small per-bucket files from the rewrite get
   consolidated).

This runs under the same lock (`lockManager.tryAcquire("entity:" + entityType)`,
lines 409-421) already guarding the merge — no new locking concern, but a
real operational one: for an entity type with a lot of existing data, this
whole-table rewrite is heavier than the bounded, adjacent-file merges the
lock was originally sized for. Accepted deliberately (see Scope) — it fires
at most once per entity type, ever, and compaction already defaults to an
off-peak 3am schedule.

`expireSnapshotsAndCleanup` (`CompactionService.java:167`, after all
per-entity/per-topic compaction in `executeRun`) needs no changes — it
already expires old snapshots and cleans up tracked-but-superseded files
lake-wide; the migration's soft-deleted flat files fall under exactly that
umbrella once their retention window passes.

### 3. `EntityReplayService`: bucket-glob fast path

`pruneCandidateFiles` (`EntityReplayService.java:782-808`) gets a new check
at its top, before the existing `listAllFiles` call. It currently takes
`(String table, String entityId)` — `table` is the already-derived
`"entity_" + entityType` string (`bareTable` at the `getEntityStats` call
site, `EntityReplayService.java:709`), but the fast path also needs the raw
`entityType` on its own (for `MessageRouter.computeBucket` and for looking up
`entity_type_configs.buckets`), which isn't threaded through today. Both
`countEntityFiles` (`EntityReplayService.java:773`) and `pruneCandidateFiles`
need `entityType` added as a parameter, passed from the `getEntityStats` call
site where it's already in scope:

```java
private List<String> pruneCandidateFiles(String table, String entityType, String entityId) {
    if (!hasLegacyUnpartitionedFiles(table)) {
        return globBucketDirectory(table, entityType, entityId);
    }
    // ... existing listAllFiles + candidateFilenamesViaColumnStats fallback, unchanged
}
```

`hasLegacyUnpartitionedFiles` reuses the exact same verified `NOT EXISTS`
query as the compaction migration's detection step (§2) — a table is "fully
migrated" the moment that query returns zero rows, which `CompactionService`
already drives toward. `globBucketDirectory` computes the bucket via
`MessageRouter.computeBucket(entityType, entityId, bucketCount)` — already
package-private in `com.joxette.replay`, directly callable, no import needed
— then globs just that one directory:

```sql
SELECT file FROM glob('<objectStoragePath>main/<table>/bucket=<N>/**/*.parquet')
```

`bucketCount` comes from `entity_type_configs.buckets` for the entity type —
a plain DuckDB query on the same shared `duckDB` connection (that table
isn't part of the DuckLake catalog). The resulting file list still goes
through the existing, unchanged `verifyCandidates` for the exact match — this
change only narrows *which files* get verified, it doesn't touch how
verification works, so `EntityFileCountIT`'s existing correctness assertions
keep holding.

For an unmigrated table, behavior is byte-for-byte what shipped in the
previous feature — this is purely an additive fast path, not a rewrite of
the existing logic.

## Testing

- `SchemaManager`: whatever existing test coverage exists for
  `migrateEntityCassetteTables`/`dropColumnIfExists` gets a parallel
  assertion that `SET PARTITIONED BY (bucket)` was issued for a given table
  (verifiable against a real DuckLake catalog only — the fake `:memory:`
  harness doesn't support it, matching the established precedent from the
  file-count feature).
- `CompactionService`: the existing fast `CompactionServiceTest` harness
  (`DuckDBTestSupport`, a fake `:memory:`-attached `lake`, confirmed to not
  support real DuckLake features) cannot exercise real migration behavior.
  A new Testcontainers/MinIO IT test, following `CompactionSnapshotCleanupIT`
  and `EntityFileCountIT`'s established pattern, is needed:
  - Create an entity table, write rows for two+ entities *before*
    partitioning is declared (forcing legacy flat files).
  - Run `SchemaManager`'s startup path (or directly issue the ALTER) to
    declare partitioning, then write more rows for the same entities
    (forcing new partitioned files alongside the old flat ones).
  - Trigger compaction and assert: (a) the old flat file(s) are gone from
    object storage after the run completes (not just soft-deleted in the
    catalog — actually reclaimed, same style of assertion as
    `CompactionSnapshotCleanupIT`), (b) every remaining file lives under a
    `bucket=N/` path, (c) row-level data survived the migration intact (same
    entities, same values, nothing lost or duplicated).
- `EntityReplayService`: extend `EntityFileCountIT` (or add a sibling IT)
  with a case that first migrates a table (per above), then asserts
  `getEntityStats` for an entity returns the same correct `fileCount` as
  before, but via the fast path — verifiable indirectly by asserting the
  bucket-scoped glob path was taken (e.g. via a debug log assertion or by
  making `hasLegacyUnpartitionedFiles`/`globBucketDirectory` package-visible
  for direct unit-style testing against the same IT's real catalog
  connection, rather than only exercising them transitively through
  `getEntityStats`).
