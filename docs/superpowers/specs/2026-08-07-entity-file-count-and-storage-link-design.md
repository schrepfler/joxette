# Entity File Count + Object Store Link — Design

## Goal

On the entity detail page, show how many physical Parquet files a specific
entity's data actually lives in, plus a link/path to the object-store
directory those files sit in.

## Background

The earlier [compaction disk-space cleanup
spec](2026-08-06-compaction-cleanup-and-entity-shortcut-design.md) explicitly
scoped this out, on the grounds that entity/general cassette tables are flat,
unpartitioned DuckLake tables — the `bucket` column has zero physical effect
on file layout, so "how many files hold this entity's data" looked like it
required either real per-entity/per-bucket physical partitioning (a separate,
larger project) or an expensive full-table scan.

Revisiting: DuckLake's catalog tracks per-file, per-column statistics
(`ducklake_file_column_stats`: `min_value`/`max_value` per column per file).
That doesn't give an *exact* per-entity file list on its own — a file's
`entity_id` range can span values that aren't actually present if writes have
no locality — but it lets us cheaply prune from "every file in the table" down
to "files that could contain this entity," then verify only those survivors
with a targeted read. That combination is genuinely answerable without
partitioning, which is why this spec exists.

The physical per-entity/per-bucket partitioning project itself remains
explicitly out of scope, same as before — this is a read-time query technique,
not a storage-layout change.

## Scope

1. **Backend**: extend the existing entity stats endpoint (`GET
   /cassettes/entities/{entityType}/{entityId}/stats`) with three new fields:
   exact physical file count, the object-store directory path, and an
   optional deep-link into a storage console UI.
2. **UI**: two new rows on the entity page's existing Stats card, populated by
   the same request the page already blocks on (no new network round trip).

No new REST endpoint, no new UI query, no schema/storage layout changes.

## Design

### 1. File count: two-stage prune + verify

New private method on `EntityReplayService`, called from `getEntityStats`
inside its existing `synchronized(duckDB)` block:

```java
private int countEntityFiles(String table, String entityId) throws SQLException
```

**Stage 1 — catalog-only prune (no object-store I/O).** Query DuckLake's
internal metadata tables (`ducklake_table`, `ducklake_column`,
`ducklake_file_column_stats`) to find the current `table_id` and the
`column_id` for `entity_id`, then select `data_file_id`s whose
`[min_value, max_value]` range could include the target entity ID — the
documented DuckLake "file pruning query" pattern. Join to `ducklake_data_file`
for the candidates' `path`.

This touches DuckLake's internal metadata schema, which isn't a stable
app-facing API — wrap it in try/catch. On failure (e.g. a future DuckLake
version renames something), log a warning and fall back to treating every
path from `ducklake_list_files('lake', table)` as a candidate, so the feature
degrades to "slower but correct" rather than breaking.

**Stage 2 — verify survivors.** For the candidate paths (whichever source
produced them):

```sql
SELECT COUNT(DISTINCT filename)
FROM read_parquet($candidates, filename => true)
WHERE entity_id = ?
```

This is the exact count. Well-compacted entity types prune to a handful of
candidates in stage 1, so stage 2 stays cheap; a poorly-compacted type with no
`entity_id` locality may prune less effectively, but stage 2 is still bounded
by however many files exist for that entity type — same cost ceiling the
existing compaction glob-count already accepts.

If an entity's data is entirely inlined (not yet flushed to Parquet),
`fileCount` is `0` — no special-casing; that's the accurate answer.

### 2. Object-store directory path

Reuse the exact path construction already in
`ReconciliationService.java:430-487` (`scanMissingFiles`):

```java
String objectStoragePath = props.getCatalog().getObjectStoragePath();
String base = objectStoragePath.endsWith("/") ? objectStoragePath : objectStoragePath + "/";
String directory = base + "main/" + table + "/";
```

(Same `base + "main/" + table + ...` pattern, without the trailing
`**/*.parquet` glob suffix since this is a directory path, not a glob.)

### 3. Optional storage console link

New config class mirroring the existing `ObjectStore` pattern in
`JoxetteProperties.java`:

```java
public static class StorageConsole {
    /**
     * URL template for deep-linking into a storage console's file browser,
     * e.g. "http://localhost:9001/rustfs/console/browser/?bucket={bucket}&key={prefix}"
     * (RustFS/MinIO-style). {bucket} and {prefix} are substituted per request
     * ({prefix} URL-encoded). Unset by default — when null, the UI shows the
     * raw s3:// path instead of a link.
     */
    private String urlTemplate;

    public String getUrlTemplate() { return urlTemplate; }
    public void setUrlTemplate(String urlTemplate) { this.urlTemplate = urlTemplate; }
}
```

Field `private StorageConsole storageConsole = new StorageConsole();` on
`JoxetteProperties`, YAML key `joxette.storage-console.url-template`.

Bucket name is parsed from `joxette.catalog.object-storage-path`
(`s3://bucket/optional-prefix/...` → `bucket`). If `urlTemplate` is unset,
`EntityStats.storageConsoleUrl` is `null`.

### 4. `EntityStats` record

`EntityStats.java` gains three fields:

```java
record EntityStats(
    String entityType, String entityId, long messageCount,
    Instant firstMessage, Instant lastMessage, Instant firstSeen, Instant lastSeen,
    Map<String, Long> countByTopic,
    int fileCount, String objectStoreDirectory, String storageConsoleUrl  // new
)
```

All construction call sites (`EntityReplayService.getEntityStats`) updated
accordingly — it's a record, so this is a compile-time-enforced change, not a
runtime risk.

### 5. UI

`ui/src/api/client.ts`'s `EntityStats` interface gains:

```ts
fileCount: number
objectStoreDirectory: string
storageConsoleUrl: string | null
```

`ui/src/routes/entities/$entityType/$entityId.tsx`'s existing Stats card
(populated by the existing `statsQuery`, no new request) gets two new rows:

- **Object Store Files**: `stats.fileCount`
- **Storage Location**: if `stats.storageConsoleUrl` is set, a link opening
  it in a new tab; otherwise the raw `stats.objectStoreDirectory` path shown
  as plain selectable text (monospace). No copy-to-clipboard component exists
  anywhere in this UI codebase today (checked — not present), so building one
  is out of scope here; selectable text is sufficient.

## Testing

- Unit test on `EntityReplayServiceTest` (or wherever `getEntityStats` is
  currently tested) covering the three new `EntityStats` fields, including
  the `storageConsoleUrl` null-when-unconfigured case.
- A real Testcontainers-MinIO IT, matching `CompactionSnapshotCleanupIT`'s
  pattern: write two entities (A and B) across multiple flushed Parquet
  files — at least one file containing only A's rows, one containing only
  B's, and ideally one mixed file containing both — then call
  `getEntityStats(type, "A")` and assert `fileCount` matches exactly the
  files containing A's rows (not B-only files, not double-counting the mixed
  file). This is the test that proves the prune+verify logic is correct, not
  just that it runs without error — a test that only checks "count > 0" would
  pass even if the query were subtly wrong (e.g. counting every file in the
  table).
- A stage-1-failure test: force the metadata-table query to fail (e.g. mock
  or a catalog state where the internal tables don't have the expected shape)
  and assert the method falls back to stage-2-over-all-files and still
  returns a correct (if less optimally-computed) count, rather than throwing.
