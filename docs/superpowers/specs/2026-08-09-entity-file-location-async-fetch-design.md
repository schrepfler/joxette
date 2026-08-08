# Entity File Location — Decoupled Async Fetch — Design

## Goal

Stop the entity detail page's file-count/storage-location data from blocking
on the same request as its fast stats (message counts, timestamps, topic
breakdown), since in practice the bucket-glob fast path from
[the bucket-partitioning work](2026-08-09-entity-bucket-partitioning-design.md)
did not make it fast enough, in production, for the assumption behind
[the original entity-file-count-and-storage-link decision](2026-08-07-entity-file-count-and-storage-link-design.md)
(ship it synchronous, revisit if needed) to hold.

## Background

Three prior decisions, in order, got revisited here:

1. The original file-count feature (2026-08-07) shipped the two-stage
   catalog-prune-then-verify file count as part of the same blocking
   `GET .../stats` response the page already waited on — a deliberate choice
   at the time, given the two-stage approach was expected to be reasonably
   bounded.
2. During the bucket-partitioning brainstorm (2026-08-09), asked again
   whether to decouple it now that a migrated table's file count collapses
   to a single cheap directory glob — the answer was "synchronous is fine
   now," on the assumption that would be fast enough in the common case.
3. In practice, that assumption didn't hold — confirmed by direct
   observation, not the two-stage/bucket-glob logic itself being wrong. The
   most likely cause is that not every entity type has been migrated yet
   (compaction runs opportunistically, not on-demand), so a meaningful
   fraction of requests still hit the slower full-table-scan fallback — but
   regardless of the specific cause, decoupling the request is the right fix
   either way: it removes the file-count computation from the critical path
   of rendering the rest of the page, whatever that computation costs on any
   given call.

The only way to actually decouple the *latency* (not just its handling) is
to split the data across two HTTP requests — a single response can't have
part of it "arrive early." This is a data-shape change, not a further tuning
of the underlying file-count logic (which is unchanged by this spec).

## Scope

1. **Backend**: remove `fileCount`, `objectStoreDirectory`, `storageConsoleUrl`
   from `EntityStats`/`GET .../stats` entirely. Add a new endpoint owning all
   three together (chosen over splitting them individually — see below).
2. **Frontend**: a second, independent query for the new endpoint, fired in
   parallel with the existing stats query. The page renders fully as soon as
   the fast stats resolve; only the storage-related UI shows its own
   independent loading state.

Explicitly out of scope:
- Any change to the underlying file-count computation itself (two-stage
  catalog-prune-then-verify, bucket-glob fast path, migration detection) —
  all of that is unchanged, just relocated to a different method/endpoint.
- Splitting `objectStoreDirectory`/`storageConsoleUrl` (cheap, pure string
  computation, no I/O) from `fileCount` (the genuinely slow one) into
  separate responses — considered, rejected in favor of one combined
  endpoint for simplicity. The path/link will wait on the same request as
  the count, even though it costs nothing to compute on its own.

## Design

### 1. New `EntityFileLocation` record

`com.joxette.replay.EntityFileLocation` (new file, mirrors `EntityStats`'
existing `@Schema`-annotated record style):

```java
public record EntityFileLocation(
        int fileCount,
        String objectStoreDirectory,
        String storageConsoleUrl
) {}
```

Deliberately named `EntityFileLocation`, not something like
`EntityStorageInfo` — the codebase already has `EntityStorageStats`
(`com.joxette.replay.EntityStorageStats`, backing the existing entity-*type*-
level `GET /entities/{entityType}/storage` endpoint in
`CassetteLifecycleService`, a per-bucket breakdown across an entire entity
type). A name that close to an existing, conceptually-different type
sitting one path segment away would be a real source of confusion.

### 2. `EntityStats` loses the three fields

Revert to its pre-2026-08-07 shape:

```java
public record EntityStats(
        String entityType, String entityId, long messageCount,
        Instant firstMessage, Instant lastMessage, Instant firstSeen, Instant lastSeen,
        Map<String, Long> countByTopic
) {}
```

`EntityReplayService.getEntityStats` drops its `fileCount`/
`objectStoreDirectory`/`storageConsoleUrl`-related code entirely: the
`countEntityFiles` call and its `DataAccessException`-wrapping try/catch
inside the `synchronized(duckDB)` block, the `bareTable` local (no longer
needed once nothing downstream uses it — `tableName` alone remains), and the
final `computeObjectStoreDirectory`/`computeStorageConsoleUrl` calls. The
`StatsQueryResult` private record drops its `fileCount` field too.

### 3. New method + endpoint

`EntityReplayService.getEntityFileLocation(String entityType, String entityId)
throws SQLException` — a new method containing exactly the code being
removed from `getEntityStats` in §2 (`countEntityFiles`,
`pruneCandidateFiles`, `listAllFiles`, `candidateFilenamesViaColumnStats`,
`verifyCandidates`, `globBucketDirectory`, `lookupBucketCount`,
`computeObjectStoreDirectory`, `computeStorageConsoleUrl` are all unchanged
implementations, just called from here instead). Still wrapped in
`TopicReplayService.withObjectStoreRetry` — the file-count read is still a
genuine object-store read that can hit transient S3/RustFS errors, same as
before.

New controller endpoint in `CassetteController.java`, alongside the existing
`GET .../{entityType}/{entityId}/stats`:

```java
@GetMapping(value = "/entities/{entityType}/{entityId}/storage",
            produces = MediaType.APPLICATION_JSON_VALUE)
public EntityFileLocation getEntityFileLocation(
        @PathVariable String entityType,
        @PathVariable String entityId
) throws SQLException {
    return entityService.getEntityFileLocation(entityType, entityId);
}
```

This is a different route shape (`.../{entityType}/{entityId}/storage`, 3
path segments) from the existing `.../{entityType}/storage` (2 path
segments, entity-*type*-level stats) — no routing collision, but worth
flagging for anyone skimming the API surface expecting `.../storage` to mean
one consistent thing at every level. Accepted as-is; renaming the existing
type-level endpoint is out of scope here.

### 4. UI

`ui/src/api/client.ts`:
- `EntityStats` interface loses `fileCount`, `objectStoreDirectory`,
  `storageConsoleUrl`.
- New `EntityFileLocation` interface with those same three fields.
- New fetch function, e.g. `cassettesApi.getEntityFileLocation(entityType, entityId)`,
  hitting the new endpoint.

`ui/src/routes/entities/$entityType/$entityId.tsx`:
- New `storageQuery = useQuery({ queryKey: [...], queryFn: () => cassettesApi.getEntityFileLocation(entityType, entityId) })`,
  independent of `statsQuery` — no `enabled` gating between them, both fire
  immediately on mount.
- The "Object Store Files" stat tile and "Storage Location" block move from
  reading `stats.*` to reading `storageQuery.data?.*`, each handling its own
  `isLoading`/`error` state (e.g. the file-count tile shows a small spinner
  or "…" in place of the number while `storageQuery.isLoading`; the rest of
  the Stats card — messages, timestamps, topic breakdown — no longer waits
  on this at all).

## Testing

- Backend unit tests: the two existing `EntityReplayServiceTest` cases for
  graceful degradation (`getEntityStats_fileCountIsZero_whenObjectStoragePathNotConfigured`,
  `getEntityStats_computesDirectoryAndConsoleUrl_evenWhenFileCountUnavailable`)
  move to target `getEntityFileLocation` instead of `getEntityStats`,
  otherwise unchanged in substance.
- `EntityFileCountIT` and `EntityBucketPartitioningIT` update their
  assertions from `stats.fileCount()`/`stats.objectStoreDirectory()`/
  `stats.storageConsoleUrl()` to call `getEntityFileLocation` separately and
  assert on its result — the underlying correctness claims each test makes
  (exact per-entity file count, correct directory/console-URL construction)
  are unchanged, only which method call the assertion reads from changes.
- UI: verify both the fast stats and the storage tile independently in a
  browser — confirm the rest of the page (messages, timestamps, topic
  breakdown) renders without waiting on the storage query, and that the
  storage tile shows its own loading state before resolving.
