# Entity File Location Async Fetch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split `fileCount`/`objectStoreDirectory`/`storageConsoleUrl` out of
`EntityStats`/`GET .../stats` into a new `EntityFileLocation` record and a
new `GET .../{entityType}/{entityId}/storage` endpoint, so the entity page's
fast stats (messages, timestamps, topic breakdown) render without waiting on
the file-count computation, whatever that costs on any given call.

**Architecture:** The underlying file-count logic (two-stage catalog-prune-
then-verify, bucket-glob fast path) is unchanged — every private helper
method stays exactly as it is, just called from a new top-level method
instead of from inside `getEntityStats`. This is a data-shape/API-surface
change, not a logic change.

**Tech Stack:** Java 25 / Spring Boot 4, DuckDB JDBC, jOOQ, React + TanStack
Query (UI).

## Global Constraints

- Every Java test source file compiles together regardless of which test
  class you run with `-Dtest=X` — removing fields from `EntityStats` breaks
  `EntityFileCountIT.java` and `EntityBucketPartitioningIT.java`
  simultaneously, since Maven's `test-compile` phase compiles the whole
  `src/test/java` tree up front. Tasks are ordered so nothing is ever
  mid-way broken: Task 1 is purely additive (new record, new method, new
  endpoint — `EntityStats` keeps its old fields, `getEntityStats` keeps
  computing them, unchanged), Task 2 migrates the two ITs onto the new
  method while the old fields still exist, and only Task 3 removes them
  once nothing references them anymore.
- `EntityFileLocation` is named to avoid confusion with the existing,
  differently-scoped `com.joxette.replay.EntityStorageStats` (entity-*type*-
  level per-bucket breakdown, backing the existing `GET
  /entities/{entityType}/storage` — two path segments, not three).

---

### Task 1: Add `EntityFileLocation` + `getEntityFileLocation` + new endpoint (additive)

**Files:**
- Create: `joxette-service/src/main/java/com/joxette/replay/EntityFileLocation.java`
- Modify: `joxette-service/src/main/java/com/joxette/replay/EntityReplayService.java`
- Modify: `joxette-service/src/main/java/com/joxette/replay/CassetteController.java`
- Modify: `joxette-service/src/test/java/com/joxette/replay/EntityReplayServiceTest.java`

**Interfaces:**
- Produces: `public EntityFileLocation EntityReplayService.getEntityFileLocation(String entityType, String entityId) throws SQLException`,
  and `GET /cassettes/entities/{entityType}/{entityId}/storage` backed by it.
  Both `EntityStats` and `getEntityStats` are untouched by this task — they
  keep computing `fileCount`/`objectStoreDirectory`/`storageConsoleUrl`
  exactly as before, so nothing else in the codebase breaks yet.

- [ ] **Step 1: Write the failing tests**

Add to `EntityReplayServiceTest.java`, right after the existing
`getEntityStats_computesDirectoryAndConsoleUrl_evenWhenFileCountUnavailable`
test (after line 439, before the blank line at 440):

```java
    // -------------------------------------------------------------------------
    // getEntityFileLocation: file count / object-store location (graceful degradation)
    // -------------------------------------------------------------------------
    //
    // These run against DuckDBTestSupport's plain `ATTACH ':memory:' AS lake`
    // harness — not a real DuckLake catalog — so ducklake_list_files() and the
    // __ducklake_metadata_lake internal tables genuinely don't exist here.
    // fileCount must gracefully degrade to 0 rather than throw. Real numeric
    // correctness against a live DuckLake catalog is proven separately by
    // EntityFileCountIT.

    @Test
    void getEntityFileLocation_fileCountIsZero_whenObjectStoragePathNotConfigured() throws Exception {
        insertEntityRow("ORD-FL1", 1, "orders.events", 0, 0L,
                Instant.parse("2024-05-01T10:00:00Z"), b("{}"));

        EntityFileLocation location = service.getEntityFileLocation(ENTITY_TYPE, "ORD-FL1");

        assertThat(location.fileCount()).isZero();
        assertThat(location.objectStoreDirectory()).isNull();
        assertThat(location.storageConsoleUrl()).isNull();
    }

    @Test
    void getEntityFileLocation_computesDirectoryAndConsoleUrl_evenWhenFileCountUnavailable() throws Exception {
        com.joxette.config.JoxetteProperties props = new com.joxette.config.JoxetteProperties();
        props.getCatalog().setObjectStoragePath("s3://test-bucket/");
        props.getStorageConsole().setUrlTemplate("http://console/?bucket={bucket}&key={prefix}");
        EntityReplayService configuredService =
                new EntityReplayService(DSL.using(duckDB, SQLDialect.DUCKDB), duckDB, props);

        insertEntityRow("ORD-FL2", 2, "orders.events", 0, 0L,
                Instant.parse("2024-05-01T10:00:00Z"), b("{}"));

        EntityFileLocation location = configuredService.getEntityFileLocation(ENTITY_TYPE, "ORD-FL2");

        // No real DuckLake catalog in this harness -> file discovery fails gracefully -> 0.
        assertThat(location.fileCount()).isZero();
        // Directory/console URL are pure string construction, independent of DuckLake -> still populated.
        assertThat(location.objectStoreDirectory()).isEqualTo("s3://test-bucket/main/entity_" + ENTITY_TYPE + "/");
        assertThat(location.storageConsoleUrl())
                .isEqualTo("http://console/?bucket=test-bucket&key=main%2Fentity_" + ENTITY_TYPE + "%2F");
    }
```

(`EntityFileLocation` needs no import — same package, `com.joxette.replay`,
as this test file.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -pl joxette-service test -Dtest=EntityReplayServiceTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compile failure — `EntityFileLocation` and
`getEntityFileLocation` don't exist yet.

- [ ] **Step 3: Create `EntityFileLocation`**

Create `joxette-service/src/main/java/com/joxette/replay/EntityFileLocation.java`:

```java
package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

/** Physical file-location info for a single entity's cassette data. */
@Schema(description = "Physical file-location info for a single entity's cassette data. " +
                       "Split out of /stats since this can be slower to compute.",
        example = """
            {
              "fileCount": 3,
              "objectStoreDirectory": "s3://joxette-data/main/entity_customer/",
              "storageConsoleUrl": "http://localhost:9001/rustfs/console/browser/?bucket=joxette-data&key=main%2Fentity_customer%2F"
            }""")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EntityFileLocation(
        @Schema(description = "Exact number of physical Parquet files that contain at least one row " +
                               "for this entity. 0 if the entity's data is still fully inlined in the " +
                               "catalog (not yet flushed) or if object storage isn't configured.",
                example = "3")
        int fileCount,

        @Schema(description = "Object-store directory this entity type's Parquet files live in " +
                               "(shared with every other entity of the same type — storage is not " +
                               "physically partitioned per entity). Null if object storage isn't configured.",
                example = "s3://joxette-data/main/entity_customer/")
        String objectStoreDirectory,

        @Schema(description = "Deep link into a storage console's file browser for objectStoreDirectory, " +
                               "if joxette.storage-console.url-template is configured. Null otherwise.",
                example = "http://localhost:9001/rustfs/console/browser/?bucket=joxette-data&key=main%2Fentity_customer%2F")
        String storageConsoleUrl
) {}
```

- [ ] **Step 4: Implement `getEntityFileLocation`**

In `EntityReplayService.java`, add a new method right after `getEntityStats`
(after its closing brace at line 731, before the `// --- Entity stats:
object-store location ---` comment at line 733):

```java
    /**
     * Physical file-location info for {@code entityId} — split from
     * {@link #getEntityStats} into its own call since the file count can be
     * slower to compute than the rest of an entity's stats (see
     * {@code docs/superpowers/specs/2026-08-09-entity-file-location-async-fetch-design.md}).
     */
    public EntityFileLocation getEntityFileLocation(String entityType, String entityId) throws SQLException {
        validateEntityType(entityType);
        String bareTable = "entity_" + entityType;

        int fileCount = TopicReplayService.withObjectStoreRetry(
                "getEntityFileLocation:" + entityType, () -> {
            synchronized (duckDB) {
                try {
                    return countEntityFiles(bareTable, entityType, entityId);
                } catch (SQLException e) {
                    throw new DataAccessException("countEntityFiles failed for " + bareTable, e);
                }
            }
        });

        String objectStoreDirectory = computeObjectStoreDirectory(bareTable);
        String storageConsoleUrl = computeStorageConsoleUrl(bareTable);
        return new EntityFileLocation(fileCount, objectStoreDirectory, storageConsoleUrl);
    }
```

- [ ] **Step 5: Run the tests, verify they pass**

Run: `mvn -pl joxette-service test -Dtest=EntityReplayServiceTest`
Expected: `Tests run: 47, Failures: 0, Errors: 0` (45 existing + 2 new).

- [ ] **Step 6: Add the new controller endpoint**

In `CassetteController.java`, add right after the existing `getEntityStats`
method (after its closing brace at line 1175, before the `// ====
Batch/cohort replay ====` comment at line 1177):

```java
    @Operation(
        operationId = "getEntityFileLocation",
        summary = "Entity physical file location",
        description = "Returns the exact number of physical Parquet files containing this entity's " +
                      "data, the object-store directory those files live in, and (if configured) a " +
                      "deep link into a storage console's file browser for that directory. Split from " +
                      "/stats into its own endpoint since this can be slower to compute than the rest " +
                      "of an entity's stats."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Entity file location",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = EntityFileLocation.class),
                examples = @ExampleObject(name = "location", value = """
                    {
                      "fileCount": 3,
                      "objectStoreDirectory": "s3://joxette-data/main/entity_customer/",
                      "storageConsoleUrl": "http://localhost:9001/rustfs/console/browser/?bucket=joxette-data&key=main%2Fentity_customer%2F"
                    }"""))),
        @ApiResponse(responseCode = "400", description = "Invalid entity type name",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/entities/{entityType}/{entityId}/storage",
                produces = MediaType.APPLICATION_JSON_VALUE)
    public EntityFileLocation getEntityFileLocation(
            @Parameter(description = "Entity type name (must match `[a-z][a-z0-9_]*`)", required = true, example = "customer")
            @PathVariable String entityType,
            @Parameter(description = "Entity identifier", required = true, example = "cust-042")
            @PathVariable String entityId
    ) throws SQLException {
        return entityService.getEntityFileLocation(entityType, entityId);
    }
```

- [ ] **Step 7: Compile-check and re-run the full test class**

Run: `mvn -pl joxette-service test -Dtest=EntityReplayServiceTest`
Expected: still `Tests run: 47, Failures: 0, Errors: 0`.

- [ ] **Step 8: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/replay/EntityFileLocation.java \
        joxette-service/src/main/java/com/joxette/replay/EntityReplayService.java \
        joxette-service/src/main/java/com/joxette/replay/CassetteController.java \
        joxette-service/src/test/java/com/joxette/replay/EntityReplayServiceTest.java
git commit -m "feat(replay): add getEntityFileLocation + GET .../storage endpoint (additive)"
```

---

### Task 2: Migrate the two ITs onto `getEntityFileLocation`

**Files:**
- Modify: `joxette-service/src/test/java/com/joxette/it/EntityFileCountIT.java`
- Modify: `joxette-service/src/test/java/com/joxette/it/EntityBucketPartitioningIT.java`

**Interfaces:**
- Consumes: `EntityReplayService.getEntityFileLocation` (Task 1).

Both ITs still compile fine right now (`EntityStats` still has its old
fields, untouched by Task 1) — this task switches their *assertions* onto
the new method while that's still true, so Task 3 can safely remove the old
fields afterward without breaking anything.

- [ ] **Step 1: Update `EntityFileCountIT.java`**

Change the import (line 4) from:
```java
import com.joxette.replay.EntityStats;
```
to:
```java
import com.joxette.replay.EntityFileLocation;
```

Change the test body (lines 90-104) from:

```java
        EntityStats statsA = entityReplayService.getEntityStats(ENTITY_TYPE, "A");
        EntityStats statsB = entityReplayService.getEntityStats(ENTITY_TYPE, "B");

        assertThat(statsA.fileCount())
                .as("entity A appears in the A-only file and the mixed file, not the B-only file")
                .isEqualTo(2);
        assertThat(statsB.fileCount())
                .as("entity B appears in the B-only file and the mixed file, not the A-only file")
                .isEqualTo(2);

        assertThat(statsA.objectStoreDirectory())
                .isEqualTo("s3://" + BUCKET + "/data/main/entity_" + ENTITY_TYPE + "/");
        assertThat(statsA.storageConsoleUrl())
                .isEqualTo("http://localhost:9001/rustfs/console/browser/?bucket=" + BUCKET
                        + "&key=data%2Fmain%2Fentity_" + ENTITY_TYPE + "%2F");
```

to:

```java
        EntityFileLocation locationA = entityReplayService.getEntityFileLocation(ENTITY_TYPE, "A");
        EntityFileLocation locationB = entityReplayService.getEntityFileLocation(ENTITY_TYPE, "B");

        assertThat(locationA.fileCount())
                .as("entity A appears in the A-only file and the mixed file, not the B-only file")
                .isEqualTo(2);
        assertThat(locationB.fileCount())
                .as("entity B appears in the B-only file and the mixed file, not the A-only file")
                .isEqualTo(2);

        assertThat(locationA.objectStoreDirectory())
                .isEqualTo("s3://" + BUCKET + "/data/main/entity_" + ENTITY_TYPE + "/");
        assertThat(locationA.storageConsoleUrl())
                .isEqualTo("http://localhost:9001/rustfs/console/browser/?bucket=" + BUCKET
                        + "&key=data%2Fmain%2Fentity_" + ENTITY_TYPE + "%2F");
```

- [ ] **Step 2: Update `EntityBucketPartitioningIT.java`**

Add the import, alongside the existing `import com.joxette.replay.EntityStats;` (line 7):
```java
import com.joxette.replay.EntityFileLocation;
```

Change (lines 142-147) from:

```java
        EntityStats statsA = entityReplayService.getEntityStats(ENTITY_TYPE, "A");
        EntityStats statsB = entityReplayService.getEntityStats(ENTITY_TYPE, "B");
        assertThat(statsA.fileCount()).as("fast path gives a correct, nonzero count for A").isGreaterThan(0);
        assertThat(statsB.fileCount()).as("fast path gives a correct, nonzero count for B").isGreaterThan(0);
        assertThat(statsA.messageCount()).as("row data intact for A after migration").isEqualTo(2);
        assertThat(statsB.messageCount()).as("row data intact for B after migration").isEqualTo(2);
```

to:

```java
        EntityStats statsA = entityReplayService.getEntityStats(ENTITY_TYPE, "A");
        EntityStats statsB = entityReplayService.getEntityStats(ENTITY_TYPE, "B");
        EntityFileLocation locationA = entityReplayService.getEntityFileLocation(ENTITY_TYPE, "A");
        EntityFileLocation locationB = entityReplayService.getEntityFileLocation(ENTITY_TYPE, "B");
        assertThat(locationA.fileCount()).as("fast path gives a correct, nonzero count for A").isGreaterThan(0);
        assertThat(locationB.fileCount()).as("fast path gives a correct, nonzero count for B").isGreaterThan(0);
        assertThat(statsA.messageCount()).as("row data intact for A after migration").isEqualTo(2);
        assertThat(statsB.messageCount()).as("row data intact for B after migration").isEqualTo(2);
```

(`EntityStats`/`statsA`/`statsB` stay — still used for `.messageCount()`.)

- [ ] **Step 3: Compile-check**

Run: `mvn -pl joxette-service test-compile`
Expected: `BUILD SUCCESS` — both files still reference `EntityStats.fileCount()`
nowhere, `EntityStats` itself is unchanged so far, only these two files'
usages moved.

- [ ] **Step 4: Run both ITs**

Run: `mvn -pl joxette-service test -Dtest=EntityFileCountIT,EntityBucketPartitioningIT -Dsurefire.failIfNoSpecifiedTests=false`

Real Testcontainers/MinIO tests (~15-30s each) — poll with `pgrep -f
surefirebooter` and wait for genuine exit before trusting any completion
notification or surefire report, per this environment's repeated pattern of
premature "completed" notifications on every prior IT run this session.

Expected: `Tests run: 2, Failures: 0, Errors: 0` — same underlying
correctness already proven when these tests were written, now read through
`getEntityFileLocation` instead of `EntityStats`.

- [ ] **Step 5: Commit**

```bash
git add joxette-service/src/test/java/com/joxette/it/EntityFileCountIT.java \
        joxette-service/src/test/java/com/joxette/it/EntityBucketPartitioningIT.java
git commit -m "test(replay): migrate file-location assertions onto getEntityFileLocation"
```

---

### Task 3: Remove `fileCount`/`objectStoreDirectory`/`storageConsoleUrl` from `EntityStats`

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/replay/EntityStats.java`
- Modify: `joxette-service/src/main/java/com/joxette/replay/EntityReplayService.java`
- Modify: `joxette-service/src/test/java/com/joxette/replay/EntityReplayServiceTest.java`

**Interfaces:**
- Nothing outside `EntityStats`/`getEntityStats` should reference the three
  removed fields anymore after Task 2 — this task is a pure removal.

- [ ] **Step 1: Revert `EntityStats` to its pre-file-count shape**

Replace the full content of `EntityStats.java` with:

```java
package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.Map;

/** Aggregate statistics for a single entity derived from its cassette. */
@Schema(description = "Aggregate statistics for a single entity derived from its entity cassette. " +
                       "Message counts are based on deduplicated records only.",
        example = """
            {
              "entityType": "customer",
              "entityId": "cust-042",
              "messageCount": 17,
              "firstMessage": "2024-01-15T09:00:00Z",
              "lastMessage": "2024-06-01T10:00:00Z",
              "firstSeen": "2024-01-15T09:00:01Z",
              "lastSeen": "2024-06-01T10:00:01Z",
              "countByTopic": {
                "customer-events": 12,
                "customer-orders": 5
              }
            }""")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EntityStats(
        @Schema(description = "Entity type name", example = "customer")
        String entityType,

        @Schema(description = "Entity identifier", example = "cust-042")
        String entityId,

        @Schema(description = "Total number of deduplicated messages recorded for this entity", example = "17")
        long messageCount,

        @Schema(description = "Timestamp of the earliest deduplicated message for this entity",
                example = "2024-01-15T09:00:00Z")
        Instant firstMessage,

        @Schema(description = "Timestamp of the most recent deduplicated message for this entity",
                example = "2024-06-01T10:00:00Z")
        Instant lastMessage,

        @Schema(description = "Timestamp when this entity was first registered in the entity registry",
                example = "2024-01-15T09:00:01Z")
        Instant firstSeen,

        @Schema(description = "Timestamp when this entity was last seen in the entity registry",
                example = "2024-06-01T10:00:01Z")
        Instant lastSeen,

        @Schema(description = "Deduplicated message count broken down by source Kafka topic",
                example = "{\"customer-events\": 12, \"customer-orders\": 5}")
        Map<String, Long> countByTopic
) {}
```

- [ ] **Step 2: Simplify `StatsQueryResult` and `getEntityStats`**

In `EntityReplayService.java`, change `StatsQueryResult` (lines 119-122) from:

```java
    private record StatsQueryResult(
            long count, Instant firstMsg, Instant lastMsg,
            Map<String, Long> countByTopic, Instant firstSeen, Instant lastSeen,
            int fileCount) {}
```

to:

```java
    private record StatsQueryResult(
            long count, Instant firstMsg, Instant lastMsg,
            Map<String, Long> countByTopic, Instant firstSeen, Instant lastSeen) {}
```

Change `getEntityStats` (lines 647-731) from its current form to:

```java
    public EntityStats getEntityStats(String entityType, String entityId) throws SQLException {
        validateEntityType(entityType);

        // Pure-read CTE — no temp table writes, safe for concurrent callers on any
        // catalog backend (embedded DuckDB, Quack, PostgreSQL).
        // entityType is validated above ([a-z][a-z0-9_]*); entityId is a bind param.
        String tableName = "lake.main.entity_" + entityType;
        String dedupCte =
                "WITH deduped AS ("
                + "  SELECT kafka_timestamp AS ts, topic"
                + "  FROM " + tableName
                + "  WHERE entity_id = {0}"
                + "  QUALIFY ROW_NUMBER() OVER"
                + "    (PARTITION BY topic, kafka_partition, kafka_offset"
                + "     ORDER BY recorded_at DESC) = 1"
                + ") ";

        org.jooq.Param<String> idParam = DSL.val(entityId);

        StatsQueryResult result = TopicReplayService.withObjectStoreRetry(
                "getEntityStats:" + entityType, () -> {
            long count = 0;
            Instant firstMsg = null;
            Instant lastMsg  = null;
            Map<String, Long> countByTopic = new LinkedHashMap<>();
            Instant firstSeen = null;
            Instant lastSeen  = null;
            synchronized (duckDB) {
                var aggRecord = dsl.fetchOne(
                        DSL.resultQuery(dedupCte
                                + "SELECT COUNT(*) AS cnt, MIN(ts) AS first_msg, MAX(ts) AS last_msg"
                                + " FROM deduped",
                                idParam));
                if (aggRecord != null) {
                    count = ((Number) aggRecord.get("cnt")).longValue();
                    var f = aggRecord.get("first_msg", java.time.OffsetDateTime.class);
                    var l = aggRecord.get("last_msg",  java.time.OffsetDateTime.class);
                    if (f != null) firstMsg = f.toInstant();
                    if (l != null) lastMsg  = l.toInstant();
                }

                dsl.fetch(
                        DSL.resultQuery(dedupCte
                                + "SELECT topic, COUNT(*) AS cnt FROM deduped"
                                + " GROUP BY topic ORDER BY topic",
                                idParam))
                   .forEach(r -> countByTopic.put(
                           r.get("topic", String.class),
                           ((Number) r.get("cnt")).longValue()));

                var regRecord = dsl
                        .select(F_FIRST_SEEN, F_LAST_SEEN)
                        .from(KNOWN_ENTITIES)
                        .where(F_ENTITY_TYPE.eq(entityType).and(F_ENTITY_ID.eq(entityId)))
                        .fetchOne();
                if (regRecord != null) {
                    firstSeen = regRecord.get(F_FIRST_SEEN).toInstant();
                    lastSeen  = regRecord.get(F_LAST_SEEN).toInstant();
                }
                return new StatsQueryResult(count, firstMsg, lastMsg, countByTopic, firstSeen, lastSeen);
            }
        });

        long count = result.count();
        Instant firstMsg = result.firstMsg();
        Instant lastMsg = result.lastMsg();
        Map<String, Long> countByTopic = result.countByTopic();
        Instant firstSeen = result.firstSeen();
        Instant lastSeen = result.lastSeen();

        return new EntityStats(entityType, entityId, count, firstMsg, lastMsg,
                firstSeen, lastSeen, countByTopic);
    }
```

(This drops the `bareTable` local entirely — nothing in this method needs it
anymore; `getEntityFileLocation`, added in Task 1, computes its own.)

- [ ] **Step 3: Delete the now-duplicate old tests**

In `EntityReplayServiceTest.java`, delete the entire block from the section
comment through the second test's closing brace (originally lines 396-439,
predating Task 1's insertion above them — re-locate by the comment text if
line numbers have shifted):

```java
    // -------------------------------------------------------------------------
    // Entity stats: file count / object-store location (graceful degradation)
    // -------------------------------------------------------------------------
    //
    // These run against DuckDBTestSupport's plain `ATTACH ':memory:' AS lake`
    // harness — not a real DuckLake catalog — so ducklake_list_files() and the
    // __ducklake_metadata_lake internal tables genuinely don't exist here.
    // fileCount must gracefully degrade to 0 rather than throw. Real numeric
    // correctness against a live DuckLake catalog is proven separately by
    // EntityFileCountIT.

    @Test
    void getEntityStats_fileCountIsZero_whenObjectStoragePathNotConfigured() throws Exception {
        insertEntityRow("ORD-FC1", 1, "orders.events", 0, 0L,
                Instant.parse("2024-05-01T10:00:00Z"), b("{}"));

        EntityStats stats = service.getEntityStats(ENTITY_TYPE, "ORD-FC1");

        assertThat(stats.fileCount()).isZero();
        assertThat(stats.objectStoreDirectory()).isNull();
        assertThat(stats.storageConsoleUrl()).isNull();
    }

    @Test
    void getEntityStats_computesDirectoryAndConsoleUrl_evenWhenFileCountUnavailable() throws Exception {
        com.joxette.config.JoxetteProperties props = new com.joxette.config.JoxetteProperties();
        props.getCatalog().setObjectStoragePath("s3://test-bucket/");
        props.getStorageConsole().setUrlTemplate("http://console/?bucket={bucket}&key={prefix}");
        EntityReplayService configuredService =
                new EntityReplayService(DSL.using(duckDB, SQLDialect.DUCKDB), duckDB, props);

        insertEntityRow("ORD-FC2", 2, "orders.events", 0, 0L,
                Instant.parse("2024-05-01T10:00:00Z"), b("{}"));

        EntityStats stats = configuredService.getEntityStats(ENTITY_TYPE, "ORD-FC2");

        // No real DuckLake catalog in this harness -> file discovery fails gracefully -> 0.
        assertThat(stats.fileCount()).isZero();
        // Directory/console URL are pure string construction, independent of DuckLake -> still populated.
        assertThat(stats.objectStoreDirectory()).isEqualTo("s3://test-bucket/main/entity_" + ENTITY_TYPE + "/");
        assertThat(stats.storageConsoleUrl())
                .isEqualTo("http://console/?bucket=test-bucket&key=main%2Fentity_" + ENTITY_TYPE + "%2F");
    }
```

These are now pure duplicates of Task 1's `getEntityFileLocation_*` tests —
same setup, same assertions, just against the old (now-removed) accessors.

- [ ] **Step 4: Compile and run the full test suite**

Run: `mvn -pl joxette-service test`

This is the point where a mistake in Tasks 1-2 would surface as a compile
failure (any lingering `stats.fileCount()`/`stats.objectStoreDirectory()`/
`stats.storageConsoleUrl()` reference anywhere in the test tree). Expected:
`BUILD SUCCESS`, `EntityReplayServiceTest` at `Tests run: 45` (47 minus the
2 deleted duplicates), no regressions anywhere else.

- [ ] **Step 5: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/replay/EntityStats.java \
        joxette-service/src/main/java/com/joxette/replay/EntityReplayService.java \
        joxette-service/src/test/java/com/joxette/replay/EntityReplayServiceTest.java
git commit -m "refactor(replay): remove file-location fields from EntityStats"
```

---

### Task 4: UI — decoupled storage query

**Files:**
- Modify: `ui/src/api/client.ts`
- Modify: `ui/src/routes/entities/$entityType/$entityId.tsx`

**Interfaces:**
- Consumes: `GET /cassettes/entities/{entityType}/{entityId}/storage`
  (Task 1).

- [ ] **Step 1: Update `client.ts`**

Change the `EntityStats` interface (lines 191-203) from:

```ts
export interface EntityStats {
  entityType: string
  entityId: string
  messageCount: number
  firstMessage: string | null
  lastMessage: string | null
  firstSeen: string | null
  lastSeen: string | null
  countByTopic: Record<string, number>
  fileCount: number
  objectStoreDirectory: string | null
  storageConsoleUrl: string | null
}
```

to:

```ts
export interface EntityStats {
  entityType: string
  entityId: string
  messageCount: number
  firstMessage: string | null
  lastMessage: string | null
  firstSeen: string | null
  lastSeen: string | null
  countByTopic: Record<string, number>
}

export interface EntityFileLocation {
  fileCount: number
  objectStoreDirectory: string | null
  storageConsoleUrl: string | null
}
```

Add the fetch function right after `getEntityStats` (lines 513-514):

```ts
  getEntityStats: (entityType: string, entityId: string) =>
    request<EntityStats>(`/cassettes/entities/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}/stats`),
  getEntityFileLocation: (entityType: string, entityId: string) =>
    request<EntityFileLocation>(`/cassettes/entities/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}/storage`),
```

- [ ] **Step 2: Add the independent `storageQuery`**

In `$entityId.tsx`, add right after the existing `statsQuery` block (after
line 145):

```tsx
  const storageQuery = useQuery({
    queryKey: ['cassettes', 'entities', entityType, entityId, 'storage'],
    queryFn: () => cassettesApi.getEntityFileLocation(entityType, entityId),
  })
```

- [ ] **Step 3: Decouple the JSX from `stats`**

Change (lines 378-407) from:

```tsx
          <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: '0.75rem' }}>
            {[
              ['Messages', stats.messageCount.toLocaleString()],
              ['First Message', stats.firstMessage?.slice(0, 19).replace('T', ' ') ?? '—'],
              ['Last Message', stats.lastMessage?.slice(0, 19).replace('T', ' ') ?? '—'],
              ['First Seen', stats.firstSeen?.slice(0, 19).replace('T', ' ') ?? '—'],
              ['Last Seen', stats.lastSeen?.slice(0, 19).replace('T', ' ') ?? '—'],
              ['Object Store Files', stats.fileCount.toLocaleString()],
            ].map(([k, v]) => (
              <div key={k} style={{ background: '#f7fafc', border: '1px solid #e2e8f0', borderRadius: 6, padding: '0.5rem 0.85rem', minWidth: 140 }}>
                <div style={{ fontSize: 11, color: '#718096', marginBottom: 2 }}>{k}</div>
                <div style={{ fontSize: 14, fontWeight: 600 }}>{v}</div>
              </div>
            ))}
          </div>
          {stats.objectStoreDirectory && (
            <div style={{ marginBottom: '0.75rem' }}>
              <div style={{ fontSize: 13, fontWeight: 600, color: '#4a5568', marginBottom: 4 }}>Storage Location</div>
              {stats.storageConsoleUrl ? (
                <a href={stats.storageConsoleUrl} target="_blank" rel="noreferrer"
                   style={{ fontSize: 13, color: '#3182ce' }}>
                  {stats.objectStoreDirectory}
                </a>
              ) : (
                <span style={{ fontSize: 13, fontFamily: 'monospace', userSelect: 'all' }}>
                  {stats.objectStoreDirectory}
                </span>
              )}
            </div>
          )}
```

to:

```tsx
          <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: '0.75rem' }}>
            {[
              ['Messages', stats.messageCount.toLocaleString()],
              ['First Message', stats.firstMessage?.slice(0, 19).replace('T', ' ') ?? '—'],
              ['Last Message', stats.lastMessage?.slice(0, 19).replace('T', ' ') ?? '—'],
              ['First Seen', stats.firstSeen?.slice(0, 19).replace('T', ' ') ?? '—'],
              ['Last Seen', stats.lastSeen?.slice(0, 19).replace('T', ' ') ?? '—'],
            ].map(([k, v]) => (
              <div key={k} style={{ background: '#f7fafc', border: '1px solid #e2e8f0', borderRadius: 6, padding: '0.5rem 0.85rem', minWidth: 140 }}>
                <div style={{ fontSize: 11, color: '#718096', marginBottom: 2 }}>{k}</div>
                <div style={{ fontSize: 14, fontWeight: 600 }}>{v}</div>
              </div>
            ))}
            <div style={{ background: '#f7fafc', border: '1px solid #e2e8f0', borderRadius: 6, padding: '0.5rem 0.85rem', minWidth: 140 }}>
              <div style={{ fontSize: 11, color: '#718096', marginBottom: 2 }}>Object Store Files</div>
              <div style={{ fontSize: 14, fontWeight: 600 }}>
                {storageQuery.isLoading ? '…' : (storageQuery.data?.fileCount.toLocaleString() ?? '—')}
              </div>
            </div>
          </div>
          {storageQuery.isLoading ? (
            <div style={{ marginBottom: '0.75rem', fontSize: 13, color: '#718096' }}>Loading storage location…</div>
          ) : storageQuery.data?.objectStoreDirectory ? (
            <div style={{ marginBottom: '0.75rem' }}>
              <div style={{ fontSize: 13, fontWeight: 600, color: '#4a5568', marginBottom: 4 }}>Storage Location</div>
              {storageQuery.data.storageConsoleUrl ? (
                <a href={storageQuery.data.storageConsoleUrl} target="_blank" rel="noreferrer"
                   style={{ fontSize: 13, color: '#3182ce' }}>
                  {storageQuery.data.objectStoreDirectory}
                </a>
              ) : (
                <span style={{ fontSize: 13, fontFamily: 'monospace', userSelect: 'all' }}>
                  {storageQuery.data.objectStoreDirectory}
                </span>
              )}
            </div>
          ) : null}
```

Note the "Object Store Files" tile and the "Storage Location" block both now
read exclusively from `storageQuery`, rendered as siblings of (not nested
inside) the `{stats && (...)}` gate they used to live inside — the Stats
card's header/messages/timestamps/topic-breakdown still gate on `stats`
alone (unchanged), but the storage-specific pieces no longer implicitly wait
on `stats` having resolved first, and carry their own independent loading
state via `storageQuery.isLoading`.

- [ ] **Step 4: Type-check**

Run: `cd ui && pnpm exec tsc --noEmit`
Expected: no new errors attributable to `$entityId.tsx` or `client.ts` (the
repo has pre-existing, unrelated `SunburstChart.tsx` type errors on `main`,
confirmed present independent of this change in prior work this session).

- [ ] **Step 5: Browser-verify against a live backend**

Start the backend from a worktree checkout and the UI dev server — the
existing `joxette-kafka`/`joxette-rustfs` Docker containers are already
running and must not be restarted or killed. Navigate to an entity page,
confirm:
- The Stats card (messages, timestamps, topic breakdown) and the "Object
  Store Files" / "Storage Location" area both render, the latter showing its
  own brief loading state independently.
- Using the browser's network tab (or `read_network_requests`), confirm two
  separate requests fire on page load: `.../stats` and `.../storage`.
- No console errors.

Stop the verification backend/UI-dev-server processes gracefully afterward
(no `kill -9` — this session has previously corrupted a throwaway local
DuckDB catalog by force-killing the backend mid-write; a plain `pkill`
without `-9` lets it flush cleanly) — do not touch the Docker containers.

- [ ] **Step 6: Commit**

```bash
git add ui/src/api/client.ts ui/src/routes/entities/\$entityType/\$entityId.tsx
git commit -m "feat(ui/entities): fetch file location independently of the stats query"
```
