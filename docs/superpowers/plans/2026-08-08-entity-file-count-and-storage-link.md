# Entity File Count + Object Store Link Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** On the entity detail page, show the exact number of physical Parquet
files a specific entity's data lives in, plus a link/path to the object-store
directory those files sit in.

**Architecture:** Extend the existing `GET
/cassettes/entities/{entityType}/{entityId}/stats` endpoint — no new endpoint,
no new UI request. `EntityReplayService.getEntityStats` gains a two-stage
file-count computation (cheap catalog-metadata prune via DuckLake's internal
`ducklake_file_column_stats`, then an exact verify via `read_parquet(...,
filename => true)`), plus pure-string directory/console-URL construction. Both
the prune stage and the exact-verify's *file-discovery* step degrade
gracefully to `fileCount = 0` if DuckLake's internal machinery isn't available
(e.g. unit tests against a plain in-memory DuckDB) — a genuine I/O failure
during the exact-verify read still propagates normally so the existing
`withObjectStoreRetry` wrapper retries it.

**Tech Stack:** Java 25 / Spring Boot 4, DuckDB JDBC + DuckLake, jOOQ, React +
TanStack Query (UI).

## Global Constraints

- No new REST endpoint and no new UI network request — everything rides on
  the existing `statsQuery` the entity page already blocks on.
- `EntityStats` is a Java record — every field addition is a compile-time-
  enforced change; there is exactly one production construction call site
  (`EntityReplayService.getEntityStats`) and three test call sites for the
  `EntityReplayService` constructor itself (`BatchReplayTest.java`,
  `EntityReplayServiceConcurrencyTest.java`, `EntityReplayServiceTest.java`).
- DuckLake's internal metadata tables (`ducklake_table`, `ducklake_column`,
  `ducklake_file_column_stats`, `ducklake_data_file`), reachable via the
  `__ducklake_metadata_lake` database in the same session as `ATTACH
  'ducklake:...' AS lake`, are not a documented app-facing API — every query
  against them is wrapped in try/catch with a fallback, never left to throw
  uncaught.
- The plain in-memory `DuckDBTestSupport` unit-test harness (`ATTACH
  ':memory:' AS lake` — no `ducklake:` prefix, no real DuckLake catalog) means
  none of the DuckLake-specific functions/tables used here exist in unit
  tests. The file-count logic must degrade to `0` there without throwing;
  real numeric correctness is proven only by a dedicated Testcontainers/MinIO
  IT test (Task 3), matching the existing `CompactionSnapshotCleanupIT`
  pattern.
- Config POJOs in this codebase (`JoxetteProperties.ObjectStore`, `.S3`, etc.)
  have no dedicated unit tests anywhere in the repo (verified by search) —
  follow that precedent for the new `StorageConsole` class; its real behavior
  is tested through `EntityReplayService`, where the logic actually lives.

---

### Task 1: `JoxetteProperties` — `StorageConsole` config class

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/config/JoxetteProperties.java`

**Interfaces:**
- Produces: `JoxetteProperties.getStorageConsole().getUrlTemplate()` — nullable
  `String`, e.g. `"http://localhost:9001/rustfs/console/browser/?bucket={bucket}&key={prefix}"`.
  Consumed by Task 3's `EntityReplayService.computeStorageConsoleUrl`.

- [ ] **Step 1: Add the `StorageConsole` nested class**

Insert immediately after the existing `ObjectStore` class (ends at line 989,
right before the `Security` section comment at line 991):

```java
    // -----------------------------------------------------------------------
    // Storage console: optional deep-link into a storage UI (RustFS/MinIO-style)
    // -----------------------------------------------------------------------

    public static class StorageConsole {
        /**
         * URL template for deep-linking into a storage console's file browser,
         * e.g. "http://localhost:9001/rustfs/console/browser/?bucket={bucket}&key={prefix}"
         * (RustFS/MinIO-style). {bucket} and {prefix} are substituted per request
         * ({prefix} URL-encoded, including its slashes). Unset by default — when
         * null or blank, entity stats responses omit the console link and the UI
         * shows the raw s3:// path instead.
         */
        private String urlTemplate;

        public String getUrlTemplate() { return urlTemplate; }
        public void setUrlTemplate(String urlTemplate) { this.urlTemplate = urlTemplate; }
    }
```

- [ ] **Step 2: Add the field and root getter/setter**

Add to the field list at the top of the class (after line 25, `private
ObjectStore objectStore = new ObjectStore();`):

```java
    private StorageConsole storageConsole = new StorageConsole();
```

Add to the "Root getters/setters" section (after line 1059, the
`getObjectStore`/`setObjectStore` pair):

```java
    public StorageConsole getStorageConsole() { return storageConsole; }
    public void setStorageConsole(StorageConsole storageConsole) { this.storageConsole = storageConsole; }
```

- [ ] **Step 3: Compile-check**

Run: `mvn -pl joxette-service compile`
Expected: `BUILD SUCCESS` — this is a pure additive POJO change, nothing else
references it yet.

- [ ] **Step 4: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/config/JoxetteProperties.java
git commit -m "feat(config): add joxette.storage-console.url-template"
```

---

### Task 2: `EntityReplayService` — wire `JoxetteProperties` in, extend records (no behavior yet)

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/replay/EntityReplayService.java`
- Modify: `joxette-service/src/main/java/com/joxette/replay/EntityStats.java`
- Modify: `joxette-service/src/test/java/com/joxette/replay/BatchReplayTest.java:89`
- Modify: `joxette-service/src/test/java/com/joxette/replay/EntityReplayServiceConcurrencyTest.java:57`
- Modify: `joxette-service/src/test/java/com/joxette/replay/EntityReplayServiceTest.java:54`

**Interfaces:**
- Consumes: `JoxetteProperties.getStorageConsole().getUrlTemplate()` and
  `JoxetteProperties.getCatalog().getObjectStoragePath()` (Task 1; the latter
  already exists).
- Produces: `EntityReplayService(DSLContext dsl, Connection duckDB,
  JoxetteProperties props)` — new 3-arg constructor. `EntityStats` gains `int
  fileCount, String objectStoreDirectory, String storageConsoleUrl` as its
  last three fields. `StatsQueryResult` (private record) gains `int
  fileCount` as its last field. These are the exact names/types Task 3's
  logic populates.

This task is purely mechanical scaffolding — no new behavior, just wiring so
the codebase compiles with the new shapes. It ends with all *existing* tests
still green (a regression check), not a new-feature test; Task 3 is where the
real logic and its tests land.

- [ ] **Step 1: Add the `JoxetteProperties` field and constructor param**

In `EntityReplayService.java`, add the import (alongside the existing
`com.joxette.replay.transform.*` imports at lines 13-16):

```java
import com.joxette.config.JoxetteProperties;
```

Change lines 114-120 from:

```java
    private final DSLContext dsl;
    private final Connection duckDB;

    public EntityReplayService(DSLContext dsl, Connection duckDB) {
        this.dsl = dsl;
        this.duckDB = duckDB;
    }
```

to:

```java
    private final DSLContext dsl;
    private final Connection duckDB;
    private final JoxetteProperties props;

    public EntityReplayService(DSLContext dsl, Connection duckDB, JoxetteProperties props) {
        this.dsl = dsl;
        this.duckDB = duckDB;
        this.props = props;
    }
```

- [ ] **Step 2: Add a logger**

Add imports (alongside `org.jooq.*` imports at the top):

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
```

Add as the first field of the class, right after the `SAFE_IDENTIFIER`/
`STREAM_PAGE_SIZE` constants (after line 58):

```java
    private static final Logger log = LoggerFactory.getLogger(EntityReplayService.class);
```

- [ ] **Step 3: Extend `StatsQueryResult` and `EntityStats`**

Change line 110-112 from:

```java
    private record StatsQueryResult(
            long count, Instant firstMsg, Instant lastMsg,
            Map<String, Long> countByTopic, Instant firstSeen, Instant lastSeen) {}
```

to:

```java
    private record StatsQueryResult(
            long count, Instant firstMsg, Instant lastMsg,
            Map<String, Long> countByTopic, Instant firstSeen, Instant lastSeen,
            int fileCount) {}
```

In `EntityStats.java`, change the record declaration (lines 27-56) to add the
three new fields after `countByTopic`:

```java
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
        Map<String, Long> countByTopic,

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

Also update the class-level `@Schema(example = ...)` JSON block at lines
12-25 to include the three new fields, for documentation accuracy:

```java
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
              },
              "fileCount": 3,
              "objectStoreDirectory": "s3://joxette-data/main/entity_customer/",
              "storageConsoleUrl": "http://localhost:9001/rustfs/console/browser/?bucket=joxette-data&key=main%2Fentity_customer%2F"
            }""")
```

- [ ] **Step 4: Fix the production construction call site (temporary placeholder values)**

In `EntityReplayService.java`, change lines 695 and 705-706 from:

```java
            return new StatsQueryResult(count, firstMsg, lastMsg, countByTopic, firstSeen, lastSeen);
```

```java
        return new EntityStats(entityType, entityId, count, firstMsg, lastMsg,
                firstSeen, lastSeen, countByTopic);
```

to (temporary — Task 3 replaces the `0`/`null`/`null` with real computation):

```java
            return new StatsQueryResult(count, firstMsg, lastMsg, countByTopic, firstSeen, lastSeen, 0);
```

```java
        return new EntityStats(entityType, entityId, count, firstMsg, lastMsg,
                firstSeen, lastSeen, countByTopic, result.fileCount(), null, null);
```

(`result.fileCount()` is available since `result` is already destructured at
this point in the method — it will be `0` until Task 3 computes it for real.)

- [ ] **Step 5: Fix the three test constructor call sites**

`BatchReplayTest.java:89`, change:
```java
                new EntityReplayService(DSL.using(conn, SQLDialect.DUCKDB), conn);
```
to:
```java
                new EntityReplayService(DSL.using(conn, SQLDialect.DUCKDB), conn, new com.joxette.config.JoxetteProperties());
```

`EntityReplayServiceConcurrencyTest.java:57`, change:
```java
        service = new EntityReplayService(DSL.using(duckDB, SQLDialect.DUCKDB), duckDB);
```
to:
```java
        service = new EntityReplayService(DSL.using(duckDB, SQLDialect.DUCKDB), duckDB, new com.joxette.config.JoxetteProperties());
```

`EntityReplayServiceTest.java:54`, change:
```java
        service = new EntityReplayService(DSL.using(duckDB, SQLDialect.DUCKDB), duckDB);
```
to:
```java
        service = new EntityReplayService(DSL.using(duckDB, SQLDialect.DUCKDB), duckDB, new com.joxette.config.JoxetteProperties());
```

- [ ] **Step 6: Run the full existing test suite for these files — confirm no regression**

Run: `mvn -pl joxette-service test -Dtest=EntityReplayServiceTest,EntityReplayServiceConcurrencyTest,BatchReplayTest`
Expected: all previously-passing tests still pass (`Failures: 0, Errors: 0`).
This is the regression gate for the mechanical scaffolding — no new
assertions yet, since `fileCount`/`objectStoreDirectory`/`storageConsoleUrl`
aren't asserted on by any existing test.

- [ ] **Step 7: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/replay/EntityReplayService.java \
        joxette-service/src/main/java/com/joxette/replay/EntityStats.java \
        joxette-service/src/test/java/com/joxette/replay/BatchReplayTest.java \
        joxette-service/src/test/java/com/joxette/replay/EntityReplayServiceConcurrencyTest.java \
        joxette-service/src/test/java/com/joxette/replay/EntityReplayServiceTest.java
git commit -m "refactor(replay): wire JoxetteProperties into EntityReplayService, extend EntityStats shape"
```

---

### Task 3: Two-stage file-count logic + directory/console-URL + tests

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/replay/EntityReplayService.java`
- Modify: `joxette-service/src/test/java/com/joxette/replay/EntityReplayServiceTest.java`
- Create: `joxette-service/src/test/java/com/joxette/it/EntityFileCountIT.java`

**Interfaces:**
- Consumes: everything from Tasks 1-2.
- Produces: the real values behind `EntityStats.fileCount`,
  `.objectStoreDirectory`, `.storageConsoleUrl` — this is the task later work
  (Task 4, UI) reads from.

This is the core logic task. The two SQL queries below (catalog-metadata
prune, and the `read_parquet(..., filename => true)` verify) were both
empirically verified against a real local DuckLake catalog before writing
this plan — they are not guesses.

- [ ] **Step 1: Write the failing unit tests**

Add to `EntityReplayServiceTest.java`, in a new section after the existing
`getEntityStats_*` tests (after the `getEntityStats_concurrentCallsDoNotCollide`
test, i.e. after line ~394 — find the end of that test method and insert
after its closing brace):

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

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -pl joxette-service test -Dtest=EntityReplayServiceTest#getEntityStats_fileCountIsZero_whenObjectStoragePathNotConfigured+getEntityStats_computesDirectoryAndConsoleUrl_evenWhenFileCountUnavailable`
Expected: FAIL — both assert on values that are currently hardcoded to `0`/
`null`/`null` unconditionally (Task 2's placeholder), so the first test
actually already passes trivially (fileCount is always 0 right now) but the
second fails on `objectStoreDirectory`/`storageConsoleUrl` both being `null`
instead of the computed strings. That's expected — the first test exists to
pin the "unconfigured" behavior going forward, the second is the one proving
real logic is missing.

- [ ] **Step 3: Implement the directory/console-URL helpers**

Add to `EntityReplayService.java`, right after `getEntityStats` (after its
closing brace, before the `// Record mappers` section comment):

```java
    // -------------------------------------------------------------------------
    // Entity stats: object-store location
    // -------------------------------------------------------------------------

    private String computeObjectStoreDirectory(String table) {
        String objectStoragePath = props.getCatalog().getObjectStoragePath();
        if (objectStoragePath == null || objectStoragePath.isBlank()) return null;
        String base = objectStoragePath.endsWith("/") ? objectStoragePath : objectStoragePath + "/";
        return base + "main/" + table + "/";
    }

    private String computeStorageConsoleUrl(String table) {
        String objectStoragePath = props.getCatalog().getObjectStoragePath();
        String urlTemplate = props.getStorageConsole().getUrlTemplate();
        if (objectStoragePath == null || objectStoragePath.isBlank()
                || urlTemplate == null || urlTemplate.isBlank()) {
            return null;
        }
        String noScheme = objectStoragePath.replaceFirst("^s3://", "");
        int slash = noScheme.indexOf('/');
        String bucket = slash < 0 ? noScheme : noScheme.substring(0, slash);
        String rest = slash < 0 ? "" : noScheme.substring(slash + 1);
        if (!rest.isEmpty() && !rest.endsWith("/")) rest = rest + "/";
        String prefix = rest + "main/" + table + "/";
        String encodedPrefix = java.net.URLEncoder.encode(prefix, java.nio.charset.StandardCharsets.UTF_8);
        return urlTemplate.replace("{bucket}", bucket).replace("{prefix}", encodedPrefix);
    }
```

Wire them into `getEntityStats` — change the final unpacking block (Task 2's
Step 4 result) from:

```java
        return new EntityStats(entityType, entityId, count, firstMsg, lastMsg,
                firstSeen, lastSeen, countByTopic, result.fileCount(), null, null);
```

to:

```java
        String objectStoreDirectory = computeObjectStoreDirectory(bareTable);
        String storageConsoleUrl = computeStorageConsoleUrl(bareTable);

        return new EntityStats(entityType, entityId, count, firstMsg, lastMsg,
                firstSeen, lastSeen, countByTopic, result.fileCount(),
                objectStoreDirectory, storageConsoleUrl);
```

This references a `bareTable` local that doesn't exist yet — add it right
after the existing `String tableName = "lake.main.entity_" + entityType;`
line (line 641):

```java
        String tableName = "lake.main.entity_" + entityType;
        String bareTable = "entity_" + entityType;
```

- [ ] **Step 4: Run the tests again — first two should now pass**

Run: `mvn -pl joxette-service test -Dtest=EntityReplayServiceTest#getEntityStats_fileCountIsZero_whenObjectStoragePathNotConfigured+getEntityStats_computesDirectoryAndConsoleUrl_evenWhenFileCountUnavailable`
Expected: PASS — `fileCount` is still always `0` (file-count logic isn't
wired up yet), but `objectStoreDirectory`/`storageConsoleUrl` now compute
correctly.

- [ ] **Step 5: Implement the two-stage file-count logic**

Add to `EntityReplayService.java`, right after the two helpers from Step 3:

```java
    // -------------------------------------------------------------------------
    // Entity stats: file count (two-stage catalog-prune + verify)
    // -------------------------------------------------------------------------

    /**
     * Exact count of physical Parquet files containing at least one row for
     * {@code entityId}. Stage 1 prunes candidates via DuckLake's internal
     * per-file column statistics (cheap, catalog-only); stage 2 verifies the
     * survivors with a targeted read. Falls back to verifying every file for
     * the table if stage 1's internal-metadata query fails (not a stable
     * app-facing API); reports 0 if DuckLake's own {@code ducklake_list_files}
     * is unavailable at all (e.g. not a real DuckLake-backed catalog).
     */
    private int countEntityFiles(String table, String entityId) throws SQLException {
        String objectStoragePath = props.getCatalog().getObjectStoragePath();
        if (objectStoragePath == null || objectStoragePath.isBlank()) return 0;

        List<String> candidates = pruneCandidateFiles(table, entityId);
        if (candidates.isEmpty()) return 0;
        return verifyCandidates(candidates, entityId);
    }

    private List<String> pruneCandidateFiles(String table, String entityId) {
        List<String> allFiles;
        try {
            allFiles = listAllFiles(table);
        } catch (SQLException e) {
            log.debug("countEntityFiles: ducklake_list_files unavailable for table '{}' ({}); reporting 0 files",
                    table, e.getMessage());
            return List.of();
        }
        if (allFiles.isEmpty()) return List.of();

        java.util.Set<String> candidateFilenames;
        try {
            candidateFilenames = candidateFilenamesViaColumnStats(table, entityId);
        } catch (SQLException e) {
            log.debug("countEntityFiles: catalog column-stats prune unavailable for table '{}' ({}); " +
                    "verifying all {} file(s)", table, e.getMessage(), allFiles.size());
            return allFiles;
        }

        List<String> filtered = new ArrayList<>();
        for (String path : allFiles) {
            String basename = path.substring(path.lastIndexOf('/') + 1);
            if (candidateFilenames.contains(basename)) filtered.add(path);
        }
        return filtered;
    }

    /** Full, fully-resolved file list for {@code table} via DuckLake's own function. */
    private List<String> listAllFiles(String table) throws SQLException {
        List<String> files = new ArrayList<>();
        try (PreparedStatement ps = duckDB.prepareStatement(
                "SELECT data_file FROM ducklake_list_files('lake', ?)")) {
            ps.setString(1, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) files.add(rs.getString(1));
            }
        }
        return files;
    }

    /**
     * Bare filenames (not full paths) of files whose entity_id column
     * min/max range could include {@code entityId}, per DuckLake's internal
     * per-file column statistics. Zone-map pruning: a file surviving this
     * filter isn't guaranteed to actually contain the value, only that it
     * isn't excluded by range — {@link #verifyCandidates} does the exact check.
     */
    private java.util.Set<String> candidateFilenamesViaColumnStats(String table, String entityId) throws SQLException {
        String sql =
                "SELECT df.path "
              + "FROM __ducklake_metadata_lake.ducklake_file_column_stats fcs "
              + "JOIN __ducklake_metadata_lake.ducklake_data_file df USING (data_file_id) "
              + "JOIN __ducklake_metadata_lake.ducklake_table t ON t.table_id = fcs.table_id "
              + "JOIN __ducklake_metadata_lake.ducklake_column c "
              + "  ON c.table_id = fcs.table_id AND c.column_id = fcs.column_id "
              + "WHERE t.table_name = ? AND t.end_snapshot IS NULL "
              + "  AND c.column_name = 'entity_id' AND c.end_snapshot IS NULL "
              + "  AND (? >= fcs.min_value OR fcs.min_value IS NULL) "
              + "  AND (? <= fcs.max_value OR fcs.max_value IS NULL)";
        java.util.Set<String> filenames = new java.util.HashSet<>();
        try (PreparedStatement ps = duckDB.prepareStatement(sql)) {
            ps.setString(1, table);
            ps.setString(2, entityId);
            ps.setString(3, entityId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String p = rs.getString(1);
                    filenames.add(p.substring(p.lastIndexOf('/') + 1));
                }
            }
        }
        return filenames;
    }

    /** Exact count: reads only {@code candidates}, filtered to real entity_id matches. */
    private int verifyCandidates(List<String> candidates, String entityId) throws SQLException {
        String filesLiteral = candidates.stream()
                .map(p -> "'" + p.replace("'", "''") + "'")
                .collect(java.util.stream.Collectors.joining(", "));
        String sql = "SELECT COUNT(DISTINCT filename) FROM read_parquet([" + filesLiteral
                + "], filename => true) WHERE entity_id = ?";
        try (PreparedStatement ps = duckDB.prepareStatement(sql)) {
            ps.setString(1, entityId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
    }
```

`ArrayList` and `List` are already imported (lines 25 and 28). Add the two
that aren't, alongside the existing `java.sql.Connection` / `java.sql.SQLException`
imports at lines 19-20:

```java
import java.sql.PreparedStatement;
import java.sql.ResultSet;
```

**Important — checked vs. unchecked exceptions:** `withObjectStoreRetry` takes
a `Supplier<T>` (see `TopicReplayService.java:493`), whose `get()` cannot
declare or propagate a checked exception. The existing code inside the lambda
only calls jOOQ methods, which throw unchecked `DataAccessException` — that's
why it compiles today. `countEntityFiles` throws checked `SQLException`
(raw JDBC), so it must be caught and rewrapped as `DataAccessException`
*before* it can cross the lambda boundary — and wrapping it that way is also
what makes a genuine transient object-store failure during
`verifyCandidates`'s `read_parquet` read get picked up by
`withObjectStoreRetry`'s existing retry logic (`isTransientObjectStoreError`
inspects the cause chain for an "IO Error" substring — a wrapped `SQLException`
with that text in its message still matches).

Wire it into `getEntityStats` — inside the `synchronized (duckDB)` block,
right after the existing `known_entities` query (after line 693, the closing
`}` of the `if (regRecord != null)` block, still inside `synchronized`):

```java
                int fileCount;
                try {
                    fileCount = countEntityFiles(bareTable, entityId);
                } catch (SQLException e) {
                    throw new DataAccessException("countEntityFiles failed for " + bareTable, e);
                }
```

And change the `return new StatsQueryResult(...)` line (694→695) from:

```java
            return new StatsQueryResult(count, firstMsg, lastMsg, countByTopic, firstSeen, lastSeen, 0);
```

to:

```java
            return new StatsQueryResult(count, firstMsg, lastMsg, countByTopic, firstSeen, lastSeen, fileCount);
```

(`bareTable` needs to be visible inside the `withObjectStoreRetry` lambda —
it's declared as a method-local variable before the lambda per Step 3 above,
and Java lambdas can capture effectively-final locals, so no change needed
there as long as `bareTable` itself is never reassigned.)

- [ ] **Step 6: Compile and run the full `EntityReplayServiceTest` class**

Run: `mvn -pl joxette-service test -Dtest=EntityReplayServiceTest`
Expected: all tests pass, including the two from Step 1 (fileCount stays 0 in
this fake-catalog harness, exactly as asserted — `pruneCandidateFiles` hits
the `listAllFiles` `SQLException` catch since `ducklake_list_files` isn't a
real function against `:memory:`-attached `lake`).

- [ ] **Step 7: Write the real-catalog IT test proving numeric correctness**

Create `joxette-service/src/test/java/com/joxette/it/EntityFileCountIT.java`,
mirroring `CompactionSnapshotCleanupIT`'s structure exactly (same MinIO
container setup, same DDL matching `SchemaManager.createEntityCassetteTable`'s
real column types):

```java
package com.joxette.it;

import com.joxette.replay.EntityReplayService;
import com.joxette.replay.EntityStats;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the two-stage catalog-prune-then-verify file count is numerically
 * exact: entity A's count must include only files that actually contain A's
 * rows (an A-only file and a mixed A+B file), never the B-only file — a test
 * that only checked "count > 0" would pass even if the query counted every
 * file in the table.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class EntityFileCountIT {

    private static final String BUCKET = "joxette-filecount-test";
    private static final String ENTITY_TYPE = "filecounttest";

    @Container
    static final MinIOContainer minio =
            new MinIOContainer(DockerImageName.parse("minio/minio:RELEASE.2024-01-16T16-07-38Z"));

    @DynamicPropertySource
    static void minioProperties(DynamicPropertyRegistry registry) {
        String s3Url = minio.getS3URL();
        String userName = minio.getUserName();
        String password = minio.getPassword();
        try (S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create(s3Url))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(userName, password)))
                .region(Region.US_EAST_1)
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build()) {
            s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        }
        registry.add("joxette.catalog.object-storage-path", () -> "s3://" + BUCKET + "/data/");
        registry.add("joxette.s3.endpoint",   () -> s3Url);
        registry.add("joxette.s3.access-key", () -> userName);
        registry.add("joxette.s3.secret-key", () -> password);
        registry.add("joxette.storage-console.url-template",
                () -> "http://localhost:9001/rustfs/console/browser/?bucket={bucket}&key={prefix}");
    }

    @Autowired private Connection duckDB;
    @Autowired private EntityReplayService entityReplayService;

    @Test
    void getEntityStats_fileCount_countsOnlyFilesContainingThatEntity() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS lake.main.entity_" + ENTITY_TYPE + " (" +
                    "recorded_at TIMESTAMPTZ, entity_id VARCHAR, bucket INTEGER, message_type VARCHAR, " +
                    "topic VARCHAR, kafka_offset BIGINT, kafka_partition INTEGER, kafka_timestamp TIMESTAMPTZ, " +
                    "kafka_key BLOB, kafka_value BLOB, metadata VARCHAR, " +
                    "headers STRUCT(key VARCHAR, value VARCHAR)[])");
        }

        Instant ts = Instant.parse("2020-01-01T00:00:00Z");
        insertAndFlush("A", 0);              // file 1: A-only
        insertAndFlush("B", 1);              // file 2: B-only
        insertBothAndFlush(ts, 2, 3);        // file 3: mixed A+B

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
    }

    private void insertAndFlush(String entityId, long offset) throws Exception {
        Instant ts = Instant.parse("2020-01-01T00:00:00Z");
        try (PreparedStatement ps = duckDB.prepareStatement(
                "INSERT INTO lake.main.entity_" + ENTITY_TYPE +
                " (recorded_at, entity_id, bucket, message_type, topic, kafka_offset, kafka_partition, " +
                "  kafka_timestamp, kafka_key, kafka_value, metadata, headers) " +
                "VALUES (?, ?, 0, 'order', 'orders.events', ?, 0, ?, ?, ?, NULL, [])")) {
            ps.setObject(1, java.sql.Timestamp.from(ts));
            ps.setString(2, entityId);
            ps.setLong(3, offset);
            ps.setObject(4, java.sql.Timestamp.from(ts));
            ps.setBytes(5, ("k" + offset).getBytes());
            ps.setBytes(6, ("v" + offset).getBytes());
            ps.executeUpdate();
        }
        try (Statement st = duckDB.createStatement()) {
            st.execute("CALL ducklake_flush_inlined_data('lake')");
        }
    }

    private void insertBothAndFlush(Instant ts, long offsetA, long offsetB) throws Exception {
        try (PreparedStatement ps = duckDB.prepareStatement(
                "INSERT INTO lake.main.entity_" + ENTITY_TYPE +
                " (recorded_at, entity_id, bucket, message_type, topic, kafka_offset, kafka_partition, " +
                "  kafka_timestamp, kafka_key, kafka_value, metadata, headers) " +
                "VALUES (?, ?, 0, 'order', 'orders.events', ?, 0, ?, ?, ?, NULL, [])")) {
            ps.setObject(1, java.sql.Timestamp.from(ts));
            ps.setString(2, "A");
            ps.setLong(3, offsetA);
            ps.setObject(4, java.sql.Timestamp.from(ts));
            ps.setBytes(5, ("k" + offsetA).getBytes());
            ps.setBytes(6, ("v" + offsetA).getBytes());
            ps.executeUpdate();

            ps.setObject(1, java.sql.Timestamp.from(ts));
            ps.setString(2, "B");
            ps.setLong(3, offsetB);
            ps.setObject(4, java.sql.Timestamp.from(ts));
            ps.setBytes(5, ("k" + offsetB).getBytes());
            ps.setBytes(6, ("v" + offsetB).getBytes());
            ps.executeUpdate();
        }
        try (Statement st = duckDB.createStatement()) {
            st.execute("CALL ducklake_flush_inlined_data('lake')");
        }
    }
}
```

- [ ] **Step 8: Run it, verify it passes**

Run: `mvn -pl joxette-service test -Dtest=EntityFileCountIT -Dsurefire.failIfNoSpecifiedTests=false`

This spins up a real Spring context + MinIO Testcontainer (~15-30s startup) —
when running in the background, poll with `pgrep -f surefirebooter` and wait
for it to genuinely exit before trusting any completion notification or
reading the surefire report (this environment has repeatedly fired
"completed" notifications before the process actually finished in prior work
on this codebase).

Expected: `Tests run: 1, Failures: 0, Errors: 0` — `fileCount` is exactly `2`
for both entities (not 3, which would mean the prune+verify degenerated to
"every file in the table"; not 1, which would mean it's missing the mixed
file). If it fails, root-cause it — the join/filter logic or the
`__ducklake_metadata_lake` addressing is the most likely culprit, not the
test's expectations. Fix forward; don't weaken the assertion.

**Deviation from the design spec, noted here deliberately:** the spec asked
for a dedicated test that forces the stage-1 metadata-table query to fail and
asserts the stage-2-over-all-files fallback still returns a correct count.
That specific branch (stage 1 throws, stage 2 succeeds) turns out to have no
clean way to trigger deterministically: it requires a real DuckLake catalog
(so `ducklake_list_files` succeeds) where the internal metadata-table query
*specifically* fails — the only way to force that without a real DuckLake
version mismatch is adding a test-only seam to `pruneCandidateFiles` (e.g. a
`forceStage1Failure` flag), which is exactly the "test-only methods on
production classes" anti-pattern the TDD skill warns against. This branch's
correctness instead rests on: (a) `verifyCandidates` being independently
proven exact for *any* candidate list, per Step 8's IT test using the real
pruned list — a broken stage 1 can only ever widen the candidate set (hurting
performance), never corrupt the count, since stage 2 re-verifies every
candidate regardless of source; (b) Step 1's unit tests, which do exercise
"both stages fail" (the fake in-memory catalog fails `ducklake_list_files`
too), proving the outer degrade-to-0 path. If this asymmetry bothers you
during review, the alternative is accepting the test-only-seam tradeoff
explicitly — flag it and we'll revisit.

- [ ] **Step 9: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/replay/EntityReplayService.java \
        joxette-service/src/main/java/com/joxette/replay/EntityStats.java \
        joxette-service/src/test/java/com/joxette/replay/EntityReplayServiceTest.java \
        joxette-service/src/test/java/com/joxette/it/EntityFileCountIT.java
git commit -m "feat(replay): compute exact per-entity file count via catalog-prune + verify"
```

---

### Task 4: UI — Stats card gets file count + storage location

**Files:**
- Modify: `ui/src/api/client.ts`
- Modify: `ui/src/routes/entities/$entityType/$entityId.tsx`

**Interfaces:**
- Consumes: `EntityStats.fileCount` (number), `.objectStoreDirectory` (string
  or null), `.storageConsoleUrl` (string or null) from Task 3, arriving on the
  page's existing `statsQuery` (no new request).

- [ ] **Step 1: Extend the `EntityStats` TypeScript interface**

In `ui/src/api/client.ts`, change lines 191-200 from:

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
  fileCount: number
  objectStoreDirectory: string | null
  storageConsoleUrl: string | null
}
```

- [ ] **Step 2: Add the two new rows to the entity page's Stats card**

In `ui/src/routes/entities/$entityType/$entityId.tsx`, change the stat-tiles
array (lines 379-385) from:

```tsx
            {[
              ['Messages', stats.messageCount.toLocaleString()],
              ['First Message', stats.firstMessage?.slice(0, 19).replace('T', ' ') ?? '—'],
              ['Last Message', stats.lastMessage?.slice(0, 19).replace('T', ' ') ?? '—'],
              ['First Seen', stats.firstSeen?.slice(0, 19).replace('T', ' ') ?? '—'],
              ['Last Seen', stats.lastSeen?.slice(0, 19).replace('T', ' ') ?? '—'],
            ].map(([k, v]) => (
```

to:

```tsx
            {[
              ['Messages', stats.messageCount.toLocaleString()],
              ['First Message', stats.firstMessage?.slice(0, 19).replace('T', ' ') ?? '—'],
              ['Last Message', stats.lastMessage?.slice(0, 19).replace('T', ' ') ?? '—'],
              ['First Seen', stats.firstSeen?.slice(0, 19).replace('T', ' ') ?? '—'],
              ['Last Seen', stats.lastSeen?.slice(0, 19).replace('T', ' ') ?? '—'],
              ['Object Store Files', stats.fileCount.toLocaleString()],
            ].map(([k, v]) => (
```

Then add a "Storage Location" block right after that grid's closing `))}\n
</div>` (after line 391, before the `{Object.keys(stats.countByTopic).length
> 0 && (` block on line 392):

```tsx
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

- [ ] **Step 3: Type-check**

Run: `cd ui && pnpm exec tsc --noEmit`
Expected: no new errors attributable to `$entityId.tsx` or `client.ts` (the
repo has pre-existing, unrelated `SunburstChart.tsx` type errors — confirmed
present on `main` independent of this change; don't try to fix those here).

- [ ] **Step 4: Browser-verify against a live backend**

Start the backend from a worktree checkout (`mvn -pl joxette-service
spring-boot:run`) and the UI dev server (`pnpm dev --port <free-port>`) — the
existing `joxette-kafka`/`joxette-rustfs` Docker containers are already
running and must not be restarted or killed. Navigate to an entity page for
an entity type with real recorded data (e.g. `fixture`), confirm:
- "Object Store Files" tile renders a number without error.
- "Storage Location" renders either a clickable link (if
  `joxette.storage-console.url-template` is set) or the raw `s3://` path as
  plain text (if not) — test both by toggling the config and restarting.
- No console errors in the browser.

Stop the verification backend/UI-dev-server processes afterward (they are
throwaway verification instances, not the user's own long-running dev setup)
— do not touch the Docker containers.

- [ ] **Step 5: Commit**

```bash
git add ui/src/api/client.ts ui/src/routes/entities/\$entityType/\$entityId.tsx
git commit -m "feat(ui/entities): show object-store file count and storage location on entity page"
```
