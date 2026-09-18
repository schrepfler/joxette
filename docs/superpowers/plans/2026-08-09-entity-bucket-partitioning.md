# Entity Bucket Partitioning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Physically partition entity cassette tables by `bucket` (real
`bucket=N/` object-store subdirectories), migrate pre-existing flat files
into that layout via compaction, and replace the slow catalog-scan file-count
fallback with a fast per-bucket glob once a table is migrated.

**Architecture:** `SchemaManager` declares partitioning on every entity table
at every startup (idempotent). `CompactionService` detects and one-time-
migrates any table with leftover unpartitioned files, before its existing
merge step. `EntityReplayService`'s file-count logic gets a fast path that
skips the catalog-metadata prune entirely once a table has no pending legacy
files, globbing the entity's own bucket directory instead. A new shared
`SchemaManager.hasUnpartitionedFiles(Connection, String)` static utility
(mirroring the existing cross-package `SchemaManager.validateEntityType`
pattern) backs both the compaction migration's detection and the file-count
fast path's gating check, so the query is defined once.

**Tech Stack:** Java 25 / Spring Boot 4, DuckDB JDBC + DuckLake, jOOQ,
Testcontainers (MinIO).

## Global Constraints

- **Column name correction from the design spec**: `entity_type_configs`'
  real bucket-count column is `bucket_count`, not `buckets` (confirmed
  against three separate DDL definitions — `SchemaManager.java`, `db/init.sql`,
  `db/jooq-codegen-schema.sql` — all agree; `CLAUDE.md` itself is wrong on
  this point, describing a column literally named `buckets`, which doesn't
  exist. `buckets` is only ever a `SELECT ... AS buckets` alias or the
  `EntityTypeConfig.buckets()` record accessor name, never a real column.
  Every raw SQL statement in this plan uses `bucket_count`.
- `CompactionService` hardcodes the catalog name as the literal string
  `"lake"` throughout (no `catalog` field/parameter exists in that class) —
  new code added there follows that convention. `SchemaManager` instead
  always threads a `catalog` parameter — new code added there follows
  *that* convention. These two files are inconsistent with each other by
  long-standing precedent; match whichever file you're editing, don't try to
  unify them as part of this plan.
- `entityType` is validated (`[a-z][a-z0-9_]*`) before reaching any of the
  new code in this plan (`CompactionService.doCompactEntityType` calls
  `SchemaManager.validateEntityType(entityType)` at its top; `EntityReplayService
  .getEntityStats` calls its own local `validateEntityType(entityType)` at
  its top) — safe to interpolate directly into SQL/temp-table names in both
  files, consistent with how `"entity_" + entityType` is already built
  unguarded elsewhere in both classes.
- The fast/fake `DuckDBTestSupport` unit-test harness (`ATTACH ':memory:' AS
  lake`) has no `ducklake` extension loaded and no `__ducklake_metadata_lake`
  tables — confirmed by this session's prior work and by
  `CompactionServiceTest`'s own class javadoc. Every new query against
  DuckLake-internal metadata or `ducklake_merge_adjacent_files`/`SET
  PARTITIONED BY` will fail against this harness; that failure path is
  itself what several unit tests below assert on (graceful degradation), and
  real correctness is proven only by the new Testcontainers/MinIO IT test in
  Task 4.

---

### Task 1: `SchemaManager.ensureTablePartitioned`

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/db/SchemaManager.java`
- Create: `joxette-service/src/test/java/com/joxette/db/SchemaManagerPartitionedTablesTest.java`

**Interfaces:**
- Produces: `static void SchemaManager.ensureTablePartitioned(Connection conn, String catalog, String tableName)` —
  package-private static, mirroring the existing `ensureTableSorted` exactly.
  Called from `migrateEntityCassetteTables`'s existing per-table loop.

- [ ] **Step 1: Write the failing tests**

Create `joxette-service/src/test/java/com/joxette/db/SchemaManagerPartitionedTablesTest.java`,
mirroring the existing `SchemaManagerSortedTablesTest.java` structure exactly
(same package, same `DuckDBTestSupport` setup/teardown, same Mockito-based
SQL-content verification pattern):

```java
package com.joxette.db;

import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SchemaManager#ensureTablePartitioned}.
 *
 * <p>Plain DuckDB (the {@link DuckDBTestSupport} harness) does not support
 * the {@code SET PARTITIONED BY} DuckLake extension, so every call through
 * the real connection exercises the warn-and-swallow path — this mirrors
 * {@link SchemaManagerSortedTablesTest}'s approach for {@code ensureTableSorted}.
 */
class SchemaManagerPartitionedTablesTest {

    private Connection conn;

    @BeforeEach
    void setUp() throws Exception {
        conn = DuckDBTestSupport.newConnection();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    @Test
    void entityCassette_firstCall_doesNotThrow() throws Exception {
        DuckDBTestSupport.createEntityTable(conn, "order");

        assertThatCode(() ->
            SchemaManager.ensureTablePartitioned(conn, "lake", "entity_order"))
                .doesNotThrowAnyException();
    }

    @Test
    void entityCassette_repeatedCall_isIdempotent() throws Exception {
        DuckDBTestSupport.createEntityTable(conn, "order");
        SchemaManager.ensureTablePartitioned(conn, "lake", "entity_order");

        assertThatCode(() ->
            SchemaManager.ensureTablePartitioned(conn, "lake", "entity_order"))
                .doesNotThrowAnyException();
    }

    @Test
    void nonExistentTable_doesNotThrow() {
        assertThatCode(() ->
            SchemaManager.ensureTablePartitioned(conn, "lake", "entity_no_such_table"))
                .doesNotThrowAnyException();
    }

    @Test
    void entityCassette_sqlContainsBucketColumn() throws Exception {
        Statement mockStmt = mock(Statement.class);
        Connection mockConn = mock(Connection.class);
        when(mockConn.createStatement()).thenReturn(mockStmt);

        SchemaManager.ensureTablePartitioned(mockConn, "lake", "entity_order");

        verify(mockStmt).execute(
                "ALTER TABLE lake.main.entity_order SET PARTITIONED BY (bucket)");
    }

    @Test
    void sqlExceptionFromDriver_isSwallowedNotPropagated() throws Exception {
        Statement mockStmt = mock(Statement.class);
        Connection mockConn = mock(Connection.class);
        when(mockConn.createStatement()).thenReturn(mockStmt);
        when(mockStmt.execute(org.mockito.ArgumentMatchers.anyString()))
                .thenThrow(new SQLException("Parser Error: syntax error at or near 'PARTITIONED'"));

        assertThatCode(() ->
            SchemaManager.ensureTablePartitioned(mockConn, "lake", "entity_order"))
                .doesNotThrowAnyException();
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -pl joxette-service test -Dtest=SchemaManagerPartitionedTablesTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compile failure — `SchemaManager.ensureTablePartitioned` does not
exist yet.

- [ ] **Step 3: Implement `ensureTablePartitioned`**

In `SchemaManager.java`, add immediately after `ensureTableSorted` (which
ends at line 930, right before the blank line preceding `exec` at line 932):

```java
    /**
     * Applies {@code ALTER TABLE … SET PARTITIONED BY (bucket)} so entity
     * cassette tables get real per-bucket object-store subdirectories
     * (Hive-style {@code bucket=N/}) instead of one flat directory shared by
     * every entity of that type.
     *
     * <p>Issued on every startup for every entity table, new or pre-existing
     * — safe to repeat; DuckLake treats a repeated {@code SET PARTITIONED BY}
     * with the same column as a no-op (verified empirically; see
     * {@code docs/superpowers/specs/2026-08-09-entity-bucket-partitioning-design.md}).
     *
     * <p><b>Important:</b> like {@link #ensureTableSorted}, this does
     * <em>not</em> retroactively move existing files into the new layout —
     * only files written after this call land under {@code bucket=N/}.
     * Migrating pre-existing flat files into the new layout is
     * {@code CompactionService}'s responsibility, not this method's.
     *
     * <p>Failures are logged as warnings and swallowed so that an older
     * DuckLake version that does not yet support the statement cannot
     * prevent startup.
     */
    static void ensureTablePartitioned(Connection conn, String catalog, String tableName) {
        String sql = "ALTER TABLE " + catalog + ".main." + tableName
                     + " SET PARTITIONED BY (bucket)";
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            log.debug("Bucket partitioning applied to {}.main.{}", catalog, tableName);
        } catch (SQLException e) {
            log.warn("Could not apply bucket partitioning to {}.main.{} ({}); " +
                     "this table's files will not be organized into bucket=N/ subdirectories",
                     catalog, tableName, e.getMessage());
        }
    }
```

- [ ] **Step 4: Wire it into `migrateEntityCassetteTables`**

Change `SchemaManager.java:877-879` from:

```java
        for (String tableName : tableNames) {
            dropColumnIfExists(conn, catalog + ".main." + tableName, "kafka_value_str");
        }
```

to:

```java
        for (String tableName : tableNames) {
            dropColumnIfExists(conn, catalog + ".main." + tableName, "kafka_value_str");
            ensureTablePartitioned(conn, catalog, tableName);
        }
```

- [ ] **Step 5: Run the tests, verify they pass**

Run: `mvn -pl joxette-service test -Dtest=SchemaManagerPartitionedTablesTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `Tests run: 5, Failures: 0, Errors: 0`.

- [ ] **Step 6: Run the full `SchemaManager` test suite for regressions**

Run: `mvn -pl joxette-service test -Dtest=SchemaManagerSortedTablesTest,SchemaManagerListCassetteTableNamesTest,SchemaManagerPartitionedTablesTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: all pass, no regression.

- [ ] **Step 7: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/db/SchemaManager.java \
        joxette-service/src/test/java/com/joxette/db/SchemaManagerPartitionedTablesTest.java
git commit -m "feat(db): partition entity cassette tables by bucket on every startup"
```

---

### Task 2: `SchemaManager.hasUnpartitionedFiles` + `CompactionService` migration step

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/db/SchemaManager.java`
- Modify: `joxette-service/src/main/java/com/joxette/compaction/CompactionService.java`
- Modify: `joxette-service/src/test/java/com/joxette/compaction/CompactionServiceTest.java`

**Interfaces:**
- Produces: `public static boolean SchemaManager.hasUnpartitionedFiles(Connection duckDB, String tableName) throws SQLException` —
  cross-package static utility (mirrors the existing `public static void
  SchemaManager.validateEntityType(String)` pattern, already called from both
  `CompactionService` and elsewhere). Reused unchanged by Task 3.
- Produces: `private void CompactionService.migrateLegacyPartitionFiles(String entityType) throws SQLException`,
  called from `doCompactEntityType` before its existing merge call.

- [ ] **Step 1: Write the failing test for `hasUnpartitionedFiles`'s graceful-failure path**

Add the import `import static org.assertj.core.api.Assertions.assertThatThrownBy;`
alongside the existing `assertThatCode` static import at the top of
`SchemaManagerPartitionedTablesTest.java` (from Task 1), then add this test
after the existing ones:

```java
    @Test
    void hasUnpartitionedFiles_againstPlainDuckDB_throwsSQLException() {
        // The fake harness has no __ducklake_metadata_lake tables — this proves
        // the method surfaces the failure as a checked SQLException rather than
        // silently returning a wrong boolean, so callers can choose their own
        // fallback behavior (CompactionService and EntityReplayService both do).
        assertThatThrownBy(() ->
                SchemaManager.hasUnpartitionedFiles(conn, "entity_order"))
                .isInstanceOf(SQLException.class);
    }
```

- [ ] **Step 2: Run it, verify it fails to compile**

Run: `mvn -pl joxette-service test -Dtest=SchemaManagerPartitionedTablesTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compile failure — `hasUnpartitionedFiles` does not exist yet.

- [ ] **Step 3: Implement `hasUnpartitionedFiles`**

In `SchemaManager.java`, add right after `ensureTablePartitioned` (from
Task 1):

```java
    /**
     * True if {@code tableName} has any physical Parquet file that predates
     * bucket partitioning being declared on it — i.e. a file with no row in
     * DuckLake's internal {@code ducklake_file_partition_value} tracking
     * table. Pure catalog-metadata query, no object-store I/O.
     *
     * <p>Used by {@code CompactionService} to decide whether a one-time
     * migration rewrite is needed, and by {@code EntityReplayService} to
     * decide whether the fast bucket-glob file-count path is safe to use
     * (it isn't, until this returns {@code false}).
     *
     * <p>Queries DuckLake's internal metadata schema (not a documented
     * app-facing API) — throws {@link SQLException} if unavailable (e.g. not
     * a real DuckLake-backed catalog), so every caller must handle that
     * explicitly rather than receiving a silently-wrong boolean.
     */
    public static boolean hasUnpartitionedFiles(Connection duckDB, String tableName) throws SQLException {
        String sql =
                "SELECT EXISTS (" +
                "  SELECT 1 FROM __ducklake_metadata_lake.ducklake_data_file df " +
                "  JOIN __ducklake_metadata_lake.ducklake_table t ON t.table_id = df.table_id " +
                "  WHERE t.table_name = ? AND t.end_snapshot IS NULL AND df.end_snapshot IS NULL " +
                "  AND NOT EXISTS (" +
                "    SELECT 1 FROM __ducklake_metadata_lake.ducklake_file_partition_value fpv " +
                "    WHERE fpv.data_file_id = df.data_file_id" +
                "  )" +
                ")";
        try (java.sql.PreparedStatement ps = duckDB.prepareStatement(sql)) {
            ps.setString(1, tableName);
            try (var rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }
```

- [ ] **Step 4: Run the test, verify it passes**

Run: `mvn -pl joxette-service test -Dtest=SchemaManagerPartitionedTablesTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `Tests run: 6, Failures: 0, Errors: 0`.

- [ ] **Step 5: Write the failing test for `CompactionService`'s graceful handling of a migration-check failure**

Add to `CompactionServiceTest.java`, in a new section after the existing
entity-compaction tests (after `executeRun_preservesEntityData`, line 287):

```java
    // -------------------------------------------------------------------------
    // Legacy-file migration (bucket partitioning) — graceful degradation
    // -------------------------------------------------------------------------
    //
    // The fake :memory:-attached `lake` harness has no __ducklake_metadata_lake
    // tables, so SchemaManager.hasUnpartitionedFiles always throws SQLException
    // here. This proves that failure doesn't abort the whole compaction run —
    // it must still complete, falling through to the (also-failing, per
    // existing tests) merge call. Real migration correctness is proven by
    // EntityBucketPartitioningIT against a genuine DuckLake catalog.

    @Test
    void executeRun_migrationCheckFails_stillCompletesRun() throws Exception {
        CompactionRun run = service.beginRun(TriggerSource.MANUAL, List.of(ENTITY_TYPE));
        service.executeRun(run.id(), List.of(ENTITY_TYPE));

        CompactionRun result = service.getRunById(run.id());
        assertThat(result.status()).isEqualTo(RunStatus.COMPLETED);
    }
```

- [ ] **Step 6: Run it to verify it currently passes (no new code yet — this is a baseline/regression pin)**

Run: `mvn -pl joxette-service test -Dtest=CompactionServiceTest#executeRun_migrationCheckFails_stillCompletesRun`
Expected: PASS — `executeRun` already tolerates the merge call itself
failing against this fake harness (existing behavior), so this test doesn't
prove anything new yet. It exists so that after Step 7 wires in the new
migration call, a regression (the new code accidentally making the run fail)
would be caught here.

- [ ] **Step 7: Implement `migrateLegacyPartitionFiles` and wire it in**

In `CompactionService.java`, add a new private method right before
`doCompactEntityType` (before line 389's javadoc):

```java
    /**
     * One-time migration: if {@code entity_{entityType}} still has any files
     * that predate bucket partitioning (see {@link SchemaManager#hasUnpartitionedFiles}),
     * rewrite the whole table so every row lands in its correct
     * {@code bucket=N/} file. The original flat file(s) end up with every
     * row soft-deleted; {@link #expireSnapshotsAndCleanup()} physically
     * reclaims them later in the same run once their retention window
     * allows it.
     *
     * <p>There is no way to scope this rewrite to just the legacy file(s)'
     * rows without an expensive full-table scan (no queryable "which file
     * did this row come from" column on a DuckLake table) — this is
     * deliberately a whole-table operation, but it only ever runs once per
     * entity type: once every file is partitioned, {@code hasUnpartitionedFiles}
     * returns {@code false} on every subsequent call and this is skipped
     * entirely.
     *
     * <p>A plain {@code DELETE} alone does not mark the original file as
     * superseded — verified empirically: the flat file remained physically
     * present even after {@code ducklake_expire_snapshots}/
     * {@code ducklake_cleanup_old_files}, because from DuckLake's
     * perspective the file is still "live," just filtered by a paired
     * delete-marker file at read time. {@code ducklake_rewrite_data_files}
     * is what actually rewrites a file exceeding its delete-ratio threshold
     * (default 0.95) — for a migrated flat file, which is always 100%
     * deleted, this produces zero replacement rows and is what makes the
     * file eligible for {@link #expireSnapshotsAndCleanup()} to physically
     * reclaim later in the same run. Skipping this call would leave
     * migration "working" (rows correctly repartitioned) but never actually
     * freeing the old files' disk space — silently reintroducing the exact
     * problem the earlier compaction-cleanup feature fixed.
     *
     * <p>Failures are logged and swallowed — a failed migration attempt
     * does not prevent the merge step that follows it, and simply retries
     * on the next compaction run.
     */
    private void migrateLegacyPartitionFiles(String entityType) {
        String table = "entity_" + entityType;
        try {
            if (!SchemaManager.hasUnpartitionedFiles(duckDB, table)) return;
        } catch (SQLException e) {
            log.debug("Could not check for unpartitioned files on entity_type='{}' ({}); skipping migration this run",
                    entityType, e.getMessage());
            return;
        }
        log.info("Migrating legacy unpartitioned files for entity_type='{}' into bucket layout", entityType);
        try (Statement st = duckDB.createStatement()) {
            st.execute("BEGIN TRANSACTION");
            st.execute("CREATE TEMP TABLE tmp_migrate_" + entityType
                    + " AS SELECT * FROM lake.main." + table);
            st.execute("DELETE FROM lake.main." + table);
            st.execute("INSERT INTO lake.main." + table
                    + " SELECT * FROM tmp_migrate_" + entityType);
            st.execute("COMMIT");
            try (ResultSet rs = st.executeQuery(
                    "SELECT * FROM ducklake_rewrite_data_files('lake', '" + table + "')")) {
                // No rows needed — this call's effect (marking the now-fully-deleted
                // flat file superseded) is what matters, not its return value.
            }
            log.info("Legacy file migration complete for entity_type='{}'", entityType);
        } catch (SQLException e) {
            log.warn("Legacy file migration failed for entity_type='{}' ({}); will retry next run",
                    entityType, e.getMessage());
        }
    }
```

Wire it into `doCompactEntityType` — change the start of the `synchronized
(duckDB)` block (`CompactionService.java:429-443`) from:

```java
            synchronized (duckDB) {
                // Apply the Parquet row-group memory cap before the merge (DuckDB 1.5.3+).
                // A value of 0 means "use the DuckDB default" — skip the SET entirely.
                if (rowGroupMemoryLimitMb > 0) {
```

to:

```java
            synchronized (duckDB) {
                migrateLegacyPartitionFiles(entityType);

                // Apply the Parquet row-group memory cap before the merge (DuckDB 1.5.3+).
                // A value of 0 means "use the DuckDB default" — skip the SET entirely.
                if (rowGroupMemoryLimitMb > 0) {
```

- [ ] **Step 8: Run the tests, verify everything still passes**

Run: `mvn -pl joxette-service test -Dtest=CompactionServiceTest`
Expected: all tests pass, including the new
`executeRun_migrationCheckFails_stillCompletesRun` (now genuinely exercising
`migrateLegacyPartitionFiles`'s catch-and-continue path against the fake
harness) and every pre-existing test (no regression).

- [ ] **Step 9: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/db/SchemaManager.java \
        joxette-service/src/main/java/com/joxette/compaction/CompactionService.java \
        joxette-service/src/test/java/com/joxette/db/SchemaManagerPartitionedTablesTest.java \
        joxette-service/src/test/java/com/joxette/compaction/CompactionServiceTest.java
git commit -m "feat(compaction): migrate legacy unpartitioned entity files into bucket layout"
```

---

### Task 3: `EntityReplayService` bucket-glob fast path

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/replay/EntityReplayService.java`
- Modify: `joxette-service/src/test/java/com/joxette/replay/EntityReplayServiceTest.java`

**Interfaces:**
- Consumes: `SchemaManager.hasUnpartitionedFiles` (Task 2),
  `MessageRouter.computeBucket(String entityType, String entityId, int buckets)`
  (already exists, package-private, same package — no import needed).
- Produces: `countEntityFiles`/`pruneCandidateFiles` both gain an `entityType`
  parameter; behavior is unchanged for any table still reporting unpartitioned
  files (falls through to the existing two-stage logic verbatim), and uses a
  new fast path otherwise.

- [ ] **Step 1: Write the failing test for the fast-path-unavailable fallback**

This is largely already covered by the two existing tests from the previous
feature (`getEntityStats_fileCountIsZero_whenObjectStoragePathNotConfigured`
and `getEntityStats_computesDirectoryAndConsoleUrl_evenWhenFileCountUnavailable`
in `EntityReplayServiceTest.java`) — against the fake harness,
`SchemaManager.hasUnpartitionedFiles` will throw, so the fast path's own
try/catch falls through to the pre-existing `listAllFiles`-fails-too path,
landing on `fileCount = 0` exactly as before. No new unit test is needed for
this fallback specifically; Step 2 just re-runs the existing suite to prove
the signature change doesn't break it.

- [ ] **Step 2: Extend `EntityStats`'s "computes directory" test name/comment for clarity, then run the existing suite to confirm current (pre-change) green baseline**

Run: `mvn -pl joxette-service test -Dtest=EntityReplayServiceTest`
Expected: `Tests run: 45, Failures: 0, Errors: 0` (current baseline from the
previous feature — confirms starting point before this task's changes).

- [ ] **Step 3: Add the `entityType` parameter and implement the fast path**

In `EntityReplayService.java`, add the import (alongside the existing
`com.joxette.config.JoxetteProperties` import at line 13):

```java
import com.joxette.db.SchemaManager;
```

Change the `getEntityStats` call site (`EntityReplayService.java:707-712`)
from:

```java
                int fileCount;
                try {
                    fileCount = countEntityFiles(bareTable, entityId);
                } catch (SQLException e) {
                    throw new DataAccessException("countEntityFiles failed for " + bareTable, e);
                }
```

to:

```java
                int fileCount;
                try {
                    fileCount = countEntityFiles(bareTable, entityType, entityId);
                } catch (SQLException e) {
                    throw new DataAccessException("countEntityFiles failed for " + bareTable, e);
                }
```

Change `countEntityFiles` (`EntityReplayService.java:773-780`) from:

```java
    private int countEntityFiles(String table, String entityId) throws SQLException {
        String objectStoragePath = props.getCatalog().getObjectStoragePath();
        if (objectStoragePath == null || objectStoragePath.isBlank()) return 0;

        List<String> candidates = pruneCandidateFiles(table, entityId);
        if (candidates.isEmpty()) return 0;
        return verifyCandidates(candidates, entityId);
    }
```

to:

```java
    private int countEntityFiles(String table, String entityType, String entityId) throws SQLException {
        String objectStoragePath = props.getCatalog().getObjectStoragePath();
        if (objectStoragePath == null || objectStoragePath.isBlank()) return 0;

        List<String> candidates = pruneCandidateFiles(table, entityType, entityId);
        if (candidates.isEmpty()) return 0;
        return verifyCandidates(candidates, entityId);
    }
```

Change `pruneCandidateFiles` (`EntityReplayService.java:782-808`) from:

```java
    private List<String> pruneCandidateFiles(String table, String entityId) {
        List<String> allFiles;
        try {
            allFiles = listAllFiles(table);
        } catch (SQLException e) {
```

to:

```java
    private List<String> pruneCandidateFiles(String table, String entityType, String entityId) {
        try {
            if (!SchemaManager.hasUnpartitionedFiles(duckDB, table)) {
                return globBucketDirectory(table, entityType, entityId);
            }
        } catch (SQLException e) {
            log.debug("countEntityFiles: bucket-glob fast path unavailable for table '{}' ({}); " +
                    "falling back to full scan", table, e.getMessage());
        }

        List<String> allFiles;
        try {
            allFiles = listAllFiles(table);
        } catch (SQLException e) {
```

(the rest of the method — from `log.debug("countEntityFiles: ducklake_list_files unavailable...")`
through its closing brace — is unchanged).

Add two new private methods right after `pruneCandidateFiles` (before
`listAllFiles`, `EntityReplayService.java:810`):

```java
    /**
     * Fast path: once a table has no legacy unpartitioned files left
     * ({@link SchemaManager#hasUnpartitionedFiles} returned {@code false}),
     * every file that could contain this entity's rows lives under its own
     * {@code bucket=N/} directory — glob just that directory instead of
     * scanning the whole table's catalog metadata.
     */
    private List<String> globBucketDirectory(String table, String entityType, String entityId) throws SQLException {
        int bucketCount = lookupBucketCount(entityType);
        int bucket = MessageRouter.computeBucket(entityType, entityId, bucketCount);
        String objectStoragePath = props.getCatalog().getObjectStoragePath();
        String base = objectStoragePath.endsWith("/") ? objectStoragePath : objectStoragePath + "/";
        String glob = base + "main/" + table + "/bucket=" + bucket + "/**/*.parquet";
        List<String> files = new ArrayList<>();
        try (PreparedStatement ps = duckDB.prepareStatement("SELECT file FROM glob(?)")) {
            ps.setString(1, glob);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) files.add(rs.getString(1));
            }
        }
        return files;
    }

    /** Bucket count configured for {@code entityType}, defaulting to 256 if not registered. */
    private int lookupBucketCount(String entityType) throws SQLException {
        try (PreparedStatement ps = duckDB.prepareStatement(
                "SELECT bucket_count FROM entity_type_configs WHERE entity_type = ?")) {
            ps.setString(1, entityType);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 256;
            }
        }
    }
```

- [ ] **Step 4: Compile and run the full `EntityReplayServiceTest` class**

Run: `mvn -pl joxette-service test -Dtest=EntityReplayServiceTest`
Expected: `Tests run: 45, Failures: 0, Errors: 0` — unchanged from Step 2's
baseline. Against the fake harness, `hasUnpartitionedFiles` always throws,
so every existing test's behavior is byte-for-byte the same as before this
task (falls through to the unchanged slow-path fallback) — this task is a
pure additive fast path, invisible to any test that can't exercise a real
DuckLake catalog.

- [ ] **Step 5: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/replay/EntityReplayService.java
git commit -m "feat(replay): fast-path entity file count via bucket directory glob"
```

---

### Task 4: Real Testcontainers IT proving migration + fast path both work

**Files:**
- Create: `joxette-service/src/test/java/com/joxette/it/EntityBucketPartitioningIT.java`

**Interfaces:**
- Consumes: `CompactionService` (Task 2), `EntityReplayService` (Task 3),
  both as real Spring-wired `@Autowired` beans against a genuine DuckLake +
  MinIO catalog — matching the established pattern from
  `CompactionSnapshotCleanupIT`/`EntityFileCountIT`.

This is the task that proves the whole feature end-to-end: old flat files
get migrated into `bucket=N/` layout by compaction, and the file-count fast
path built on top of that migration gives the same correct answer the
already-proven slow path would.

- [ ] **Step 1: Write the IT test**

Create `joxette-service/src/test/java/com/joxette/it/EntityBucketPartitioningIT.java`:

```java
package com.joxette.it;

import com.joxette.compaction.CompactionService;
import com.joxette.compaction.RunStatus;
import com.joxette.compaction.TriggerSource;
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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the bucket-partitioning migration end-to-end: entities written
 * before partitioning was declared live in old flat files; after compaction
 * runs, those files are gone, every remaining file lives under a
 * {@code bucket=N/} path, all rows survived intact, and the file-count fast
 * path (built on top of the same migration signal) gives the same correct
 * per-entity count the slow scan-based path already proved correct for
 * (see {@code EntityFileCountIT}).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class EntityBucketPartitioningIT {

    private static final String BUCKET = "joxette-partition-test";
    private static final String ENTITY_TYPE = "partitiontest";

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
        // 0h retention so the snapshot created by this test's own migration is
        // immediately eligible for expiry -- otherwise expireSnapshotsAndCleanup()
        // (default 24h retention) would leave the old flat file physically present
        // on disk even though its rows are already soft-deleted, and the
        // "no flat files remain" assertion below would fail. Same pattern as
        // CompactionSnapshotCleanupIT.
        registry.add("joxette.compaction.snapshot-retention-hours", () -> "0");
    }

    @Autowired private Connection duckDB;
    @Autowired private CompactionService compactionService;
    @Autowired private EntityReplayService entityReplayService;

    @Test
    void compaction_migratesLegacyFilesIntoBucketLayout_andFastPathStaysCorrect() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS lake.main.entity_" + ENTITY_TYPE + " (" +
                    "recorded_at TIMESTAMPTZ, entity_id VARCHAR, bucket INTEGER, message_type VARCHAR, " +
                    "topic VARCHAR, kafka_offset BIGINT, kafka_partition INTEGER, kafka_timestamp TIMESTAMPTZ, " +
                    "kafka_key BLOB, kafka_value BLOB, metadata VARCHAR, " +
                    "headers STRUCT(key VARCHAR, value VARCHAR)[])");
            st.execute("INSERT INTO entity_type_configs (entity_type, bucket_count) VALUES ('"
                    + ENTITY_TYPE + "', 64) ON CONFLICT DO NOTHING");
        }

        // Write BEFORE partitioning is declared -- old flat files.
        insertAndFlush("A", 0);
        insertAndFlush("B", 1);

        int flatFilesBefore = countFilesOutsideBucketDirs();
        assertThat(flatFilesBefore).as("both pre-partition inserts landed as flat files").isEqualTo(2);

        // Declare partitioning (what SchemaManager does at every startup -- issued
        // directly here since the table itself was created after this test's Spring
        // context already booted, so SchemaManager's own startup pass never saw it).
        try (Statement st = duckDB.createStatement()) {
            st.execute("ALTER TABLE lake.main.entity_" + ENTITY_TYPE + " SET PARTITIONED BY (bucket)");
        }

        // Write AFTER partitioning -- new bucket=N files, same two entities.
        insertAndFlush("A", 2);
        insertAndFlush("B", 3);

        int bucketFilesBeforeCompaction = countFilesUnderBucketDirs();
        assertThat(bucketFilesBeforeCompaction)
                .as("post-partition inserts landed under bucket=N/ immediately, no migration needed for them")
                .isGreaterThanOrEqualTo(1);

        var run = compactionService.beginRun(TriggerSource.MANUAL, List.of(ENTITY_TYPE));
        compactionService.executeRun(run.id(), List.of(ENTITY_TYPE));

        var completed = compactionService.getRunById(run.id());
        assertThat(completed.status()).isEqualTo(RunStatus.COMPLETED);

        assertThat(countFilesOutsideBucketDirs())
                .as("old flat files were migrated away -- none should remain outside bucket=N/")
                .isZero();
        assertThat(countFilesUnderBucketDirs())
                .as("every remaining file lives under some bucket=N/ directory")
                .isGreaterThan(0);

        try (Statement st = duckDB.createStatement();
             var rs = st.executeQuery("SELECT COUNT(*) FROM lake.main.entity_" + ENTITY_TYPE)) {
            rs.next();
            assertThat(rs.getInt(1)).as("no rows lost or duplicated by the migration").isEqualTo(4);
        }

        EntityStats statsA = entityReplayService.getEntityStats(ENTITY_TYPE, "A");
        EntityStats statsB = entityReplayService.getEntityStats(ENTITY_TYPE, "B");
        assertThat(statsA.fileCount()).as("fast path gives a correct, nonzero count for A").isGreaterThan(0);
        assertThat(statsB.fileCount()).as("fast path gives a correct, nonzero count for B").isGreaterThan(0);
        assertThat(statsA.messageCount()).as("row data intact for A after migration").isEqualTo(2);
        assertThat(statsB.messageCount()).as("row data intact for B after migration").isEqualTo(2);
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

    private int countFilesOutsideBucketDirs() throws Exception {
        try (Statement st = duckDB.createStatement();
             var rs = st.executeQuery(
                     "SELECT COUNT(*) FROM glob('s3://" + BUCKET + "/data/main/entity_"
                     + ENTITY_TYPE + "/*.parquet')")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private int countFilesUnderBucketDirs() throws Exception {
        try (Statement st = duckDB.createStatement();
             var rs = st.executeQuery(
                     "SELECT COUNT(*) FROM glob('s3://" + BUCKET + "/data/main/entity_"
                     + ENTITY_TYPE + "/bucket=*/**/*.parquet')")) {
            rs.next();
            return rs.getInt(1);
        }
    }
}
```

Note on bucket collisions: entities `"A"` and `"B"` with `bucket_count=64`
may or may not land in the same bucket via `MessageRouter.computeBucket` —
the assertions above are deliberately written to not depend on which bucket
either one lands in (no hardcoded bucket numbers), only on "some bucket
directory has files" / "no flat files remain" / "row/file counts are
correct," so this test is robust to whatever the hash actually produces.

- [ ] **Step 2: Run it**

Run: `mvn -pl joxette-service test -Dtest=EntityBucketPartitioningIT -Dsurefire.failIfNoSpecifiedTests=false`

This spins up a real Spring context + MinIO Testcontainer (~15-30s startup)
— when running in the background, poll with `pgrep -f surefirebooter` and
wait for it to genuinely exit before trusting any completion notification or
reading the surefire report (this environment has repeatedly fired
"completed" notifications before the process actually finished, in every
prior IT test run this session).

Expected: `Tests run: 1, Failures: 0, Errors: 0`. If it fails, root-cause it
per the assertions' own messages (they're written to pinpoint which stage —
pre-migration flat-file count, post-migration flat-file count, bucket-file
count, row count, or fast-path file count — diverged from expectation) rather
than guessing. Fix forward; don't weaken an assertion to make it pass.

- [ ] **Step 3: Commit**

```bash
git add joxette-service/src/test/java/com/joxette/it/EntityBucketPartitioningIT.java
git commit -m "test(compaction,replay): verify bucket-partition migration and fast-path file count end-to-end"
```
