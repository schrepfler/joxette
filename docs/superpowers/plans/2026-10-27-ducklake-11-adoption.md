# DuckLake 1.1 Adoption Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Use the DuckLake 1.1 features that fit Joxette: tables declare partitioning, sort order and stats options at creation; cassettes are time-partitioned so retention retires whole files; compaction honours `lookback-days`; retention's file rewrite is bounded like compaction's merge.

**Architecture:** `SchemaManager` builds each cassette table's full DDL in pure static functions (`generalCassetteDdl`, `entityCassetteDdl`), so the DDL can be asserted as a string and also executed against a real DuckLake catalog in a temp directory. Compaction and retention build their maintenance SQL in pure static functions too (`CompactionService.mergeSql`, `RetentionService.rewriteSql`), and both run it through one shared bounded-batch-with-retry loop (`CompactionService.runInBatches`). The legacy "migrate flat files into `bucket=N/`" code goes away, because tables are now partitioned from the moment they are created.

**Tech Stack:** Java 25, Spring Boot 4, DuckDB JDBC `org.duckdb:duckdb_jdbc:2.0.0.0`, DuckLake extension (catalog spec 1.1), JUnit 5 + AssertJ, Maven.

## Global Constraints

- No backward compatibility. Existing catalogs are not migrated. Tables created before this change keep their old partition/sort spec; recreate dev catalogs (delete the `.ducklake` file and the data path) after deploying.
- DuckLake 1.1 DDL clause order is exactly `CREATE TABLE … ( … ) PARTITIONED BY (…) SORTED BY (…) WITH (…)`. `WITH` before `PARTITIONED BY` is a parser error.
- Inline `SORTED BY` must not contain `ASC`/`DESC`: `SORTED BY (a ASC)` is a parser error. Ascending is the default.
- `skip_stats_columns` must not list a VARIANT column: it raises `Not implemented Error: Statistics cannot be skipped for VARIANT columns`. So `metadata` is never in the list.
- Do NOT enable `write_deletion_vectors`. DuckLake marks it "[EXPERIMENTAL - do not use outside testing]". With it on, a DELETE covering a whole file did not retire that file, which defeats the time-partitioning retention win.
- `ducklake_merge_adjacent_files(..., older_than => TIMESTAMP)` selects files by when they *entered the lake*, not by row timestamps.
- `max_compacted_files` caps the files a call *writes*. A rewrite with `max_compacted_files => 2` was observed reading 3 files and writing 1.
- Every build and test command needs JDK 25 (the SDKMAN default is 27, which the enforcer rejects). Prefix commands with:
  `export JAVA_HOME=~/.sdkman/candidates/java/25.0.4-graal PATH=~/.sdkman/candidates/java/25.0.4-graal/bin:$PATH &&`
- The single-test command form is:
  `mvn -B -q -pl joxette-service -am test -Dtest='<Classes>' -Dsurefire.failIfNoSpecifiedTests=false`
  Run from the repo root (`/Users/schrepfler/repos/joxette`).
- Tests that need a real DuckLake catalog use `DuckDBTestSupport.newDuckLakeConnection(@TempDir Path)` (added in Task 1). They need the `ducklake` extension download, like the existing DuckLake tests. `DuckDBTestSupport.newConnection()` is plain DuckDB and cannot run DuckLake DDL.
- Commit messages end with: `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`

## Decisions baked into this plan

| Decision | Why |
|---|---|
| Time-partition on `recorded_at`, not `kafka_timestamp` | `RetentionService` deletes `WHERE recorded_at < cutoff`. Partitions must line up with that predicate for whole files to retire. |
| General: `epoch_day(recorded_at)`; entity: `(bucket, epoch_month(recorded_at))` | Entity tables already fan out into 256 buckets. Per-day partitions would mean 256 × 365 partitions a year and worse small-file counts. Per-month keeps ~3K partitions a year. Retention then retires whole months and rewrites only the boundary month. |
| Global granularity, no per-topic `time_partition` column | `topic_configs` has no such column (only CLAUDE.md mentions it). Retention works in days, so day granularity for topics is enough (YAGNI). |
| `skip_stats_columns = ['kafka_value', 'headers']` | Nothing filters on them. `kafka_key` stays (the `$.key` transform pushdown filters on it), as do `message_type` and `entity_id`. `metadata` is VARIANT and cannot be skipped. |
| `lookback-days` default 30 → 1 (entity and general) | With `older_than` now actually applied, 30 would leave a month of small inline-flush files in the hot window that replay reads most. One day keeps compaction off files that are still being added to. |
| `minFilesPerBucket` / `minFilesPerPartition` stay unwired | Out of scope here; DuckLake 1.1 adds nothing that maps onto them. |

---

### Task 0: Commit the pending DuckDB 2.0 upgrade

The working tree already contains the DuckDB 2.0.0.0 upgrade from an earlier session (lambda syntax, `tableHasTrackedFiles` fix, test connection leak fix, doc versions). Commit it first so this plan's commits stay separate.

- [ ] **Step 1: Confirm what's pending**

Run: `git status --short`
Expected: modified `pom.xml`, `CLAUDE.md`, `README.md`, `memory-bank/techContext.md`, `SchemaManager.java`, `HeadersHelper.java`, `db/init.sql`, `HeadersHelperTest.java`, `CassetteLifecycleService.java`, `ReconciliationServiceLifecycleTest.java`.

- [ ] **Step 2: Commit in two parts**

```bash
git add pom.xml CLAUDE.md README.md memory-bank/techContext.md \
  joxette-service/src/main/java/com/joxette/db/SchemaManager.java \
  joxette-service/src/main/java/com/joxette/replay/HeadersHelper.java \
  joxette-service/src/main/resources/db/init.sql \
  joxette-service/src/test/java/com/joxette/replay/HeadersHelperTest.java \
  joxette-service/src/test/java/com/joxette/reconciliation/ReconciliationServiceLifecycleTest.java
git commit -m "$(cat <<'EOF'
Upgrade DuckDB to 2.0.0.0

2.0 disables the single-arrow lambda syntax by default, so the header
macros use `lambda h : ...`. Also closes a duplicated connection the
reconciliation lifecycle test leaked.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
git add joxette-service/src/main/java/com/joxette/replay/CassetteLifecycleService.java
git commit -m "$(cat <<'EOF'
Detect deleted entity tables from DuckLake file history

DuckLake 1.1 retires a file from ducklake_list_files as soon as all its
rows are deleted, so an emptied table looked like a lost catalog and
recoverOrphanedFiles=true resurrected GDPR-erased rows. Check the
catalog's ducklake_data_file history for the table's current id instead.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 1: Declare sort order and bucket partitioning inline in CREATE TABLE

Today `SchemaManager` creates each table, then runs `ALTER TABLE … SET SORTED BY` / `SET PARTITIONED BY (bucket)` as separate statements whose failures are only logged. This task moves both into the CREATE, so a table can't exist without them.

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/db/SchemaManager.java`: `createGeneralCassetteTable`, `createEntityCassetteTable`, `migrateEntityCassetteTables`; delete `ensureTableSorted`, `ensureTablePartitioned`
- Modify: `joxette-service/src/test/java/com/joxette/support/DuckDBTestSupport.java`: add `newDuckLakeConnection`, `flushInlinedData`
- Create: `joxette-service/src/test/java/com/joxette/db/SchemaManagerDdlTest.java`
- Create: `joxette-service/src/test/java/com/joxette/db/DuckLakeTableLayoutTest.java`
- Delete: `joxette-service/src/test/java/com/joxette/db/SchemaManagerSortedTablesTest.java`
- Modify: `joxette-service/src/test/java/com/joxette/db/SchemaManagerPartitionedTablesTest.java`: drop the `ensureTablePartitioned` tests and keep the `hasUnpartitionedFiles` ones

**Interfaces:**
- Produces:
  - `static final String SchemaManager.GENERAL_SORTED_BY`
  - `static final String SchemaManager.ENTITY_SORTED_BY`
  - `static final String SchemaManager.ENTITY_PARTITIONED_BY`
  - `static String SchemaManager.generalCassetteDdl(String catalog, String tableName, String flexType)`
  - `static String SchemaManager.entityCassetteDdl(String catalog, String tableName, String flexType)`
  - `public static Connection DuckDBTestSupport.newDuckLakeConnection(java.nio.file.Path dir)`
  - `public static void DuckDBTestSupport.flushInlinedData(Connection conn)`

- [ ] **Step 1: Add the real-DuckLake test helpers to `DuckDBTestSupport`**

Add after `newConnection()`:

```java
    /**
     * Opens an in-memory DuckDB connection with a real DuckLake catalog attached as
     * {@code lake}, catalog and Parquet files both under {@code dir}. Unlike
     * {@link #newConnection()}, DuckLake-only DDL ({@code PARTITIONED BY},
     * {@code SORTED BY}, table options) and the {@code ducklake_*} functions work here.
     */
    public static Connection newDuckLakeConnection(java.nio.file.Path dir) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("INSTALL ducklake");
            st.execute("LOAD ducklake");
            st.execute("ATTACH 'ducklake:" + dir.resolve("catalog.ducklake") + "' AS lake (DATA_PATH '"
                    + dir.resolve("files") + "/')");
        } catch (SQLException e) {
            conn.close();
            throw e;
        }
        return conn;
    }

    /** Writes every inlined row of the {@code lake} catalog out to Parquet files. */
    public static void flushInlinedData(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("CALL ducklake_flush_inlined_data('lake')");
        }
    }
```

- [ ] **Step 2: Write the failing string test**

Create `joxette-service/src/test/java/com/joxette/db/SchemaManagerDdlTest.java`:

```java
package com.joxette.db;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** String checks on the cassette DDL; {@link DuckLakeTableLayoutTest} runs it against real DuckLake. */
class SchemaManagerDdlTest {

    @Test
    void generalDdl_declaresSortOrderInline() {
        String ddl = SchemaManager.generalCassetteDdl("lake", "general_orders_events", "VARIANT");

        assertThat(ddl)
                .contains("CREATE TABLE IF NOT EXISTS lake.main.general_orders_events (")
                .contains("SORTED BY (kafka_timestamp, kafka_partition, kafka_offset)")
                .doesNotContain("ASC"); // DuckLake rejects ASC inside an inline SORTED BY
    }

    @Test
    void entityDdl_declaresBucketPartitioningAndSortOrderInline() {
        String ddl = SchemaManager.entityCassetteDdl("lake", "entity_order", "VARIANT");

        assertThat(ddl)
                .contains("CREATE TABLE IF NOT EXISTS lake.main.entity_order (")
                .contains("PARTITIONED BY (bucket)")
                .contains("SORTED BY (entity_id, kafka_timestamp, recorded_at)")
                .doesNotContain("ASC");
    }

    @Test
    void ddl_usesJsonWhenVariantIsUnsupported() {
        assertThat(SchemaManager.entityCassetteDdl("lake", "entity_order", "JSON"))
                .contains("metadata        JSON");
    }
}
```

- [ ] **Step 3: Write the failing real-DuckLake test**

Create `joxette-service/src/test/java/com/joxette/db/DuckLakeTableLayoutTest.java`:

```java
package com.joxette.db;

import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs SchemaManager's cassette DDL against a real DuckLake catalog in a temp directory
 * (local files, no object store) and checks what DuckLake recorded and wrote.
 */
class DuckLakeTableLayoutTest {

    @TempDir
    Path dir;

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DuckDBTestSupport.newDuckLakeConnection(dir);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null) conn.close();
    }

    @Test
    void entityTable_isPartitionedAndSortedFromCreation() throws SQLException {
        exec(SchemaManager.entityCassetteDdl("lake", "entity_order", "VARIANT"));

        assertThat(partitionKeys("entity_order")).containsExactly("bucket");
        assertThat(sortKeys("entity_order")).containsExactly("entity_id", "kafka_timestamp", "recorded_at");
    }

    @Test
    void generalTable_isSortedFromCreation() throws SQLException {
        exec(SchemaManager.generalCassetteDdl("lake", "general_orders_events", "VARIANT"));

        assertThat(sortKeys("general_orders_events"))
                .containsExactly("kafka_timestamp", "kafka_partition", "kafka_offset");
    }

    @Test
    void ddl_isIdempotent() throws SQLException {
        exec(SchemaManager.entityCassetteDdl("lake", "entity_order", "VARIANT"));
        exec(SchemaManager.entityCassetteDdl("lake", "entity_order", "VARIANT"));

        assertThat(partitionKeys("entity_order")).containsExactly("bucket");
        assertThat(sortKeys("entity_order")).containsExactly("entity_id", "kafka_timestamp", "recorded_at");
    }

    // -------------------------------------------------------------------------

    private void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    /** Partition keys in order, as {@code column} or {@code transform(column)}. */
    private List<String> partitionKeys(String table) throws SQLException {
        return strings("""
                SELECT CASE WHEN pc.transform = 'identity' THEN c.column_name
                            ELSE pc.transform || '(' || c.column_name || ')' END
                FROM __ducklake_metadata_lake.ducklake_partition_column pc
                JOIN __ducklake_metadata_lake.ducklake_table t
                  ON t.table_id = pc.table_id AND t.end_snapshot IS NULL
                JOIN __ducklake_metadata_lake.ducklake_column c
                  ON c.table_id = pc.table_id AND c.column_id = pc.column_id AND c.end_snapshot IS NULL
                WHERE t.table_name = ?
                ORDER BY pc.partition_key_index
                """, table);
    }

    private List<String> sortKeys(String table) throws SQLException {
        return strings("""
                SELECT se.expression
                FROM __ducklake_metadata_lake.ducklake_sort_expression se
                JOIN __ducklake_metadata_lake.ducklake_table t
                  ON t.table_id = se.table_id AND t.end_snapshot IS NULL
                WHERE t.table_name = ?
                ORDER BY se.sort_id, se.sort_key_index
                """, table);
    }

    private List<String> strings(String sql, String param) throws SQLException {
        List<String> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, param);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(rs.getString(1));
            }
        }
        return out;
    }
}
```

- [ ] **Step 4: Run both tests to verify they fail**

Run: `mvn -B -q -pl joxette-service -am test -Dtest='SchemaManagerDdlTest,DuckLakeTableLayoutTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation FAILURE, `cannot find symbol … method generalCassetteDdl` / `entityCassetteDdl`.

- [ ] **Step 5: Implement the DDL builders in `SchemaManager`**

Add these constants next to the other static fields at the top of the class:

```java
    /** Sort order DuckLake applies when it flushes inlined rows and merges files. */
    static final String GENERAL_SORTED_BY = "kafka_timestamp, kafka_partition, kafka_offset";
    static final String ENTITY_SORTED_BY  = "entity_id, kafka_timestamp, recorded_at";

    /** Entity rows land under {@code bucket=N/}, so one entity's replay reads one bucket. */
    static final String ENTITY_PARTITIONED_BY = "bucket";
```

Replace the whole `createGeneralCassetteTable` method (javadoc included) with:

```java
    /**
     * Records every raw Kafka message from a topic verbatim.
     * {@code metadata} holds any decoded JSON/VARIANT envelope for ad-hoc queries.
     */
    private void createGeneralCassetteTable(Connection conn, String catalog,
                                             String tableName, String flexType)
            throws SQLException {
        exec(conn, generalCassetteDdl(catalog, tableName, flexType));
        log.debug("DuckLake table ready: {}.main.{}", catalog, tableName);
    }

    /**
     * DDL for a general cassette table. Sort order is declared in the CREATE itself
     * (DuckLake 1.1), so the table never exists without it. Inline {@code SORTED BY}
     * takes no {@code ASC}: ascending is the default and the keyword is a parser error.
     */
    static String generalCassetteDdl(String catalog, String tableName, String flexType) {
        return String.format("""
            CREATE TABLE IF NOT EXISTS %s.main.%s (
                recorded_at     TIMESTAMPTZ NOT NULL,
                kafka_offset    BIGINT      NOT NULL,
                kafka_partition INTEGER     NOT NULL,
                kafka_timestamp TIMESTAMPTZ NOT NULL,
                kafka_key       BLOB,
                kafka_value     BLOB,
                metadata        %s,
                headers         STRUCT(key VARCHAR, value VARCHAR)[],
                message_type    VARCHAR
            )
            SORTED BY (%s)
            """, catalog, tableName, flexType, GENERAL_SORTED_BY);
    }
```

Replace the whole `createEntityCassetteTable` method (javadoc included) with:

```java
    /**
     * Records Kafka messages for a specific entity type, partitioned by bucket.
     * {@code bucket} = {@code hash(entity_id) mod bucket_count} for even distribution.
     */
    private void createEntityCassetteTable(Connection conn, String catalog,
                                            String tableName, String flexType)
            throws SQLException {
        exec(conn, entityCassetteDdl(catalog, tableName, flexType));
        log.debug("DuckLake table ready: {}.main.{}", catalog, tableName);
    }

    /**
     * DDL for an entity cassette table. Partitioning and sort order are declared in the
     * CREATE itself (DuckLake 1.1), so every file the table ever gets is laid out by
     * them — there is no window, and no swallowed ALTER failure, that leaves a table
     * writing unpartitioned files.
     */
    static String entityCassetteDdl(String catalog, String tableName, String flexType) {
        return String.format("""
            CREATE TABLE IF NOT EXISTS %s.main.%s (
                recorded_at     TIMESTAMPTZ NOT NULL,
                entity_id       VARCHAR     NOT NULL,
                bucket          INTEGER     NOT NULL,
                message_type    VARCHAR,
                topic           VARCHAR     NOT NULL,
                kafka_offset    BIGINT      NOT NULL,
                kafka_partition INTEGER     NOT NULL,
                kafka_timestamp TIMESTAMPTZ NOT NULL,
                kafka_key       BLOB,
                kafka_value     BLOB,
                metadata        %s,
                headers         STRUCT(key VARCHAR, value VARCHAR)[]
            )
            PARTITIONED BY (%s)
            SORTED BY (%s)
            """, catalog, tableName, flexType, ENTITY_PARTITIONED_BY, ENTITY_SORTED_BY);
    }
```

In `migrateEntityCassetteTables`, delete the line `ensureTablePartitioned(conn, catalog, tableName);`, leaving the loop body as:

```java
        for (String tableName : tableNames) {
            dropColumnIfExists(conn, catalog + ".main." + tableName, "kafka_value_str");
        }
```

Delete the `ensureTableSorted` and `ensureTablePartitioned` methods and their javadocs entirely.

- [ ] **Step 6: Remove the tests for the deleted ALTER helpers**

```bash
git rm joxette-service/src/test/java/com/joxette/db/SchemaManagerSortedTablesTest.java
```

In `SchemaManagerPartitionedTablesTest.java`, delete every `@Test` method that calls `SchemaManager.ensureTablePartitioned` (at least `nonExistentTable_doesNotThrow`, `entityCassette_sqlContainsBucketColumn`, `sqlExceptionFromDriver_isSwallowedNotPropagated`) and any `@BeforeEach` setup or imports used only by them. Keep the `hasUnpartitionedFiles…` tests.

Run: `git grep -n -E 'ensureTable(Sorted|Partitioned)' -- joxette-service`
Expected: no output.

- [ ] **Step 7: Run the new and neighbouring tests**

Run: `mvn -B -q -pl joxette-service -am test -Dtest='SchemaManagerDdlTest,DuckLakeTableLayoutTest,SchemaManagerPartitionedTablesTest,SchemaManager*Test' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: BUILD SUCCESS. Spot-check that `joxette-service/target/surefire-reports/TEST-com.joxette.db.DuckLakeTableLayoutTest.xml` shows `tests="3"` and `failures="0"`.

- [ ] **Step 8: Commit**

```bash
git add -A joxette-service/src/main/java/com/joxette/db/SchemaManager.java \
  joxette-service/src/test/java/com/joxette/support/DuckDBTestSupport.java \
  joxette-service/src/test/java/com/joxette/db/
git commit -m "$(cat <<'EOF'
Declare cassette sort order and bucket partitioning in CREATE TABLE

DuckLake 1.1 accepts PARTITIONED BY and SORTED BY inline, so tables are
born with them instead of relying on follow-up ALTERs whose failures were
only logged.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Time-partition cassette tables by recorded_at

General tables get `PARTITIONED BY (epoch_day(recorded_at))`; entity tables get `(bucket, epoch_month(recorded_at))`. A retention DELETE whose cutoff falls on a partition boundary then retires whole files, with no delete files and nothing to rewrite (verified on DuckDB 2.0.0).

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/db/SchemaManager.java`: constants and `generalCassetteDdl`
- Modify: `joxette-service/src/test/java/com/joxette/db/SchemaManagerDdlTest.java`
- Modify: `joxette-service/src/test/java/com/joxette/db/DuckLakeTableLayoutTest.java`

**Interfaces:**
- Consumes: `SchemaManager.generalCassetteDdl`, `entityCassetteDdl`, `DuckDBTestSupport.newDuckLakeConnection`, `flushInlinedData` (Task 1)
- Produces: `static final String SchemaManager.GENERAL_PARTITIONED_BY = "epoch_day(recorded_at)"`; `ENTITY_PARTITIONED_BY` becomes `"bucket, epoch_month(recorded_at)"`

- [ ] **Step 1: Update the string tests to the new partitioning**

In `SchemaManagerDdlTest`, replace `generalDdl_declaresSortOrderInline` and `entityDdl_declaresBucketPartitioningAndSortOrderInline` with:

```java
    @Test
    void generalDdl_partitionsByDayAndDeclaresSortOrderInline() {
        String ddl = SchemaManager.generalCassetteDdl("lake", "general_orders_events", "VARIANT");

        assertThat(ddl)
                .contains("CREATE TABLE IF NOT EXISTS lake.main.general_orders_events (")
                .contains("PARTITIONED BY (epoch_day(recorded_at))")
                .contains("SORTED BY (kafka_timestamp, kafka_partition, kafka_offset)")
                .doesNotContain("ASC"); // DuckLake rejects ASC inside an inline SORTED BY
    }

    @Test
    void entityDdl_partitionsByBucketAndMonthAndDeclaresSortOrderInline() {
        String ddl = SchemaManager.entityCassetteDdl("lake", "entity_order", "VARIANT");

        assertThat(ddl)
                .contains("CREATE TABLE IF NOT EXISTS lake.main.entity_order (")
                .contains("PARTITIONED BY (bucket, epoch_month(recorded_at))")
                .contains("SORTED BY (entity_id, kafka_timestamp, recorded_at)")
                .doesNotContain("ASC");
    }
```

- [ ] **Step 2: Add the layout and retention-retirement tests**

In `DuckLakeTableLayoutTest`:

1. In `entityTable_isPartitionedAndSortedFromCreation` and `ddl_isIdempotent`, change `containsExactly("bucket")` to `containsExactly("bucket", "epoch_month(recorded_at)")`.
2. Add `assertThat(partitionKeys("general_orders_events")).containsExactly("epoch_day(recorded_at)");` at the end of `generalTable_isSortedFromCreation`, and rename that method to `generalTable_isPartitionedAndSortedFromCreation`.
3. Add these tests and helpers:

```java
    @Test
    void entityRows_landInBucketThenMonthDirectories() throws SQLException {
        exec(SchemaManager.entityCassetteDdl("lake", "entity_order", "VARIANT"));
        insertEntityRow(3, "2026-09-15 10:00:00+00");
        insertEntityRow(3, "2026-10-15 10:00:00+00");
        DuckDBTestSupport.flushInlinedData(conn);

        // EntityReplayService globs bucket=N/**/*.parquet — the month level must sit below bucket.
        assertThat(dataFiles("entity_order"))
                .hasSize(2)
                .allSatisfy(f -> assertThat(f)
                        .matches(".*/main/entity_order/bucket=3/epoch_month=\\d+/[^/]+\\.parquet"));
    }

    @Test
    void generalRows_landInDayDirectories() throws SQLException {
        exec(SchemaManager.generalCassetteDdl("lake", "general_orders_events", "VARIANT"));
        insertGeneralRow("2026-10-01 10:00:00+00");
        insertGeneralRow("2026-10-02 10:00:00+00");
        DuckDBTestSupport.flushInlinedData(conn);

        assertThat(dataFiles("general_orders_events"))
                .hasSize(2)
                .allSatisfy(f -> assertThat(f)
                        .matches(".*/main/general_orders_events/epoch_day=\\d+/[^/]+\\.parquet"));
    }

    @Test
    void retentionDeleteOnADayBoundary_retiresWholeFilesWithoutDeleteFiles() throws SQLException {
        exec(SchemaManager.generalCassetteDdl("lake", "general_orders_events", "VARIANT"));
        insertGeneralRow("2026-10-01 10:00:00+00");
        insertGeneralRow("2026-10-02 10:00:00+00");
        DuckDBTestSupport.flushInlinedData(conn);

        // Same predicate shape RetentionService issues: recorded_at < cutoff.
        exec("DELETE FROM lake.main.general_orders_events "
                + "WHERE recorded_at < TIMESTAMPTZ '2026-10-02 00:00:00+00'");

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*), count(delete_file) "
                     + "FROM ducklake_list_files('lake', 'general_orders_events')")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).as("the 2026-10-01 file is retired, the 2026-10-02 one stays").isEqualTo(1);
            assertThat(rs.getLong(2)).as("nothing left for ducklake_rewrite_data_files to do").isZero();
        }
    }

    private void insertEntityRow(int bucket, String recordedAt) throws SQLException {
        exec("INSERT INTO lake.main.entity_order "
                + "(recorded_at, entity_id, bucket, topic, kafka_offset, kafka_partition, kafka_timestamp) "
                + "VALUES (TIMESTAMPTZ '" + recordedAt + "', 'order-1', " + bucket + ", 'orders.events', 0, 0, "
                + "TIMESTAMPTZ '" + recordedAt + "')");
    }

    private void insertGeneralRow(String recordedAt) throws SQLException {
        exec("INSERT INTO lake.main.general_orders_events "
                + "(recorded_at, kafka_offset, kafka_partition, kafka_timestamp) "
                + "VALUES (TIMESTAMPTZ '" + recordedAt + "', 0, 0, TIMESTAMPTZ '" + recordedAt + "')");
    }

    private List<String> dataFiles(String table) throws SQLException {
        return strings("SELECT data_file FROM ducklake_list_files('lake', ?)", table);
    }
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `mvn -B -q -pl joxette-service -am test -Dtest='SchemaManagerDdlTest,DuckLakeTableLayoutTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL. The string tests expect `PARTITIONED BY (epoch_day(recorded_at))` and `(bucket, epoch_month(recorded_at))`; the layout tests find no `epoch_*` directory and a partition-key list of just `[bucket]`.

- [ ] **Step 4: Implement**

In `SchemaManager`, replace the `ENTITY_PARTITIONED_BY` constant and its comment with:

```java
    /**
     * Time partitions follow {@code recorded_at} because that is what RetentionService's
     * DELETE filters on: once a cutoff passes a partition boundary, DuckLake retires that
     * partition's files whole — no delete files, nothing to rewrite. General tables use
     * days; entity tables use months, because they already fan out into one directory per
     * bucket and daily partitions would multiply small files by 256.
     */
    static final String GENERAL_PARTITIONED_BY = "epoch_day(recorded_at)";
    static final String ENTITY_PARTITIONED_BY  = "bucket, epoch_month(recorded_at)";
```

In `generalCassetteDdl`, change the closing of the text block from:

```java
            )
            SORTED BY (%s)
            """, catalog, tableName, flexType, GENERAL_SORTED_BY);
```

to:

```java
            )
            PARTITIONED BY (%s)
            SORTED BY (%s)
            """, catalog, tableName, flexType, GENERAL_PARTITIONED_BY, GENERAL_SORTED_BY);
```

Update the entity class javadoc line "Records Kafka messages for a specific entity type, partitioned by bucket." to "Records Kafka messages for a specific entity type, partitioned by bucket and month."

- [ ] **Step 5: Run the tests to verify they pass**

Run: `mvn -B -q -pl joxette-service -am test -Dtest='SchemaManagerDdlTest,DuckLakeTableLayoutTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: BUILD SUCCESS. The `DuckLakeTableLayoutTest` report shows `tests="6"` and `failures="0"`.

- [ ] **Step 6: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/db/SchemaManager.java joxette-service/src/test/java/com/joxette/db/
git commit -m "$(cat <<'EOF'
Time-partition cassette tables by recorded_at

General tables partition by epoch_day(recorded_at), entity tables by
(bucket, epoch_month(recorded_at)). Retention deletes on recorded_at, so
once the cutoff passes a partition boundary DuckLake retires those files
whole instead of writing delete files that need a rewrite.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Skip min/max statistics on payload columns

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/db/SchemaManager.java`
- Modify: `joxette-service/src/test/java/com/joxette/db/SchemaManagerDdlTest.java`
- Modify: `joxette-service/src/test/java/com/joxette/db/DuckLakeTableLayoutTest.java`

**Interfaces:**
- Consumes: Task 1/2 DDL builders and test helpers (`exec`, `insertEntityRow`)
- Produces: `static final String SchemaManager.PAYLOAD_COLUMNS_WITHOUT_STATS = "'kafka_value', 'headers'"`

- [ ] **Step 1: Write the failing tests**

Add to `SchemaManagerDdlTest`:

```java
    @Test
    void ddl_skipsStatsOnlyOnUnfilteredNonVariantPayloadColumns() {
        for (String ddl : new String[] {
                SchemaManager.generalCassetteDdl("lake", "general_orders_events", "VARIANT"),
                SchemaManager.entityCassetteDdl("lake", "entity_order", "VARIANT")}) {
            assertThat(ddl)
                    .contains("WITH (skip_stats_columns = ['kafka_value', 'headers'])")
                    // VARIANT columns can't skip stats; kafka_key is filtered by the $.key pushdown
                    .doesNotContain("'metadata'")
                    .doesNotContain("'kafka_key'");
        }
    }
```

Add to `DuckLakeTableLayoutTest`:

```java
    @Test
    void payloadColumns_getNoMinMaxStats_whileFilteredColumnsKeepThem() throws SQLException {
        exec(SchemaManager.entityCassetteDdl("lake", "entity_order", "VARIANT"));
        exec("INSERT INTO lake.main.entity_order "
                + "(recorded_at, entity_id, bucket, topic, kafka_offset, kafka_partition, kafka_timestamp, kafka_value) "
                + "VALUES (TIMESTAMPTZ '2026-10-01 10:00:00+00', 'order-1', 0, 'orders.events', 0, 0, "
                + "TIMESTAMPTZ '2026-10-01 10:00:00+00', 'payload'::BLOB)");
        DuckDBTestSupport.flushInlinedData(conn);

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM lake.options() "
                     + "WHERE option_name = 'skip_stats_columns' AND scope = 'TABLE' "
                     + "AND scope_entry = 'main.entity_order'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).as("skip_stats_columns is set on the table").isEqualTo(1);
        }
        assertThat(minValue("entity_order", "kafka_value")).isNull();
        assertThat(minValue("entity_order", "entity_id")).isEqualTo("order-1");
    }

    private String minValue(String table, String column) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                SELECT max(fcs.min_value)
                FROM __ducklake_metadata_lake.ducklake_file_column_stats fcs
                JOIN __ducklake_metadata_lake.ducklake_table t
                  ON t.table_id = fcs.table_id AND t.end_snapshot IS NULL
                JOIN __ducklake_metadata_lake.ducklake_column c
                  ON c.table_id = fcs.table_id AND c.column_id = fcs.column_id AND c.end_snapshot IS NULL
                WHERE t.table_name = ? AND c.column_name = ?
                """)) {
            ps.setString(1, table);
            ps.setString(2, column);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `mvn -B -q -pl joxette-service -am test -Dtest='SchemaManagerDdlTest,DuckLakeTableLayoutTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL. No `WITH (skip_stats_columns …)` in the DDL; the options count is 0.

- [ ] **Step 3: Implement**

Add the constant in `SchemaManager`:

```java
    /**
     * Columns DuckLake records no min/max bounds for (row counts are still kept). Only
     * columns nothing filters on: kafka_key stays because the transform pushdown filters
     * on {@code $.key}. VARIANT columns ({@code metadata}) can't skip stats at all.
     */
    static final String PAYLOAD_COLUMNS_WITHOUT_STATS = "'kafka_value', 'headers'";
```

In both `generalCassetteDdl` and `entityCassetteDdl`, add `WITH (skip_stats_columns = [%s])` as the last clause (it must follow `SORTED BY`), and pass `PAYLOAD_COLUMNS_WITHOUT_STATS` as the last format argument. The general tail becomes:

```java
            )
            PARTITIONED BY (%s)
            SORTED BY (%s)
            WITH (skip_stats_columns = [%s])
            """, catalog, tableName, flexType, GENERAL_PARTITIONED_BY, GENERAL_SORTED_BY,
                PAYLOAD_COLUMNS_WITHOUT_STATS);
```

and the entity tail becomes:

```java
            )
            PARTITIONED BY (%s)
            SORTED BY (%s)
            WITH (skip_stats_columns = [%s])
            """, catalog, tableName, flexType, ENTITY_PARTITIONED_BY, ENTITY_SORTED_BY,
                PAYLOAD_COLUMNS_WITHOUT_STATS);
```

- [ ] **Step 4: Run to verify pass**

Run: `mvn -B -q -pl joxette-service -am test -Dtest='SchemaManagerDdlTest,DuckLakeTableLayoutTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/db/SchemaManager.java joxette-service/src/test/java/com/joxette/db/
git commit -m "$(cat <<'EOF'
Skip min/max stats on cassette payload columns

kafka_value and headers are never filtered on, so their per-file bounds
only grow the catalog. kafka_key keeps its stats for the $.key pushdown;
metadata is VARIANT, which DuckLake can't skip stats for.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Remove the legacy bucket-layout migration

Tables are now partitioned from creation and old catalogs are not supported, so `CompactionService.migrateLegacyPartitionFiles` is dead. It was also the remaining unbounded `ducklake_rewrite_data_files` call in compaction. Its `DuckDbSession` was the only user of the `compactionDuckDbSession` bean.

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/compaction/CompactionService.java`
- Modify: `joxette-service/src/main/java/com/joxette/config/DuckDBConfig.java`
- Modify: `joxette-service/src/main/java/com/joxette/db/SchemaManager.java`: `hasUnpartitionedFiles` javadoc only
- Modify: `joxette-service/src/test/java/com/joxette/compaction/CompactionServiceTest.java`
- Modify: `joxette-service/src/test/java/com/joxette/compaction/CompactionLockRaceTest.java`
- Delete: `joxette-service/src/test/java/com/joxette/it/EntityBucketPartitioningIT.java`

**Interfaces:**
- Produces: the constructor `CompactionService(Connection duckDB, JoxetteProperties props, ConfigRepository configRepo, JoxetteMetrics joxetteMetrics, CompactionLockManager lockManager)`. The `DuckDbSession` parameter is gone; Tasks 5 and 6 rely on this signature.

- [ ] **Step 1: Delete the migration code in `CompactionService`**

1. Delete the method `migrateLegacyPartitionFiles(String entityType)` and its whole javadoc (it starts `/**\n     * One-time migration: if {@code entity_{entityType}} still has any files`).
2. In `doCompactEntityType`, delete the line `migrateLegacyPartitionFiles(entityType);` and the blank line after it.
3. Delete the field `private final DuckDbSession        session;`, the constructor parameter `@Qualifier("compactionDuckDbSession") DuckDbSession session,` and the assignment `this.session               = session;`.
4. In the class javadoc's "Connection isolation" paragraph, replace
   `{@link #duckDB} and {@link #session} are <strong>not</strong> the connection every`
   with `{@link #duckDB} is <strong>not</strong> the connection every`, then replace
   `{@code compactionDuckDbConnection} / {@code compactionDuckDbSession} from`
   with `{@code compactionDuckDbConnection} from`. Change the next line's "they are" to "it is" if the sentence needs it to stay grammatical.
5. Keep `import com.joxette.db.DuckDbSession;` (`DuckDbSession.rollbackQuietly` is still used). Remove `import com.joxette.db.SchemaManager;` only if `SchemaManager.validateEntityType` is no longer referenced (it is, so it stays).

- [ ] **Step 2: Delete the bean in `DuckDBConfig`**

Delete:

```java
    /** {@link DuckDbSession} wrapping {@link #compactionDuckDbConnection}. */
    @Bean
    public DuckDbSession compactionDuckDbSession(
            @Qualifier("compactionDuckDbConnection") Connection compactionDuckDbConnection) {
        return new DuckDbSession(compactionDuckDbConnection);
    }
```

Then run `git grep -n 'DuckDbSession' -- joxette-service/src/main/java/com/joxette/config/DuckDBConfig.java`. If nothing else uses the import, remove it.

- [ ] **Step 3: Update `hasUnpartitionedFiles`' javadoc in `SchemaManager`**

Replace the paragraph starting `<p>Used by {@code CompactionService} to decide whether a one-time` with:

```java
     * <p>Used by {@code EntityReplayService} to decide whether the fast bucket-glob
     * file-count path is safe. Tables declare their partitioning in the CREATE, so
     * this is {@code false} for every table created by this version.
```

- [ ] **Step 4: Update the tests**

```bash
perl -0pi -e 's/,\s*new com\.joxette\.db\.DuckDbSession\((?:[^()]|\([^()]*\))*\)//g' \
  joxette-service/src/test/java/com/joxette/compaction/CompactionServiceTest.java \
  joxette-service/src/test/java/com/joxette/compaction/CompactionLockRaceTest.java
git grep -n 'DuckDbSession' -- joxette-service/src/test/java/com/joxette/compaction/CompactionServiceTest.java joxette-service/src/test/java/com/joxette/compaction/CompactionLockRaceTest.java
git rm joxette-service/src/test/java/com/joxette/it/EntityBucketPartitioningIT.java
```

Expected from the `git grep`: no output.

In `CompactionServiceTest`, delete the comment block headed `// Legacy-file migration (bucket partitioning) — graceful degradation` and the test `executeRun_migrationCheckFails_stillCompletesRun` that follows it.

- [ ] **Step 5: Run the compaction tests**

Run: `mvn -B -q -pl joxette-service -am test -Dtest='CompactionServiceTest,CompactionLockRaceTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: BUILD SUCCESS.

Run: `git grep -n -E 'migrateLegacyPartitionFiles|compactionDuckDbSession|EntityBucketPartitioningIT' -- joxette-service`
Expected: no output.

- [ ] **Step 6: Commit**

```bash
git add -A joxette-service/src
git commit -m "$(cat <<'EOF'
Remove the legacy bucket-layout migration from compaction

Cassette tables now declare their partitioning when they are created,
so there are no flat files left to migrate. Drops the migration, its
unbounded ducklake_rewrite_data_files call, and the compaction
DuckDbSession bean only it used.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Honour lookback-days with older_than

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/compaction/CompactionService.java`: add `mergeSql`, change `executeMergeBatch` and its two callers
- Modify: `joxette-service/src/main/java/com/joxette/config/JoxetteProperties.java`: `Compaction.Entity.lookbackDays` and `Compaction.General.lookbackDays` defaults 30 → 1
- Modify: `joxette-service/src/test/java/com/joxette/support/DuckDBTestSupport.java`: add `filesProcessed`, `dataFileCount`
- Create: `joxette-service/src/test/java/com/joxette/compaction/CompactionMergeSqlTest.java`

**Interfaces:**
- Consumes: `DuckDBTestSupport.newDuckLakeConnection`, `flushInlinedData` (Task 1); the constructor from Task 4
- Produces:
  - `static String CompactionService.mergeSql(String tableName, long maxFileSizeBytes, int batchSize, int lookbackDays)`
  - `public static long DuckDBTestSupport.filesProcessed(Connection conn, String maintenanceSql)`
  - `public static long DuckDBTestSupport.dataFileCount(Connection conn, String table)`

- [ ] **Step 1: Add test helpers to `DuckDBTestSupport`**

```java
    /**
     * Runs a {@code ducklake_merge_adjacent_files} / {@code ducklake_rewrite_data_files}
     * call and returns the summed {@code files_processed} (0 when it returns no rows).
     */
    public static long filesProcessed(Connection conn, String maintenanceSql) throws SQLException {
        long total = 0;
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(maintenanceSql)) {
            while (rs.next()) total += rs.getLong("files_processed");
        }
        return total;
    }

    /** Live data files DuckLake tracks for {@code lake.main.<table>}. */
    public static long dataFileCount(Connection conn, String table) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT count(*) FROM ducklake_list_files('lake', ?)")) {
            ps.setString(1, table);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }
```

Add `import java.sql.PreparedStatement;` and `import java.sql.ResultSet;` if the file doesn't already have them.

- [ ] **Step 2: Write the failing test**

Create `joxette-service/src/test/java/com/joxette/compaction/CompactionMergeSqlTest.java`:

```java
package com.joxette.compaction;

import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/** {@link CompactionService#mergeSql} as text, and run against a real DuckLake catalog. */
class CompactionMergeSqlTest {

    private static final long TARGET_FILE_SIZE = 256L * 1024 * 1024;

    @TempDir
    Path dir;

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DuckDBTestSupport.newDuckLakeConnection(dir);
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE lake.main.t (id INTEGER)");
            st.execute("INSERT INTO lake.main.t VALUES (1)");
        }
        DuckDBTestSupport.flushInlinedData(conn);
        try (Statement st = conn.createStatement()) {
            st.execute("INSERT INTO lake.main.t VALUES (2)");
        }
        DuckDBTestSupport.flushInlinedData(conn); // two small files, both just written
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null) conn.close();
    }

    @Test
    void mergeSql_withoutLookback_hasNoOlderThan() {
        assertThat(CompactionService.mergeSql("t", TARGET_FILE_SIZE, 8, 0))
                .isEqualTo("CALL ducklake_merge_adjacent_files('lake', 't', max_compacted_files => 8, "
                        + "max_file_size => 268435456)");
    }

    @Test
    void mergeSql_withLookback_passesOlderThan() {
        assertThat(CompactionService.mergeSql("t", TARGET_FILE_SIZE, 8, 7))
                .isEqualTo("CALL ducklake_merge_adjacent_files('lake', 't', max_compacted_files => 8, "
                        + "max_file_size => 268435456, older_than => (now() - INTERVAL '7 days')::TIMESTAMP)");
    }

    @Test
    void lookback_leavesFilesNewerThanTheCutoffAlone() throws SQLException {
        assertThat(DuckDBTestSupport.filesProcessed(conn, CompactionService.mergeSql("t", TARGET_FILE_SIZE, 8, 1)))
                .isZero();
        assertThat(DuckDBTestSupport.dataFileCount(conn, "t")).isEqualTo(2);
    }

    @Test
    void noLookback_mergesTheSameFiles() throws SQLException {
        assertThat(DuckDBTestSupport.filesProcessed(conn, CompactionService.mergeSql("t", TARGET_FILE_SIZE, 8, 0)))
                .isEqualTo(2);
        assertThat(DuckDBTestSupport.dataFileCount(conn, "t")).isEqualTo(1);
    }
}
```

- [ ] **Step 3: Run to verify failure**

Run: `mvn -B -q -pl joxette-service -am test -Dtest='CompactionMergeSqlTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation FAILURE, `cannot find symbol … method mergeSql`.

- [ ] **Step 4: Implement**

In `CompactionService`, replace `executeMergeBatch` (javadoc included) with:

```java
    /**
     * Runs one bounded {@code ducklake_merge_adjacent_files} call, capped to at most
     * {@code batchSize} compacted-file operations via {@code max_compacted_files}, and
     * limited to files older than {@code lookbackDays} (see {@link #mergeSql}).
     *
     * <p>Heals the connection on failure: {@code CALL ducklake_merge_adjacent_files}
     * can leave an aborted transaction behind exactly like any other statement (see
     * {@link DuckDbSession}'s class javadoc), and a retry against a still-wedged
     * connection would fail immediately regardless of whether the underlying I/O issue
     * was transient — defeating the retry this exists to support.
     */
    private FileStats executeMergeBatch(String tableName, long maxFileSizeBytes, int batchSize,
                                        int lookbackDays) throws SQLException {
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery(mergeSql(tableName, maxFileSizeBytes, batchSize, lookbackDays))) {
            FileStats stats = FileStats.EMPTY;
            while (rs.next()) {
                stats = stats.add(new FileStats(
                        rs.getLong("files_processed"),
                        rs.getLong("files_created")));
            }
            return stats;
        } catch (SQLException e) {
            DuckDbSession.rollbackQuietly(duckDB, "ducklake_merge_adjacent_files batch for " + tableName);
            throw e;
        }
    }

    /**
     * The merge call for one batch. {@code older_than} (DuckLake 1.1) selects files by when
     * they entered the lake, so with {@code lookbackDays > 0} files still being added to by
     * inline flushes are left alone until they have settled; {@code 0} merges everything.
     */
    static String mergeSql(String tableName, long maxFileSizeBytes, int batchSize, int lookbackDays) {
        StringBuilder sql = new StringBuilder("CALL ducklake_merge_adjacent_files('lake', '")
                .append(tableName).append("', max_compacted_files => ").append(batchSize)
                .append(", max_file_size => ").append(maxFileSizeBytes);
        if (lookbackDays > 0) {
            sql.append(", older_than => (now() - INTERVAL '").append(lookbackDays).append(" days')::TIMESTAMP");
        }
        return sql.append(')').toString();
    }
```

In `doCompactEntityType`, add `int lookbackDays = props.getCompaction().getEntity().getLookbackDays();` next to `maxBatches`, and change the attempt lambda to:

```java
                        () -> executeMergeBatch("entity_" + entityType, maxFileSizeBytes, batchSize, lookbackDays),
```

In `doCompactGeneralTopic`, change the attempt lambda to:

```java
                        () -> executeMergeBatch(tableName, maxFileSizeBytes, batchSize, general.getLookbackDays()),
```

In `JoxetteProperties`, change both `private int lookbackDays = 30;` (in `Compaction.Entity` and `Compaction.General`) to:

```java
            /**
             * Merge only files that entered the lake more than this many days ago, so
             * compaction leaves files still being added to by inline flushes alone. Passed
             * to {@code ducklake_merge_adjacent_files} as {@code older_than}; 0 = no limit.
             */
            private int lookbackDays = 1;
```

- [ ] **Step 5: Run to verify pass**

Run: `mvn -B -q -pl joxette-service -am test -Dtest='CompactionMergeSqlTest,CompactionServiceTest,CompactionLockRaceTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: BUILD SUCCESS. (`CompactionServiceTest.testProperties()` sets `lookbackDays(0)` for entity, and its general-merge SQL assertion uses `contains`, so it is unaffected.)

- [ ] **Step 6: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/compaction/CompactionService.java \
  joxette-service/src/main/java/com/joxette/config/JoxetteProperties.java \
  joxette-service/src/test/java/com/joxette/support/DuckDBTestSupport.java \
  joxette-service/src/test/java/com/joxette/compaction/CompactionMergeSqlTest.java
git commit -m "$(cat <<'EOF'
Honour compaction lookback-days via older_than

lookback-days was documented and exposed but never read. DuckLake 1.1's
older_than lets the merge skip files that entered the lake recently.
Defaults drop from 30 to 1 day, so a month of small files no longer sits
in the window replay reads most.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Bound retention's rewrite with max_compacted_files

`RetentionService.rewriteDataFiles` makes one uncapped `ducklake_rewrite_data_files` call per table, the failure mode compaction already fixed for merges. This task reuses compaction's bounded-batch-with-retry loop.

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/compaction/CompactionService.java`: extract `static runInBatches`
- Modify: `joxette-service/src/main/java/com/joxette/compaction/RetentionService.java`: `rewriteSql`, `rewriteDataFiles`, `executeRewriteBatch`
- Modify: `joxette-service/src/main/java/com/joxette/config/JoxetteProperties.java`: `Retention.maxRewrittenFilesPerBatch`, `Retention.maxBatchesPerRun`
- Create: `joxette-service/src/test/java/com/joxette/compaction/RetentionRewriteSqlTest.java`

**Interfaces:**
- Consumes: `CompactionService.FileStats`, `CompactionService.MergeBatchAttempt` (existing, package-private); `DuckDBTestSupport.newDuckLakeConnection`, `flushInlinedData`, `filesProcessed` (Tasks 1, 5)
- Produces:
  - `static CompactionService.FileStats CompactionService.runInBatches(MergeBatchAttempt attempt, int maxBatches, int maxRetriesPerBatch, long baseDelayMs)`
  - `static String RetentionService.rewriteSql(String tableName, double deleteThreshold, int batchSize)`
  - `int JoxetteProperties.Retention.getMaxRewrittenFilesPerBatch()` (default 8)
  - `int JoxetteProperties.Retention.getMaxBatchesPerRun()` (default 1000)

- [ ] **Step 1: Write the failing test**

Create `joxette-service/src/test/java/com/joxette/compaction/RetentionRewriteSqlTest.java`:

```java
package com.joxette.compaction;

import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/** {@link RetentionService#rewriteSql} as text, and run against a real DuckLake catalog. */
class RetentionRewriteSqlTest {

    @TempDir
    Path dir;

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DuckDBTestSupport.newDuckLakeConnection(dir);
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE lake.main.t (id INTEGER)");
        }
        for (int file = 0; file < 3; file++) {
            try (Statement st = conn.createStatement()) {
                st.execute("INSERT INTO lake.main.t SELECT i FROM range(" + file * 10 + ", " + (file * 10 + 10) + ") r(i)");
            }
            DuckDBTestSupport.flushInlinedData(conn);
        }
        try (Statement st = conn.createStatement()) {
            st.execute("DELETE FROM lake.main.t WHERE id % 10 < 6"); // 60% of every file
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null) conn.close();
    }

    @Test
    void rewriteSql_capsFilesPerCall() {
        assertThat(RetentionService.rewriteSql("t", 0.5, 8))
                .isEqualTo("CALL ducklake_rewrite_data_files('lake', 't', delete_threshold => 0.5, "
                        + "max_compacted_files => 8)");
    }

    @Test
    void rewriteSql_rewritesDeleteHeavyFiles_thenFindsNothingLeft() throws SQLException {
        assertThat(DuckDBTestSupport.filesProcessed(conn, RetentionService.rewriteSql("t", 0.5, 8))).isEqualTo(3);
        assertThat(DuckDBTestSupport.filesProcessed(conn, RetentionService.rewriteSql("t", 0.5, 8))).isZero();

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(delete_file) FROM ducklake_list_files('lake', 't')")) {
            rs.next();
            assertThat(rs.getLong(1)).as("no delete files left after the rewrite").isZero();
        }
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `mvn -B -q -pl joxette-service -am test -Dtest='RetentionRewriteSqlTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation FAILURE, `cannot find symbol … method rewriteSql`.

- [ ] **Step 3: Extract the batch loop in `CompactionService`**

Replace the body of `mergeInBatches` with a delegation, and move the loop into a static method. `runOneBatchWithRetry` becomes static; `sleepBackoff` already is:

```java
    FileStats mergeInBatches(MergeBatchAttempt attempt, int maxBatches, int maxRetriesPerBatch, long baseDelayMs) {
        return runInBatches(attempt, maxBatches, maxRetriesPerBatch, baseDelayMs);
    }

    /**
     * The bounded-batch loop behind {@link #mergeInBatches}, shared with
     * {@link RetentionService}'s {@code ducklake_rewrite_data_files} calls, which fail
     * the same way when one call covers a whole table.
     */
    static FileStats runInBatches(MergeBatchAttempt attempt, int maxBatches, int maxRetriesPerBatch, long baseDelayMs) {
        FileStats total = FileStats.EMPTY;
        for (int batch = 1; batch <= maxBatches; batch++) {
            FileStats batchStats = runOneBatchWithRetry(attempt, maxRetriesPerBatch, baseDelayMs);
            if (batchStats == null) {
                break; // retries exhausted, or a non-transient failure — stop, keep what we have
            }
            total = total.add(batchStats);
            if (batchStats.filesProcessed() == 0) {
                break; // nothing left to compact
            }
        }
        return total;
    }
```

Change `private FileStats runOneBatchWithRetry(` to `private static FileStats runOneBatchWithRetry(`. Keep the existing javadoc on `mergeInBatches`.

- [ ] **Step 4: Add the retention properties**

In `JoxetteProperties.Retention`, after `rewriteDeleteThreshold`:

```java
        /**
         * Caps one {@code ducklake_rewrite_data_files} call via its {@code max_compacted_files}
         * parameter, which bounds the files a call writes (it may read more). Same reason as
         * compaction's {@code max-compacted-files-per-batch}: a failed object-store read then
         * costs one small batch, retried, instead of the whole table's rewrite.
         */
        private int maxRewrittenFilesPerBatch = 8;

        /** Safety cap on bounded rewrite calls per table per retention run. */
        private int maxBatchesPerRun = 1000;

        public int getMaxRewrittenFilesPerBatch() { return maxRewrittenFilesPerBatch; }
        public void setMaxRewrittenFilesPerBatch(int maxRewrittenFilesPerBatch) {
            this.maxRewrittenFilesPerBatch = maxRewrittenFilesPerBatch;
        }

        public int getMaxBatchesPerRun() { return maxBatchesPerRun; }
        public void setMaxBatchesPerRun(int maxBatchesPerRun) { this.maxBatchesPerRun = maxBatchesPerRun; }
```

- [ ] **Step 5: Bound the rewrite in `RetentionService`**

Replace the `rewriteDataFiles` method (javadoc included) with:

```java
    private static final int REWRITE_BATCH_RETRY_ATTEMPTS = 3;
    private static final long REWRITE_BATCH_RETRY_BASE_DELAY_MS = 500;

    /**
     * Rewrites Parquet files with a high delete-marker ratio for {@code tableName} via
     * {@code ducklake_rewrite_data_files}, so tombstoned rows are reclaimed from object
     * storage instead of only being logically deleted. Called after a bulk retention delete;
     * partitions the cutoff passed entirely were already retired by the DELETE itself, so this
     * mostly handles the partition the cutoff falls inside.
     *
     * <p>Bounded, retried batches via {@link CompactionService#runInBatches}, for the reason
     * given on {@code CompactionService.doCompactEntityType}. Never throws: a failing batch is
     * logged there and earlier batches' work is kept.
     */
    private void rewriteDataFiles(String tableName, String label) {
        var retention = props.getRetention();
        log.debug("Rewriting delete-heavy files for {} ({})", label, tableName);
        synchronized (lake) {
            CompactionService.FileStats stats = CompactionService.runInBatches(
                    () -> executeRewriteBatch(tableName, retention.getRewriteDeleteThreshold(),
                            retention.getMaxRewrittenFilesPerBatch()),
                    retention.getMaxBatchesPerRun(), REWRITE_BATCH_RETRY_ATTEMPTS, REWRITE_BATCH_RETRY_BASE_DELAY_MS);
            log.debug("Rewrite complete for {} ({}): files_processed={} files_created={}",
                    label, tableName, stats.filesProcessed(), stats.filesCreated());
        }
    }

    private CompactionService.FileStats executeRewriteBatch(String tableName, double deleteThreshold,
                                                            int batchSize) throws SQLException {
        try (Statement st = lake.createStatement();
             ResultSet rs = st.executeQuery(rewriteSql(tableName, deleteThreshold, batchSize))) {
            CompactionService.FileStats stats = CompactionService.FileStats.EMPTY;
            while (rs.next()) {
                stats = stats.add(new CompactionService.FileStats(
                        rs.getLong("files_processed"), rs.getLong("files_created")));
            }
            return stats;
        } catch (SQLException e) {
            // The connection must not inherit the failure, or its next statement (the retry)
            // fails with "Current transaction is aborted".
            DuckDbSession.rollbackQuietly(lake, "retention: ducklake_rewrite_data_files for " + tableName);
            throw e;
        }
    }

    static String rewriteSql(String tableName, double deleteThreshold, int batchSize) {
        return "CALL ducklake_rewrite_data_files('lake', '" + tableName + "',"
             + " delete_threshold => " + deleteThreshold
             + ", max_compacted_files => " + batchSize + ")";
    }
```

`RetentionService` already imports `java.sql.*` and `DuckDbSession`. `DuckDbErrors` may become unused in this file; remove its import only if `git grep -n DuckDbErrors -- joxette-service/src/main/java/com/joxette/compaction/RetentionService.java` shows just the import.

- [ ] **Step 6: Run to verify pass**

Run: `mvn -B -q -pl joxette-service -am test -Dtest='RetentionRewriteSqlTest,RetentionServiceRewriteTest,CompactionServiceTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: BUILD SUCCESS. `RetentionServiceRewriteTest` still sees the "Rewriting delete-heavy files" debug line. Its fake lake has no `ducklake_rewrite_data_files`, so the batch fails non-transiently, is logged by `CompactionService`, and the run completes.

- [ ] **Step 7: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/compaction/CompactionService.java \
  joxette-service/src/main/java/com/joxette/compaction/RetentionService.java \
  joxette-service/src/main/java/com/joxette/config/JoxetteProperties.java \
  joxette-service/src/test/java/com/joxette/compaction/RetentionRewriteSqlTest.java
git commit -m "$(cat <<'EOF'
Bound retention's ducklake_rewrite_data_files calls

DuckLake 1.1 adds max_compacted_files to the rewrite. Retention now runs
it through compaction's bounded, retried batch loop, so one failed
object-store read costs a small batch instead of the whole table.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: Update docs and config, then run the full suite

**Files:**
- Modify: `CLAUDE.md`
- Modify: `README.md`
- Modify: `joxette-service/src/main/resources/application.yml`

- [ ] **Step 1: `application.yml`**

Under `compaction.entity:` and `compaction.general:`, change `lookback-days: 30` to:

```yaml
      lookback-days: 1  # merge only files that entered the lake over N days ago (older_than); 0 = all
```

Under `retention:` (after `schedule:`), add:

```yaml
    # rewrite-delete-threshold: 0.1
    # max-rewritten-files-per-batch: 8  # caps one ducklake_rewrite_data_files call (max_compacted_files)
    # max-batches-per-run: 1000         # safety cap on rewrite batches per table per run
```

- [ ] **Step 2: `CLAUDE.md`**

1. Directly after the general cassette table's ```` ```sql ```` block, add:
   `Partitioned in DuckLake by `epoch_day(recorded_at)` and sorted by `(kafka_timestamp, kafka_partition, kafka_offset)`, both declared in the `CREATE TABLE`. Stats are skipped for `kafka_value` and `headers`.`
2. Replace `Partitioned in DuckLake by `(entity_bucket, date)`. Entity bucket = `hash(entity_type, entity_id) % configured_bucket_count`.` with:
   `Partitioned in DuckLake by `(bucket, epoch_month(recorded_at))` and sorted by `(entity_id, kafka_timestamp, recorded_at)`, both declared in the `CREATE TABLE`. Entity bucket = `hash(entity_type, entity_id) % configured_bucket_count`. Time partitions follow `recorded_at` because retention deletes on it: partitions wholly past the cutoff are retired without delete files.`
3. Delete the line `    time_partition  VARCHAR DEFAULT 'hour',` from the `topic_configs` DDL. The column doesn't exist.
4. In "Bucketed Entities", replace `Parquet files are partitioned by `(entity_bucket, date)`.` with `Parquet files are partitioned by `(bucket, epoch_month(recorded_at))`.`, and replace the layout block content `/{topic}/{date_or_hour}/{entity_bucket}_{sequence}.parquet` with:
   ```
   main/entity_{type}/bucket={n}/epoch_month={months_since_epoch}/ducklake-{uuid}.parquet
   main/general_{topic}/epoch_day={days_since_epoch}/ducklake-{uuid}.parquet
   ```
5. Replace `- **Lookback** — only compact data older than N days (default: 30) to avoid churning hot data` with `- **Lookback** — only merge files that entered the lake more than N days ago (default: 1), via `ducklake_merge_adjacent_files(older_than => …)`, so files still being added to are left alone`.
6. Change both `lookback-days: 30` occurrences in CLAUDE.md YAML blocks to `lookback-days: 1`.

- [ ] **Step 3: `README.md`**

Change `lookback-days: 30` to `lookback-days: 1`.

Run: `git grep -n -E 'lookback-days: 30|time_partition|entity_bucket, date' -- CLAUDE.md README.md joxette-service/src/main/resources`
Expected: no output.

- [ ] **Step 4: Full suite**

Run: `export JAVA_HOME=~/.sdkman/candidates/java/25.0.4-graal PATH=~/.sdkman/candidates/java/25.0.4-graal/bin:$PATH && mvn -B verify > /tmp/joxette-verify.log 2>&1; echo exit=$?; grep -E 'Tests run:.*(Failures|Errors): [1-9]|BUILD' /tmp/joxette-verify.log`
Expected: `exit=0` and `BUILD SUCCESS`. The trailing "Surefire is going to kill self fork JVM" line is pre-existing and not a failure.

If `docs/openapi.json` changed only in the server port and the `JsonNode` property order (springdoc noise), discard it: `git checkout docs/openapi.json`.

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md README.md joxette-service/src/main/resources/application.yml
git commit -m "$(cat <<'EOF'
Document DuckLake 1.1 table layout, lookback and retention batching

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

## Out of scope

- **Deletion vectors (`write_deletion_vectors`)**: experimental per DuckLake's own option text, and with them on a whole-file delete did not retire the file, which would undo Task 2's retention win.
- **Per-topic time-partition granularity**: no `time_partition` column exists; add one only if a topic needs hourly partitions.
- **Wiring `min-files-per-bucket` / `min-files-per-partition`**: unrelated to DuckLake 1.1.
- **Removing `SchemaManager.hasUnpartitionedFiles` and EntityReplayService's slow path**: always `false` for new tables, but removing it touches replay. Leave for a cleanup pass.
