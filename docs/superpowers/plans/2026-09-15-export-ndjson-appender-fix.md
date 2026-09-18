# ExportService NDJSON Appender Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `ExportService.exportNdjson`'s ~150x-slower-than-necessary DuckDB insert path by streaming records into a native `DuckDBAppender` instead of collecting a `List` and JDBC-batch-inserting it.

**Architecture:** Duplicate the shared `duckDB` connection once per export (mirroring `CassetteBatchWriter`'s existing per-writer connection pattern), use that one connection consistently for creating the temp table, appending rows via `DuckDBAppender`, running `COPY`, and dropping the table. `exportParquet` and `loadAll` are untouched — `exportNdjson` gets its own new streaming logic inline, not a shared helper.

**Tech Stack:** Java 25, DuckDB JDBC 1.5.5.1 (`org.duckdb.DuckDBAppender`, `org.duckdb.DuckDBConnection`), Spring Boot 4.1.1, JUnit 5, AssertJ.

## Global Constraints

- Wire format / output file contents must not change: each output line must be the same JSON `objectMapper.writeValueAsString(EntityRecord)` would produce today. The row-count return value and the "zero records → no file written" behavior must also be preserved exactly.
- Out of scope (per the approved spec, `docs/superpowers/specs/2026-09-15-export-ndjson-appender-design.md`): `exportParquet`, `loadAll`, jox-json/Jackson. None of these are touched by this plan.
- Every task's changes must be verified with `mvn -pl joxette-service test` (scoped to the new test, then the full module) before moving to the next task. Each task lands as its own commit.

## Task 1: Characterize current behavior with a direct test

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/exports/ExportService.java` (visibility only — no behavior change)
- Create: `joxette-service/src/test/java/com/joxette/exports/ExportServiceNdjsonTest.java`

**Interfaces:**
- Produces: `exportNdjson` becomes package-private (was `private`), same signature:
  `long exportNdjson(String jobId, String entityType, List<String> entityIds, Instant from, Instant to, List<String> messageTypes, String outputPath) throws Exception`.
  This is the only visibility change in this task — the method body is untouched here.

- [ ] **Step 1: Write the test**

Create `joxette-service/src/test/java/com/joxette/exports/ExportServiceNdjsonTest.java`:
```java
package com.joxette.exports;

import com.joxette.config.JoxetteProperties;
import com.joxette.replay.EntityReplayService;
import com.joxette.support.DuckDBTestSupport;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises {@link ExportService#exportNdjson} directly (package-private for
 * exactly this reason). No prior test called it directly -- this is new
 * coverage, not a pre-existing regression net.
 */
class ExportServiceNdjsonTest {

    private static final String ENTITY_TYPE = "ndjsontest";

    private Connection conn;
    private ExportService service;
    private Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        conn = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createEntityTable(conn, ENTITY_TYPE);
        EntityReplayService entityReplayService =
                new EntityReplayService(DSL.using(conn, SQLDialect.DUCKDB), conn, new JoxetteProperties());
        ObjectMapper objectMapper = new ObjectMapper();
        // repository/properties/taskRegistry are never touched by exportNdjson itself
        // (only by submit()/execute(), which this test bypasses) -- null is safe here.
        service = new ExportService(null, entityReplayService, conn, null, objectMapper, null);
        tempDir = Files.createTempDirectory("export-ndjson-test");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null && !conn.isClosed()) conn.close();
        if (tempDir != null) {
            Files.walk(tempDir).sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> p.toFile().delete());
        }
    }

    @Test
    void exportsAllRecordsAcrossMultipleEntities() throws Exception {
        DuckDBTestSupport.insertEntityRow(conn, ENTITY_TYPE, "entity-1", 0, "created",
                "orders.events", 0, 0L, Instant.parse("2026-01-01T00:00:00Z"),
                Instant.parse("2026-01-01T00:00:00.100Z"), "key-1",
                "{\"amount\":10}".getBytes(StandardCharsets.UTF_8));
        DuckDBTestSupport.insertEntityRow(conn, ENTITY_TYPE, "entity-1", 0, "updated",
                "orders.events", 0, 1L, Instant.parse("2026-01-01T00:01:00Z"),
                Instant.parse("2026-01-01T00:01:00.100Z"), "key-1",
                "{\"amount\":20}".getBytes(StandardCharsets.UTF_8));
        DuckDBTestSupport.insertEntityRow(conn, ENTITY_TYPE, "entity-2", 0, "created",
                "orders.events", 0, 2L, Instant.parse("2026-01-01T00:02:00Z"),
                Instant.parse("2026-01-01T00:02:00.100Z"), "key-2",
                "{\"amount\":30}".getBytes(StandardCharsets.UTF_8));

        String outputPath = tempDir.resolve("out.ndjson").toString();
        long count = service.exportNdjson("job-1", ENTITY_TYPE, List.of("entity-1", "entity-2"),
                null, null, null, outputPath);

        assertThat(count).isEqualTo(3L);

        List<String> lines = Files.readAllLines(java.nio.file.Path.of(outputPath));
        assertThat(lines).hasSize(3);

        ObjectMapper mapper = new ObjectMapper();
        List<String> entityIdsSeen = lines.stream().map(line -> {
            JsonNode node = mapper.readTree(line);
            assertThat(node.get("topic").asText()).isEqualTo("orders.events");
            return node.get("entityId").asText();
        }).toList();
        assertThat(entityIdsSeen).containsExactly("entity-1", "entity-1", "entity-2");
    }

    @Test
    void returnsZeroAndWritesNoFileWhenNoRecordsMatch() throws Exception {
        String outputPath = tempDir.resolve("empty.ndjson").toString();
        long count = service.exportNdjson("job-2", ENTITY_TYPE, List.of("no-such-entity"),
                null, null, null, outputPath);

        assertThat(count).isEqualTo(0L);
        assertThat(Files.exists(java.nio.file.Path.of(outputPath))).isFalse();
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -pl joxette-service test -Dtest=ExportServiceNdjsonTest`
Expected: `FAIL` — compile error, `exportNdjson` has private access in `ExportService`.

- [ ] **Step 3: Widen visibility only (no behavior change)**

In `joxette-service/src/main/java/com/joxette/exports/ExportService.java`, change:
```java
    private long exportNdjson(String jobId, String entityType, List<String> entityIds,
                              Instant from, Instant to, List<String> messageTypes,
                              String outputPath) throws Exception {
```
to:
```java
    // Package-private so ExportServiceNdjsonTest can exercise it directly.
    long exportNdjson(String jobId, String entityType, List<String> entityIds,
                      Instant from, Instant to, List<String> messageTypes,
                      String outputPath) throws Exception {
```

- [ ] **Step 4: Run the test to verify it passes against the current implementation**

Run: `mvn -pl joxette-service test -Dtest=ExportServiceNdjsonTest`
Expected: `Tests run: 2, Failures: 0, Errors: 0` — this confirms the test correctly characterizes today's behavior (List + JDBC batch insert) before Task 2 changes the implementation.

- [ ] **Step 5: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/exports/ExportService.java \
        joxette-service/src/test/java/com/joxette/exports/ExportServiceNdjsonTest.java
git commit -m "$(cat <<'EOF'
Add direct test coverage for ExportService.exportNdjson

Widens exportNdjson from private to package-private (same reasoning as
other package-private test hooks in this codebase, e.g.
SseReplayHandler.activeThreadCount) so this new test can call it
directly instead of going through the async submit()/
BackgroundTaskRegistry path. No behavior change yet -- this test
characterizes the current List + JDBC batch insert implementation
before the Appender rewrite in the next commit.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Rewrite `exportNdjson` to stream into a DuckDB Appender

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/exports/ExportService.java`

**Interfaces:**
- Consumes: `ExportServiceNdjsonTest` (Task 1) — must still pass unchanged, asserting on the same row count / file contents / empty-case behavior.
- No public signature changes.

- [ ] **Step 1: Add the new imports**

In `joxette-service/src/main/java/com/joxette/exports/ExportService.java`, add:
```java
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
```

- [ ] **Step 2: Replace `exportNdjson`'s body**

Replace:
```java
    // Package-private so ExportServiceNdjsonTest can exercise it directly.
    long exportNdjson(String jobId, String entityType, List<String> entityIds,
                      Instant from, Instant to, List<String> messageTypes,
                      String outputPath) throws Exception {
        List<EntityRecord> records = loadAll(entityType, entityIds, from, to, messageTypes);
        if (records.isEmpty()) {
            return 0L;
        }
        // Write NDJSON to a local/S3 path by building the content and writing via DuckDB.
        // For local paths write through a DuckDB COPY; for remote paths use the same COPY
        // path since DuckDB httpfs handles s3:// writes transparently.
        String tmpTable = "export_tmp_ndjson_" + jobId.replace("-", "_");
        try (Statement st = duckDB.createStatement()) {
            st.execute("""
                    CREATE TEMP TABLE %s (line VARCHAR)
                    """.formatted(tmpTable));

            try (PreparedStatement ps = duckDB.prepareStatement(
                    "INSERT INTO " + tmpTable + " VALUES (?)")) {
                for (EntityRecord r : records) {
                    ps.setString(1, objectMapper.writeValueAsString(r));
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            // DuckDB COPY … TO with FORMAT CSV and no separator/header gives one line per row.
            st.execute("COPY (SELECT line FROM " + tmpTable + ") TO '" + outputPath
                       + "' (FORMAT CSV, HEADER false, QUOTE '', DELIMITER '')");
            st.execute("DROP TABLE IF EXISTS " + tmpTable);
        }
        return records.size();
    }
```
with:
```java
    // Package-private so ExportServiceNdjsonTest can exercise it directly.
    long exportNdjson(String jobId, String entityType, List<String> entityIds,
                      Instant from, Instant to, List<String> messageTypes,
                      String outputPath) throws Exception {
        // Stream records straight into a DuckDB temp table via the native Appender --
        // ~150x faster than a JDBC PreparedStatement batch INSERT at this row count
        // (see docs/superpowers/specs/2026-09-15-export-ndjson-appender-design.md) --
        // then COPY that table out. For remote paths the same COPY path is used since
        // DuckDB httpfs handles s3:// writes transparently.
        //
        // A temp table is scoped to whichever connection creates it, so the whole
        // operation runs on one duplicated connection end to end, independent of the
        // shared duckDB connection -- mirrors CassetteBatchWriter's per-writer
        // connection and sidesteps needing synchronized(duckDB) for this operation.
        DuckDBConnection duckConn = duckDB.unwrap(DuckDBConnection.class);
        String tmpTable = "export_tmp_ndjson_" + jobId.replace("-", "_");
        try (DuckDBConnection conn = duckConn.duplicate()) {
            try (Statement st = conn.createStatement()) {
                st.execute("""
                        CREATE TEMP TABLE %s (line VARCHAR)
                        """.formatted(tmpTable));
            }

            long[] count = {0L};
            try (DuckDBAppender appender =
                    conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, tmpTable)) {
                for (String entityId : entityIds) {
                    entityReplayService.streamEntityEvents(
                            entityType, entityId, from, to,
                            record -> {
                                try {
                                    appender.beginRow();
                                    appender.append(objectMapper.writeValueAsString(record));
                                    appender.endRow();
                                    count[0]++;
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            },
                            TransformPipeline.IDENTITY, "",
                            Order.ASC, null, null, messageTypes);
                }
            }

            try (Statement st = conn.createStatement()) {
                if (count[0] == 0L) {
                    st.execute("DROP TABLE IF EXISTS " + tmpTable);
                    return 0L;
                }
                // DuckDB COPY … TO with FORMAT CSV and no separator/header gives one line per row.
                st.execute("COPY (SELECT line FROM " + tmpTable + ") TO '" + outputPath
                           + "' (FORMAT CSV, HEADER false, QUOTE '', DELIMITER '')");
                st.execute("DROP TABLE IF EXISTS " + tmpTable);
            }
            return count[0];
        }
    }
```

- [ ] **Step 3: Compile**

Run: `mvn -pl joxette-service compile`
Expected: `BUILD SUCCESS`. If it fails on the `createAppender`/`DuckDBAppender` call, double check the import is `org.duckdb.DuckDBAppender` (not `org.duckdb.DuckDBConnection.DuckDBAppender`) and that `conn` is declared as `DuckDBConnection` (not `java.sql.Connection`) — `createAppender` is a `DuckDBConnection`-specific method, not part of `java.sql.Connection`.

- [ ] **Step 4: Run `ExportServiceNdjsonTest` to verify identical behavior**

Run: `mvn -pl joxette-service test -Dtest=ExportServiceNdjsonTest`
Expected: `Tests run: 2, Failures: 0, Errors: 0` — same assertions as Task 1, now passing against the Appender-based implementation. This is the direct proof the rewrite preserves row count, per-line JSON content, and the empty-case contract.

If `returnsZeroAndWritesNoFileWhenNoRecordsMatch` fails because a file *was* created: check that the `count[0] == 0L` branch runs `DROP TABLE` and returns before reaching the `COPY` call — a stray `COPY` on an empty table would create an empty file, which the test's `Files.exists(...)` check would (correctly) catch as a regression from today's "no file at all" behavior.

- [ ] **Step 5: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/exports/ExportService.java
git commit -m "$(cat <<'EOF'
Stream exportNdjson records into a DuckDB Appender

Replaces loadAll() + JDBC PreparedStatement batch INSERT with a
duplicated connection (CassetteBatchWriter's existing pattern) and a
native DuckDBAppender fed directly from entityReplayService.
streamEntityEvents(...)'s sink -- no intermediate List<EntityRecord>.
The empty-result check moves from "before touching the DB" (checking
records.isEmpty()) to "after appending" (checking the row counter),
since streaming can't know the count in advance; the DROP-without-COPY
behavior for zero records is preserved.

ExportServiceNdjsonTest passes unchanged, confirming identical row
count, per-line JSON content, and empty-case behavior. exportParquet
and loadAll are untouched -- loadAll still backs exportParquet exactly
as before.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Final verification

**Files:** None — verification only.

**Interfaces:** None.

- [ ] **Step 1: Confirm `loadAll` and `exportParquet` are unchanged**

Run:
```bash
git diff HEAD~2 -- joxette-service/src/main/java/com/joxette/exports/ExportService.java | grep -A3 -B3 "exportParquet\|private List<EntityRecord> loadAll"
```
Expected: no output (neither method appears in the diff across the last two commits) — confirms this plan touched only `exportNdjson`'s visibility and body.

- [ ] **Step 2: Run the full `joxette-service` test suite**

Run: `mvn -pl joxette-service test`
Expected: `BUILD SUCCESS`, all green, including `ExportServiceNdjsonTest`, `ExportControllerTest`, and `ExportJobRepositoryTest` (the existing tests covering `ExportService`'s public surface, unaffected by this internal change).

- [ ] **Step 3: Run the integration suite**

Run: `mvn -pl joxette-service verify`
Expected: `BUILD SUCCESS`. No existing integration test currently exercises the NDJSON export path end-to-end through the real `/exports` REST API (confirm by checking whether any `*IT.java` file references `ExportOutputFormat.NDJSON` or the `/exports` endpoints with an NDJSON format) — if one exists, it's the strongest end-to-end confirmation; if not, `ExportServiceNdjsonTest`'s direct coverage plus the full unit suite is the available evidence, consistent with how this plan scoped testing.

- [ ] **Step 4: Manual smoke check (optional, only if a populated entity type is available locally)**

If a running instance with real entity data is available:
```bash
curl -s -X POST -H "Content-Type: application/json" \
  -d '{"entityIds":["<some-entity-id>"],"outputFormat":"NDJSON"}' \
  "http://localhost:<port>/v1/entities/<entityType>/exports"
```
then poll `GET /v1/entities/<entityType>/exports/<jobId>` until `status` is `COMPLETED`, and inspect the resulting file. If no populated instance is available, rely on Step 2/3's automated coverage instead and note that in the final commit message.

- [ ] **Step 5: No commit needed**

This task is verification-only; nothing changes. If Steps 1–3 all pass, the fix is complete.
