# Catalog/Object-Storage Reconciliation — Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a scheduled + on-demand catalog/object-storage drift audit (orphaned files, missing files) across every `lake.main.*` cassette table, with a `reconciliation_history` record, two Prometheus gauges, and opt-in per-file orphan recovery.

**Architecture:** New `com.joxette.reconciliation` package mirroring `com.joxette.compaction`'s `RetentionService`/`RetentionScheduler` shape exactly (`AtomicBoolean` run guard, `insertRunRecord`/`updateRunRecord`/`getHistory`/`getStatus`/`beginRun`/`executeRun`), plus the existing `CompactionLockManager` reused as-is for cross-instance mutual exclusion. Endpoints are added to the existing `CompactionController` (`/compaction/reconciliation-status`, `/compaction/reconciliation-history`, `/compaction/trigger-reconciliation`), following the precedent retention already set for sharing that controller/namespace.

**Tech Stack:** Java 25, Spring Boot 4, DuckDB JDBC 1.5.5.1, DuckLake extension (`ducklake_delete_orphaned_files`, `ducklake_list_files`, `ducklake_add_data_files`, `parquet_file_metadata`, `glob`), JUnit 5 + AssertJ + Mockito, Testcontainers MinIO for integration tests, Jackson `ObjectMapper`.

## Global Constraints

- `synchronized(duckDB)` wraps every JDBC `Statement`/`PreparedStatement` execution against the shared connection — no exceptions, per `CLAUDE.md`'s DuckDB connection model.
- `CALL ducklake_delete_orphaned_files('lake', cleanup_all => true, dry_run => true)` — **both** `cleanup_all => true` and `dry_run => true` are required together. `dry_run => true` alone silently returns zero rows even when a real orphaned file exists — verified directly against a live local DuckLake catalog while writing the design spec (`docs/superpowers/specs/2026-08-06-catalog-reconciliation-design.md`).
- `ducklake_add_data_files` registers exactly one file per call — there is no batch/glob form.
- The `details` JSON column on `reconciliation_history` is write-only from the application's perspective in this plan — nothing reads it back; it exists for manual operator inspection (`docs/superpowers/specs/2026-08-06-catalog-reconciliation-design.md`'s "never silently dropped" requirement).
- Missing-file detection and orphan recovery are both no-ops (skipped, logged) when `joxette.catalog.object-storage-path` is blank (local-filesystem mode) — this mirrors the existing, identical limitation in `CassetteLifecycleService.resolveEntityDataSource()`, not a new one introduced here.
- Every new SQL string uses `?` placeholders via `PreparedStatement` wherever a value is user- or config-supplied; table names are only ever inlined after validation (`SchemaManager.normalize`/`SchemaManager.validateEntityType`), matching existing convention.

---

### Task 1: `reconciliation_history` schema + `joxette.reconciliation.*` config

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/db/SchemaManager.java:373` (immediately after the existing `retention_history` `CREATE TABLE` block, before the `transform_presets` comment)
- Modify: `joxette-service/src/test/java/com/joxette/support/DuckDBTestSupport.java:229` (immediately after the existing `retention_history`-equivalent... — there is none; insert after the `known_entities` table block, before `seq_compaction_history`, to keep fast-test schema parity with production)
- Modify: `joxette-service/src/main/java/com/joxette/config/JoxetteProperties.java:16` (field), `:402` (after the `Retention` class, before the `Kafka` comment), `:967` (getter/setter)
- Test: `joxette-service/src/test/java/com/joxette/db/ReconciliationSchemaAndConfigTest.java`

**Interfaces:**
- Produces: `reconciliation_history` table (columns: `id, started_at, completed_at, status, triggered_by, targets, tables_scanned, orphaned_files, orphaned_bytes, missing_files, missing_bytes, recovered_files, recovery_requested, details, error_message`), `seq_reconciliation_history` sequence, `JoxetteProperties.Reconciliation` (fields `enabled` (default `true`), `schedule` (default `"0 0 4 * * *"`), getters `isEnabled()`/`getSchedule()`), `JoxetteProperties.getReconciliation()`.

- [ ] **Step 1: Write the failing test**

```java
package com.joxette.db;

import com.joxette.config.JoxetteProperties;
import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

class ReconciliationSchemaAndConfigTest {

    private Connection duckDB;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
    }

    @Test
    void reconciliationHistoryTable_existsWithExpectedColumns() throws Exception {
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT column_name FROM duckdb_columns() " +
                     "WHERE table_name = 'reconciliation_history' ORDER BY column_name")) {
            java.util.List<String> columns = new java.util.ArrayList<>();
            while (rs.next()) columns.add(rs.getString(1));
            assertThat(columns).containsExactlyInAnyOrder(
                    "id", "started_at", "completed_at", "status", "triggered_by",
                    "targets", "tables_scanned", "orphaned_files", "orphaned_bytes",
                    "missing_files", "missing_bytes", "recovered_files",
                    "recovery_requested", "details", "error_message");
        }
    }

    @Test
    void reconciliationHistoryTable_acceptsAnInsertWithGeneratedId() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("""
                    INSERT INTO reconciliation_history
                        (started_at, status, triggered_by, targets, recovery_requested)
                    VALUES (now(), 'running', 'manual', ['orders.events'], false)
                    """);
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM reconciliation_history")) {
                rs.next();
                assertThat(rs.getLong(1)).isEqualTo(1L);
            }
        }
    }

    @Test
    void reconciliationConfig_hasExpectedDefaults() {
        JoxetteProperties props = new JoxetteProperties();
        assertThat(props.getReconciliation().isEnabled()).isTrue();
        assertThat(props.getReconciliation().getSchedule()).isEqualTo("0 0 4 * * *");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl joxette-service -am test -Dtest=ReconciliationSchemaAndConfigTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — `reconciliation_history` table does not exist (SQL binder error), and `getReconciliation()` does not exist (compile error). Comment out the config-defaults test temporarily if the compile error blocks the other two from running, then restore it once `JoxetteProperties.Reconciliation` exists in Step 3.

- [ ] **Step 3: Add the schema DDL**

In `joxette-service/src/main/java/com/joxette/db/SchemaManager.java`, immediately after the closing `""");` of the `retention_history` `CREATE TABLE` block (the block ending right before the `// Named transform pipeline presets` comment), insert:

```java
            stmt.execute("CREATE SEQUENCE IF NOT EXISTS seq_reconciliation_history START 1");

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS reconciliation_history (
                    id                 INTEGER     PRIMARY KEY
                                         DEFAULT nextval('seq_reconciliation_history'),
                    started_at         TIMESTAMPTZ NOT NULL,
                    completed_at       TIMESTAMPTZ,
                    status             VARCHAR     NOT NULL
                                         CHECK (status IN ('running', 'completed', 'failed')),
                    triggered_by       VARCHAR     NOT NULL,
                    targets            VARCHAR[],
                    tables_scanned     INTEGER     NOT NULL DEFAULT 0,
                    orphaned_files     INTEGER     NOT NULL DEFAULT 0,
                    orphaned_bytes     BIGINT      NOT NULL DEFAULT 0,
                    missing_files      INTEGER     NOT NULL DEFAULT 0,
                    missing_bytes      BIGINT      NOT NULL DEFAULT 0,
                    recovered_files    INTEGER     NOT NULL DEFAULT 0,
                    recovery_requested BOOLEAN     NOT NULL DEFAULT false,
                    details            JSON,
                    error_message      VARCHAR
                )
                """);
```

- [ ] **Step 4: Mirror the DDL in `DuckDBTestSupport`**

In `joxette-service/src/test/java/com/joxette/support/DuckDBTestSupport.java`, immediately after the closing `""");` of the `known_entities` table block (before `st.execute("CREATE SEQUENCE IF NOT EXISTS seq_compaction_history START 1");`), insert:

```java
            st.execute("CREATE SEQUENCE IF NOT EXISTS seq_reconciliation_history START 1");
            st.execute("""
                    CREATE TABLE IF NOT EXISTS reconciliation_history (
                        id                 INTEGER     PRIMARY KEY
                                             DEFAULT nextval('seq_reconciliation_history'),
                        started_at         TIMESTAMPTZ NOT NULL,
                        completed_at       TIMESTAMPTZ,
                        status             VARCHAR     NOT NULL
                                             CHECK (status IN ('running', 'completed', 'failed')),
                        triggered_by       VARCHAR     NOT NULL,
                        targets            VARCHAR[],
                        tables_scanned     INTEGER     NOT NULL DEFAULT 0,
                        orphaned_files     INTEGER     NOT NULL DEFAULT 0,
                        orphaned_bytes     BIGINT      NOT NULL DEFAULT 0,
                        missing_files      INTEGER     NOT NULL DEFAULT 0,
                        missing_bytes      BIGINT      NOT NULL DEFAULT 0,
                        recovered_files    INTEGER     NOT NULL DEFAULT 0,
                        recovery_requested BOOLEAN     NOT NULL DEFAULT false,
                        details            JSON,
                        error_message      VARCHAR
                    )
                    """);
```

- [ ] **Step 5: Add `JoxetteProperties.Reconciliation`**

In `joxette-service/src/main/java/com/joxette/config/JoxetteProperties.java`, add the field near the top alongside the other section fields (line 16-17 area):

```java
    private Reconciliation reconciliation = new Reconciliation();
```

Immediately after the closing `}` of the `Retention` class (before the `// Kafka` section comment), add:

```java
    // -----------------------------------------------------------------------
    // Reconciliation
    // -----------------------------------------------------------------------

    public static class Reconciliation {
        /**
         * Whether to run the scheduled catalog/object-storage reconciliation
         * audit on this node. The {@code POST /compaction/trigger-reconciliation}
         * endpoint remains available regardless of this flag.
         */
        private boolean enabled = true;

        /**
         * Cron expression for the scheduled reconciliation run.
         * Uses Spring 6-field format: {@code <sec> <min> <hour> <dom> <month> <dow>}.
         * Default: daily at 04:00:00 — after retention (01:00) and compaction (03:00),
         * so the scan reflects a settled catalog rather than racing either job.
         */
        private String schedule = "0 0 4 * * *";

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }

        public String getSchedule() { return schedule; }
        public void setSchedule(String schedule) { this.schedule = schedule; }
    }
```

Add the root getter/setter pair near `getRetention()`/`setRetention()`:

```java
    public Reconciliation getReconciliation() { return reconciliation; }
    public void setReconciliation(Reconciliation reconciliation) { this.reconciliation = reconciliation; }
```

- [ ] **Step 6: Run test to verify it passes**

Run: `mvn -pl joxette-service -am test -Dtest=ReconciliationSchemaAndConfigTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `Tests run: 3, Failures: 0, Errors: 0`

- [ ] **Step 7: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/db/SchemaManager.java \
        joxette-service/src/test/java/com/joxette/support/DuckDBTestSupport.java \
        joxette-service/src/main/java/com/joxette/config/JoxetteProperties.java \
        joxette-service/src/test/java/com/joxette/db/ReconciliationSchemaAndConfigTest.java
git commit -m "feat(reconciliation): add reconciliation_history schema and joxette.reconciliation config"
```

---

### Task 2: DTOs, `ConflictException` factory, and `ReconciliationService` run lifecycle

**Files:**
- Create: `joxette-service/src/main/java/com/joxette/reconciliation/ReconciliationRun.java`
- Create: `joxette-service/src/main/java/com/joxette/reconciliation/ReconciliationStatus.java`
- Create: `joxette-service/src/main/java/com/joxette/reconciliation/ReconciliationService.java`
- Modify: `joxette-service/src/main/java/com/joxette/api/error/ConflictException.java:37` (after `retentionAlreadyRunning()`)
- Test: `joxette-service/src/test/java/com/joxette/reconciliation/ReconciliationServiceLifecycleTest.java`

**Interfaces:**
- Consumes: `com.joxette.compaction.RunStatus` (`RUNNING`/`COMPLETED`/`FAILED`, `.getValue()`), `com.joxette.compaction.TriggerSource` (`SCHEDULED`/`MANUAL`, `.getValue()`), `com.joxette.compaction.CompactionLockManager` (`boolean tryAcquire(String target)`, `void release(String target)`), `com.joxette.metrics.JoxetteMetrics` (extended in Task 6 — pass `null`-safe no-op for now, see Step 3), `com.joxette.config.JoxetteProperties` (`getReconciliation().getSchedule()`), `com.fasterxml.jackson.databind.ObjectMapper`.
- Produces: `ReconciliationRun(long id, Instant startedAt, Instant completedAt, RunStatus status, TriggerSource triggeredBy, List<String> targets, int tablesScanned, int orphanedFiles, long orphanedBytes, int missingFiles, long missingBytes, int recoveredFiles, boolean recoveryRequested, String errorMessage)`; `ReconciliationStatus(ReconciliationRun lastRun, Instant nextScheduledRun, boolean running)`; `ReconciliationService` public methods `getStatus()`, `getHistory(int limit)`, `beginRun(TriggerSource, List<String> targets, boolean recoverOrphanedFiles)`, `runScheduled()`, `executeRun(long runId, List<String> targets, boolean recoverOrphanedFiles)`, `isRunning()`; `ConflictException.reconciliationAlreadyRunning()`. `executeRun` in this task calls two stub package-private methods, `scanOrphanedFiles()` and `scanMissingFiles(List<String> tables)`, and `recoverOrphans(List<OrphanedFile>)`, all returning empty results — Tasks 3-5 replace their bodies without changing these signatures.

- [ ] **Step 1: Write the failing test**

```java
package com.joxette.reconciliation;

import com.joxette.cluster.InstanceRegistry;
import com.joxette.compaction.CompactionLockManager;
import com.joxette.compaction.RunStatus;
import com.joxette.compaction.TriggerSource;
import com.joxette.config.JoxetteProperties;
import com.joxette.metrics.JoxetteMetrics;
import com.joxette.support.DuckDBTestSupport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReconciliationServiceLifecycleTest {

    private static final String TEST_INSTANCE_ID = "test-instance";

    private Connection duckDB;
    private InstanceRegistry instanceRegistry;
    private ReconciliationService service;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, "orders.events");
        DuckDBTestSupport.createEntityTable(duckDB, "order");

        instanceRegistry = DuckDBTestSupport.newInstanceRegistry(duckDB);
        DuckDBTestSupport.registerLiveInstance(duckDB, TEST_INSTANCE_ID);
        CompactionLockManager lockManager =
                new CompactionLockManager(duckDB, 120, TEST_INSTANCE_ID, instanceRegistry);

        JoxetteProperties props = new JoxetteProperties();
        service = new ReconciliationService(duckDB, props, lockManager,
                new JoxetteMetrics(new SimpleMeterRegistry()), new ObjectMapper());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
    }

    @Test
    void beginRun_insertsRunningRow() throws Exception {
        ReconciliationRun run = service.beginRun(TriggerSource.MANUAL, null, false);

        assertThat(run.id()).isPositive();
        assertThat(run.status()).isEqualTo(RunStatus.RUNNING);
        assertThat(run.triggeredBy()).isEqualTo(TriggerSource.MANUAL);
        assertThat(service.isRunning()).isTrue();
    }

    @Test
    void beginRun_whileAlreadyRunning_throwsConflict() throws Exception {
        service.beginRun(TriggerSource.MANUAL, null, false);

        assertThatThrownBy(() -> service.beginRun(TriggerSource.MANUAL, null, false))
                .isInstanceOf(com.joxette.api.error.ConflictException.class)
                .hasMessageContaining("Reconciliation");
    }

    @Test
    void executeRun_withStubbedScans_completesAndCountsTables() throws Exception {
        ReconciliationRun started = service.beginRun(TriggerSource.MANUAL, null, false);

        service.executeRun(started.id(), null, false);

        assertThat(service.isRunning()).isFalse();
        ReconciliationStatus status = service.getStatus();
        assertThat(status.running()).isFalse();
        assertThat(status.lastRun().id()).isEqualTo(started.id());
        assertThat(status.lastRun().status()).isEqualTo(RunStatus.COMPLETED);
        assertThat(status.lastRun().tablesScanned()).isEqualTo(2); // general_orders_events + entity_order
        assertThat(status.lastRun().orphanedFiles()).isZero();
        assertThat(status.lastRun().missingFiles()).isZero();
    }

    @Test
    void getHistory_returnsRunsNewestFirst() throws Exception {
        ReconciliationRun first = service.beginRun(TriggerSource.MANUAL, null, false);
        service.executeRun(first.id(), null, false);
        ReconciliationRun second = service.beginRun(TriggerSource.SCHEDULED, List.of("orders.events"), false);
        service.executeRun(second.id(), List.of("orders.events"), false);

        List<ReconciliationRun> history = service.getHistory(20);

        assertThat(history).hasSize(2);
        assertThat(history.get(0).id()).isEqualTo(second.id());
        assertThat(history.get(1).id()).isEqualTo(first.id());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl joxette-service -am test -Dtest=ReconciliationServiceLifecycleTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL to compile — `ReconciliationService`, `ReconciliationRun`, `ReconciliationStatus` do not exist yet.

- [ ] **Step 3: Add `ConflictException.reconciliationAlreadyRunning()`**

In `joxette-service/src/main/java/com/joxette/api/error/ConflictException.java`, immediately after `retentionAlreadyRunning()`:

```java
    public static ConflictException reconciliationAlreadyRunning() {
        return new ConflictException("Reconciliation run already in progress");
    }
```

- [ ] **Step 4: Create `ReconciliationRun`**

```java
package com.joxette.reconciliation;

import com.joxette.compaction.RunStatus;
import com.joxette.compaction.TriggerSource;

import java.time.Instant;
import java.util.List;

/** Snapshot of a single reconciliation audit run recorded in {@code reconciliation_history}. */
public record ReconciliationRun(
        long id,
        Instant startedAt,
        Instant completedAt,
        RunStatus status,
        TriggerSource triggeredBy,
        List<String> targets,
        int tablesScanned,
        int orphanedFiles,
        long orphanedBytes,
        int missingFiles,
        long missingBytes,
        int recoveredFiles,
        boolean recoveryRequested,
        String errorMessage
) {}
```

- [ ] **Step 5: Create `ReconciliationStatus`**

```java
package com.joxette.reconciliation;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

/**
 * Live status summary returned by {@code GET /compaction/reconciliation-status}.
 *
 * @param lastRun          most-recent entry from {@code reconciliation_history};
 *                         {@code null} if no run has ever occurred
 * @param nextScheduledRun next cron fire time, or {@code null} if it cannot be determined
 * @param running          {@code true} while a reconciliation run is actively executing
 */
@Schema(description = "Live catalog/object-storage reconciliation status summary")
public record ReconciliationStatus(
        ReconciliationRun lastRun,
        Instant nextScheduledRun,
        boolean running
) {}
```

- [ ] **Step 6: Create `ReconciliationService`**

```java
package com.joxette.reconciliation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.compaction.CompactionLockManager;
import com.joxette.compaction.RunStatus;
import com.joxette.compaction.TriggerSource;
import com.joxette.config.JoxetteProperties;
import com.joxette.metrics.JoxetteMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Audits whether every {@code lake.main.*} cassette table's DuckLake catalog
 * manifest matches what is actually present in object storage, recording each
 * run in {@code reconciliation_history}. See
 * {@code docs/superpowers/specs/2026-08-06-catalog-reconciliation-design.md}.
 *
 * <p>An {@link AtomicBoolean} guards against overlapping runs on this instance;
 * a {@link CompactionLockManager} lock (target {@value #LOCK_TARGET}) guards
 * against overlapping runs across instances sharing a catalog.
 */
@Service
@DependsOn("dbSchemaManager")
public class ReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationService.class);

    static final String LOCK_TARGET = "reconciliation";
    private static final int MAX_DETAILS_ENTRIES = 500;

    private final Connection duckDB;
    private final JoxetteProperties props;
    private final CompactionLockManager lockManager;
    private final ObjectMapper objectMapper;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger lastOrphanedCount = new AtomicInteger(0);
    private final AtomicInteger lastMissingCount = new AtomicInteger(0);

    public ReconciliationService(Connection duckDB, JoxetteProperties props,
                                  CompactionLockManager lockManager, JoxetteMetrics joxetteMetrics,
                                  ObjectMapper objectMapper) {
        this.duckDB      = duckDB;
        this.props       = props;
        this.lockManager = lockManager;
        this.objectMapper = objectMapper;
        joxetteMetrics.registerReconciliationOrphanedFilesGauge(lastOrphanedCount::get);
        joxetteMetrics.registerReconciliationMissingFilesGauge(lastMissingCount::get);
    }

    // =========================================================================
    // Public API
    // =========================================================================

    public boolean isRunning() {
        return running.get();
    }

    /** Convenience entry point used by {@link ReconciliationScheduler}. Never recovers orphans. */
    public void runScheduled() {
        if (!running.compareAndSet(false, true)) {
            log.warn("Skipping scheduled reconciliation: a run is still in progress");
            return;
        }
        boolean lockAcquired;
        try {
            lockAcquired = lockManager.tryAcquire(LOCK_TARGET);
        } catch (SQLException e) {
            running.set(false);
            log.error("Failed to acquire reconciliation lock", e);
            return;
        }
        if (!lockAcquired) {
            running.set(false);
            log.info("Skipping scheduled reconciliation: another instance holds the lock");
            return;
        }
        long runId;
        try {
            runId = insertRunRecord(TriggerSource.SCHEDULED, null, false);
        } catch (SQLException e) {
            lockManager.release(LOCK_TARGET);
            running.set(false);
            log.error("Failed to insert reconciliation run record", e);
            return;
        }
        executeRun(runId, null, false);
    }

    /**
     * Atomically marks a new run as started, acquires the cross-instance lock,
     * and inserts a {@code "running"} row in {@code reconciliation_history}.
     *
     * @throws com.joxette.api.error.ConflictException if a run is already in progress
     *         locally or another instance holds the distributed lock
     */
    public ReconciliationRun beginRun(TriggerSource triggeredBy, List<String> targets,
                                       boolean recoverOrphanedFiles) throws SQLException {
        if (!running.compareAndSet(false, true)) {
            throw com.joxette.api.error.ConflictException.reconciliationAlreadyRunning();
        }
        boolean lockAcquired;
        try {
            lockAcquired = lockManager.tryAcquire(LOCK_TARGET);
        } catch (SQLException e) {
            running.set(false);
            throw e;
        }
        if (!lockAcquired) {
            running.set(false);
            throw com.joxette.api.error.ConflictException.reconciliationAlreadyRunning();
        }
        long id;
        try {
            id = insertRunRecord(triggeredBy, targets, recoverOrphanedFiles);
        } catch (SQLException e) {
            lockManager.release(LOCK_TARGET);
            running.set(false);
            throw e;
        }
        return getRunById(id);
    }

    /**
     * Performs the reconciliation scan (and optional recovery) for a run started
     * with {@link #beginRun} or {@link #runScheduled}. Always releases the
     * distributed lock and resets the running flag on exit, even on error.
     */
    public void executeRun(long runId, List<String> targets, boolean recoverOrphanedFiles) {
        int tablesScanned = 0;
        int orphanedFiles = 0;
        long orphanedBytes = 0;
        int missingFiles = 0;
        long missingBytes = 0;
        int recoveredFiles = 0;
        try {
            List<String> tables = resolveTargetTables(targets);
            tablesScanned = tables.size();

            List<OrphanedFile> orphans = scanOrphanedFiles();
            orphanedFiles = orphans.size();
            orphanedBytes = orphans.stream().mapToLong(OrphanedFile::sizeBytes).sum();

            List<String> missing = scanMissingFiles(tables);
            missingFiles = missing.size();
            missingBytes = 0; // populated in Task 4 alongside the real scan

            if (recoverOrphanedFiles && !orphans.isEmpty()) {
                recoveredFiles = recoverOrphans(orphans);
            }

            String detailsJson = buildDetailsJson(orphans, missing);
            updateRunRecord(runId, RunStatus.COMPLETED, tablesScanned, orphanedFiles, orphanedBytes,
                    missingFiles, missingBytes, recoveredFiles, detailsJson, null);
            lastOrphanedCount.set(orphanedFiles);
            lastMissingCount.set(missingFiles);
            log.info("Reconciliation run {} completed: {} tables scanned, {} orphaned files, " +
                            "{} missing files, {} recovered",
                    runId, tablesScanned, orphanedFiles, missingFiles, recoveredFiles);
        } catch (Exception e) {
            log.error("Reconciliation run {} failed", runId, e);
            try {
                updateRunRecord(runId, RunStatus.FAILED, tablesScanned, orphanedFiles, orphanedBytes,
                        missingFiles, missingBytes, recoveredFiles, null, e.getMessage());
            } catch (SQLException se) {
                log.error("Failed to update reconciliation_history for run {}", runId, se);
            }
        } finally {
            lockManager.release(LOCK_TARGET);
            running.set(false);
        }
    }

    public List<ReconciliationRun> getHistory(int limit) throws SQLException {
        List<ReconciliationRun> result = new ArrayList<>();
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    SELECT id, started_at, completed_at, status, triggered_by, targets,
                           tables_scanned, orphaned_files, orphaned_bytes,
                           missing_files, missing_bytes, recovered_files,
                           recovery_requested, error_message
                    FROM reconciliation_history
                    ORDER BY started_at DESC
                    LIMIT ?
                    """)) {
                ps.setInt(1, limit);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) result.add(mapRun(rs));
                }
            }
        }
        return result;
    }

    public ReconciliationRun getRunById(long id) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    SELECT id, started_at, completed_at, status, triggered_by, targets,
                           tables_scanned, orphaned_files, orphaned_bytes,
                           missing_files, missing_bytes, recovered_files,
                           recovery_requested, error_message
                    FROM reconciliation_history WHERE id = ?
                    """)) {
                ps.setLong(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) return mapRun(rs);
                    throw new SQLException("reconciliation_history row not found for id " + id);
                }
            }
        }
    }

    public ReconciliationStatus getStatus() throws SQLException {
        ReconciliationRun lastRun = queryLastRun();
        Instant next = computeNextScheduledRun();
        return new ReconciliationStatus(lastRun, next, running.get());
    }

    // =========================================================================
    // Scan/recovery seams — real bodies added in Tasks 3-5
    // =========================================================================

    /** Represents one file found in object storage but not tracked by the catalog. */
    record OrphanedFile(String path, String tableName, long sizeBytes) {}

    /** Stub — replaced with the real {@code ducklake_delete_orphaned_files} scan in Task 3. */
    List<OrphanedFile> scanOrphanedFiles() throws SQLException {
        return List.of();
    }

    /** Stub — replaced with the real {@code ducklake_list_files} vs {@code glob} diff in Task 4. */
    List<String> scanMissingFiles(List<String> tables) throws SQLException {
        return List.of();
    }

    /** Stub — replaced with the real {@code ducklake_add_data_files} loop in Task 5. */
    int recoverOrphans(List<OrphanedFile> orphans) throws SQLException {
        return 0;
    }

    // =========================================================================
    // Table resolution
    // =========================================================================

    /**
     * Resolves {@code targets} (compaction's {@code "topic"} / {@code "entity:type"}
     * scoping format) to unqualified {@code lake.main} table names. {@code null} or
     * empty targets means every {@code general_*}/{@code entity_*} table.
     */
    List<String> resolveTargetTables(List<String> targets) throws SQLException {
        if (targets == null || targets.isEmpty()) {
            return listAllCassetteTables();
        }
        List<String> tables = new ArrayList<>();
        for (String target : targets) {
            if (target.startsWith("entity:")) {
                tables.add("entity_" + target.substring("entity:".length()));
            } else {
                tables.add("general_" + com.joxette.db.SchemaManager.normalize(target));
            }
        }
        return tables;
    }

    private List<String> listAllCassetteTables() throws SQLException {
        List<String> tables = new ArrayList<>();
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery("""
                         SELECT table_name FROM duckdb_tables()
                         WHERE database_name = 'lake' AND schema_name = 'main'
                           AND (table_name LIKE 'general\\_%' ESCAPE '\\'
                                OR table_name LIKE 'entity\\_%' ESCAPE '\\')
                         ORDER BY table_name
                         """)) {
                while (rs.next()) tables.add(rs.getString(1));
            }
        }
        return tables;
    }

    // =========================================================================
    // Details JSON
    // =========================================================================

    private record ReconciliationDetails(
            List<String> orphanedFiles, boolean orphanedTruncated,
            List<String> missingFiles, boolean missingTruncated) {}

    private String buildDetailsJson(List<OrphanedFile> orphans, List<String> missing) {
        List<String> orphanPaths = orphans.stream().map(OrphanedFile::path)
                .limit(MAX_DETAILS_ENTRIES).toList();
        List<String> missingCapped = missing.stream().limit(MAX_DETAILS_ENTRIES).toList();
        ReconciliationDetails details = new ReconciliationDetails(
                orphanPaths, orphans.size() > MAX_DETAILS_ENTRIES,
                missingCapped, missing.size() > MAX_DETAILS_ENTRIES);
        try {
            return objectMapper.writeValueAsString(details);
        } catch (Exception e) {
            log.warn("Failed to serialise reconciliation details JSON: {}", e.getMessage());
            return null;
        }
    }

    // =========================================================================
    // reconciliation_history CRUD
    // =========================================================================

    private long insertRunRecord(TriggerSource triggeredBy, List<String> targets,
                                  boolean recoverOrphanedFiles) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    INSERT INTO reconciliation_history
                        (started_at, status, triggered_by, targets, recovery_requested)
                    VALUES (?, 'running', ?, ?, ?)
                    RETURNING id
                    """)) {
                ps.setTimestamp(1, Timestamp.from(Instant.now()));
                ps.setString(2, triggeredBy.getValue());
                // Matches CompactionService.insertRunRecord's exact null-vs-array handling —
                // a true SQL NULL for "no targets" (all tables), never an empty array, so the
                // two remain distinguishable on read-back.
                if (targets == null) {
                    ps.setObject(3, null);
                } else {
                    ps.setArray(3, duckDB.createArrayOf("VARCHAR", targets.toArray(new String[0])));
                }
                ps.setBoolean(4, recoverOrphanedFiles);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) return rs.getLong(1);
                    throw new SQLException("INSERT into reconciliation_history returned no generated id");
                }
            }
        }
    }

    private void updateRunRecord(long runId, RunStatus status, int tablesScanned,
                                  int orphanedFiles, long orphanedBytes,
                                  int missingFiles, long missingBytes,
                                  int recoveredFiles, String detailsJson,
                                  String errorMessage) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    UPDATE reconciliation_history
                    SET completed_at    = ?,
                        status          = ?,
                        tables_scanned  = ?,
                        orphaned_files  = ?,
                        orphaned_bytes  = ?,
                        missing_files   = ?,
                        missing_bytes   = ?,
                        recovered_files = ?,
                        details         = ?,
                        error_message   = ?
                    WHERE id = ?
                    """)) {
                ps.setTimestamp(1, Timestamp.from(Instant.now()));
                ps.setString(2, status.getValue());
                ps.setInt(3, tablesScanned);
                ps.setInt(4, orphanedFiles);
                ps.setLong(5, orphanedBytes);
                ps.setInt(6, missingFiles);
                ps.setLong(7, missingBytes);
                ps.setInt(8, recoveredFiles);
                ps.setString(9, detailsJson);
                ps.setString(10, errorMessage);
                ps.setLong(11, runId);
                ps.executeUpdate();
            }
        }
    }

    private ReconciliationRun queryLastRun() throws SQLException {
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery("""
                    SELECT id, started_at, completed_at, status, triggered_by, targets,
                           tables_scanned, orphaned_files, orphaned_bytes,
                           missing_files, missing_bytes, recovered_files,
                           recovery_requested, error_message
                    FROM reconciliation_history
                    ORDER BY started_at DESC LIMIT 1
                    """)) {
                return rs.next() ? mapRun(rs) : null;
            }
        }
    }

    private Instant computeNextScheduledRun() {
        try {
            CronExpression expr = CronExpression.parse(props.getReconciliation().getSchedule());
            LocalDateTime next = expr.next(LocalDateTime.now());
            if (next == null) return null;
            return next.atZone(ZoneId.systemDefault()).toInstant();
        } catch (Exception e) {
            log.warn("Cannot parse reconciliation cron '{}' to compute next run: {}",
                    props.getReconciliation().getSchedule(), e.getMessage());
            return null;
        }
    }

    private static ReconciliationRun mapRun(ResultSet rs) throws SQLException {
        Timestamp completedTs = rs.getTimestamp("completed_at");

        // Matches CompactionService.mapRun's exact array read-back: DuckDB JDBC's
        // java.sql.Array.getArray() returns Object[], not String[] — casting directly
        // to String[] throws ClassCastException. null stays null (no targets = all tables),
        // distinct from an empty list.
        java.sql.Array targetsArr = rs.getArray("targets");
        List<String> targets = null;
        if (targetsArr != null) {
            Object[] vals = (Object[]) targetsArr.getArray();
            targets = java.util.Arrays.stream(vals).map(Object::toString).toList();
        }

        return new ReconciliationRun(
                rs.getLong("id"),
                rs.getTimestamp("started_at").toInstant(),
                completedTs != null ? completedTs.toInstant() : null,
                RunStatus.fromValue(rs.getString("status")),
                TriggerSource.fromValue(rs.getString("triggered_by")),
                targets,
                rs.getInt("tables_scanned"),
                rs.getInt("orphaned_files"),
                rs.getLong("orphaned_bytes"),
                rs.getInt("missing_files"),
                rs.getLong("missing_bytes"),
                rs.getInt("recovered_files"),
                rs.getBoolean("recovery_requested"),
                rs.getString("error_message")
        );
    }
}
```

Note: `joxetteMetrics.registerReconciliationOrphanedFilesGauge(...)`/`registerReconciliationMissingFilesGauge(...)` do not exist yet — Task 6 adds them. Until then this file will not compile; Task 6 must land before this task's test suite is green in CI, but for TDD purposes within this task add temporary no-op stubs directly on `JoxetteMetrics` now (Task 6 replaces the stub bodies with the real gauge registration, not the method signatures):

```java
    // Temporary stub — Task 6 replaces the body with real Gauge registration.
    public void registerReconciliationOrphanedFilesGauge(java.util.function.Supplier<Integer> supplier) {}
    public void registerReconciliationMissingFilesGauge(java.util.function.Supplier<Integer> supplier) {}
```

Add these two methods to `joxette-service/src/main/java/com/joxette/metrics/JoxetteMetrics.java` now, in the `// Catalog sizes` section, immediately after `registerInlinedDataGauge`.

- [ ] **Step 7: Run test to verify it passes**

Run: `mvn -pl joxette-service -am test -Dtest=ReconciliationServiceLifecycleTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `Tests run: 4, Failures: 0, Errors: 0`

- [ ] **Step 8: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/reconciliation/ \
        joxette-service/src/main/java/com/joxette/api/error/ConflictException.java \
        joxette-service/src/main/java/com/joxette/metrics/JoxetteMetrics.java \
        joxette-service/src/test/java/com/joxette/reconciliation/ReconciliationServiceLifecycleTest.java
git commit -m "feat(reconciliation): add ReconciliationService run lifecycle with stubbed scans"
```

---

### Task 3: Real orphan-file detection (`ducklake_delete_orphaned_files`)

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/reconciliation/ReconciliationService.java` (`scanOrphanedFiles()` body only)
- Test: `joxette-service/src/test/java/com/joxette/it/ReconciliationOrphanedFilesIT.java`

**Interfaces:**
- Consumes: `JoxetteProperties.Catalog.getObjectStoragePath()` (already injected via `props` field from Task 2).
- Produces: no signature change — `scanOrphanedFiles()` still returns `List<OrphanedFile>`, now populated for real.

- [ ] **Step 1: Write the failing test**

```java
package com.joxette.it;

import com.joxette.reconciliation.ReconciliationService;
import com.joxette.compaction.RunStatus;
import com.joxette.compaction.TriggerSource;
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
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class ReconciliationOrphanedFilesIT {

    private static final String BUCKET = "joxette-recon-test";

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
    }

    @Autowired private Connection duckDB;
    @Autowired private ReconciliationService reconciliationService;

    @Test
    void reconciliation_detectsAndSizesAStrayOrphanedFile() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS lake.main.general_orders_events (" +
                    "recorded_at TIMESTAMPTZ, kafka_offset BIGINT, kafka_partition INTEGER, " +
                    "kafka_timestamp TIMESTAMPTZ, kafka_key VARCHAR, kafka_value BLOB, " +
                    "metadata VARCHAR, headers STRUCT(key VARCHAR, value VARCHAR)[], message_type VARCHAR)");
            // A stray file dropped directly into the table's default DuckLake path,
            // never registered via any catalog write — simulates external drift.
            st.execute("COPY (SELECT 1 AS id) TO " +
                    "'s3://" + BUCKET + "/data/main/general_orders_events/stray.parquet' (FORMAT PARQUET)");
        }

        var run = reconciliationService.beginRun(TriggerSource.MANUAL, null, false);
        reconciliationService.executeRun(run.id(), null, false);

        var status = reconciliationService.getStatus();
        assertThat(status.lastRun().status()).isEqualTo(RunStatus.COMPLETED);
        assertThat(status.lastRun().orphanedFiles()).isEqualTo(1);
        assertThat(status.lastRun().orphanedBytes()).isGreaterThan(0);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl joxette-service -am verify -Dit.test=ReconciliationOrphanedFilesIT -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false`
Expected: FAIL — `orphanedFiles` is `0` (stub still in place).

- [ ] **Step 3: Implement `scanOrphanedFiles()`**

Replace the stub body in `joxette-service/src/main/java/com/joxette/reconciliation/ReconciliationService.java`:

```java
    List<OrphanedFile> scanOrphanedFiles() throws SQLException {
        List<String> paths = new ArrayList<>();
        synchronized (duckDB) {
            // cleanup_all => true is REQUIRED alongside dry_run => true — without it,
            // this call silently returns zero rows even when orphaned files exist.
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery(
                         "CALL ducklake_delete_orphaned_files('lake', cleanup_all => true, dry_run => true)")) {
                while (rs.next()) paths.add(rs.getString("path"));
            }
        }
        List<OrphanedFile> orphans = new ArrayList<>();
        for (String path : paths) {
            long size = orphanedFileSizeBytes(path);
            orphans.add(new OrphanedFile(path, extractTableName(path), size));
        }
        return orphans;
    }

    /**
     * Reads the file size via {@code parquet_file_metadata} — DuckDB has no cheap
     * raw file-stat primitive, so this is the lightest verified way to size an
     * orphaned file without duplicating S3 credential handling in Java.
     * Non-fatal: a single unreadable file (e.g. corrupt, mid-write) logs and
     * contributes 0 bytes rather than failing the whole scan.
     */
    private long orphanedFileSizeBytes(String path) {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "SELECT file_size_bytes FROM parquet_file_metadata(?)")) {
                ps.setString(1, path);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getLong(1) : 0L;
                }
            } catch (SQLException e) {
                log.warn("Could not read size of orphaned file '{}': {}", path, e.getMessage());
                return 0L;
            }
        }
    }

    /**
     * Extracts the owning table name from a DuckLake data-file path following the
     * {@code {data_path}/main/{tableName}/...} convention (see
     * https://ducklake.select/docs/stable/duckdb/usage/paths), the same convention
     * already relied on by {@code CassetteLifecycleService.resolveEntityDataSource}.
     */
    private static String extractTableName(String path) {
        int idx = path.indexOf("/main/");
        if (idx < 0) return "unknown";
        String rest = path.substring(idx + "/main/".length());
        int slash = rest.indexOf('/');
        return slash < 0 ? rest : rest.substring(0, slash);
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl joxette-service -am verify -Dit.test=ReconciliationOrphanedFilesIT -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false`
Expected: `Tests run: 1, Failures: 0, Errors: 0`

- [ ] **Step 5: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/reconciliation/ReconciliationService.java \
        joxette-service/src/test/java/com/joxette/it/ReconciliationOrphanedFilesIT.java
git commit -m "feat(reconciliation): implement real orphaned-file detection via ducklake_delete_orphaned_files"
```

---

### Task 4: Real missing-file detection (`ducklake_list_files` vs `glob`)

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/reconciliation/ReconciliationService.java` (`scanMissingFiles`, and wire `missingBytes` into `executeRun`)
- Test: `joxette-service/src/test/java/com/joxette/it/ReconciliationMissingFilesIT.java`

**Interfaces:**
- Consumes: `props.getCatalog().getObjectStoragePath()`.
- Produces: `scanMissingFiles(List<String> tables)` now returns real results; adds a package-private `long lastMissingBytesScanned()`-style accumulation — implemented by having `scanMissingFiles` return `List<MissingFile>` (a new nested record) instead of `List<String>`, since `executeRun` needs both the path list (for `details`) and the byte total. **This changes `scanMissingFiles`'s return type from Task 2/3's `List<String>` to `List<MissingFile>`** — update the two call sites in `executeRun` accordingly (shown in Step 3 below).

- [ ] **Step 1: Write the failing test**

```java
package com.joxette.it;

import com.joxette.reconciliation.ReconciliationService;
import com.joxette.compaction.RunStatus;
import com.joxette.compaction.TriggerSource;
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
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class ReconciliationMissingFilesIT {

    private static final String BUCKET = "joxette-recon-missing-test";

    @Container
    static final MinIOContainer minio =
            new MinIOContainer(DockerImageName.parse("minio/minio:RELEASE.2024-01-16T16-07-38Z"));

    private static String s3Url;
    private static String userName;
    private static String password;

    @DynamicPropertySource
    static void minioProperties(DynamicPropertyRegistry registry) {
        s3Url = minio.getS3URL();
        userName = minio.getUserName();
        password = minio.getPassword();
        try (S3Client s3 = s3Client()) {
            s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        }
        registry.add("joxette.catalog.object-storage-path", () -> "s3://" + BUCKET + "/data/");
        registry.add("joxette.s3.endpoint",   () -> s3Url);
        registry.add("joxette.s3.access-key", () -> userName);
        registry.add("joxette.s3.secret-key", () -> password);
    }

    private static S3Client s3Client() {
        return S3Client.builder()
                .endpointOverride(URI.create(s3Url))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(userName, password)))
                .region(Region.US_EAST_1)
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();
    }

    @Autowired private Connection duckDB;
    @Autowired private ReconciliationService reconciliationService;

    @Test
    void reconciliation_detectsAFileDeletedOutOfBand() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS lake.main.general_audit_log (" +
                    "recorded_at TIMESTAMPTZ, kafka_offset BIGINT, kafka_partition INTEGER, " +
                    "kafka_timestamp TIMESTAMPTZ, kafka_key VARCHAR, kafka_value BLOB, " +
                    "metadata VARCHAR, headers STRUCT(key VARCHAR, value VARCHAR)[], message_type VARCHAR)");
            st.execute("INSERT INTO lake.main.general_audit_log (recorded_at, kafka_offset, " +
                    "kafka_partition, kafka_timestamp, kafka_key, kafka_value, headers) " +
                    "VALUES (now(), 0, 0, now(), 'k', 'v', [])");
            st.execute("CALL ducklake_flush_inlined_data('lake')");
            st.execute("CHECKPOINT");
        }

        String trackedKey;
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT data_file FROM ducklake_list_files('lake', 'general_audit_log')")) {
            rs.next();
            String dataFile = rs.getString(1); // s3://bucket/data/main/general_audit_log/ducklake-...parquet
            trackedKey = dataFile.substring(("s3://" + BUCKET + "/").length());
        }

        // Delete the tracked file directly from the bucket — out-of-band, not via DuckLake.
        try (S3Client s3 = s3Client()) {
            s3.deleteObject(DeleteObjectRequest.builder().bucket(BUCKET).key(trackedKey).build());
        }

        var run = reconciliationService.beginRun(TriggerSource.MANUAL, null, false);
        reconciliationService.executeRun(run.id(), null, false);

        var status = reconciliationService.getStatus();
        assertThat(status.lastRun().status()).isEqualTo(RunStatus.COMPLETED);
        assertThat(status.lastRun().missingFiles()).isEqualTo(1);
        assertThat(status.lastRun().missingBytes()).isGreaterThan(0);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl joxette-service -am verify -Dit.test=ReconciliationMissingFilesIT -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false`
Expected: FAIL — `missingFiles` is `0` (stub still in place).

- [ ] **Step 3: Implement `scanMissingFiles` and wire `missingBytes`**

In `ReconciliationService.java`, replace the `OrphanedFile` record area and stub with:

```java
    /** Represents one file the catalog's current snapshot references that no longer exists in storage. */
    record MissingFile(String path, long lastKnownSizeBytes) {}
```

Replace the `scanMissingFiles` stub:

```java
    List<MissingFile> scanMissingFiles(List<String> tables) throws SQLException {
        String objectStoragePath = props.getCatalog().getObjectStoragePath();
        if (objectStoragePath == null || objectStoragePath.isBlank()) {
            log.debug("scanMissingFiles: no object-storage-path configured; skipping " +
                    "(local-filesystem mode is not covered by this scan)");
            return List.of();
        }
        String base = objectStoragePath.endsWith("/") ? objectStoragePath : objectStoragePath + "/";

        List<MissingFile> missing = new ArrayList<>();
        for (String table : tables) {
            java.util.Map<String, Long> tracked = new java.util.LinkedHashMap<>();
            synchronized (duckDB) {
                try (PreparedStatement ps = duckDB.prepareStatement(
                        "SELECT data_file, data_file_size_bytes FROM ducklake_list_files('lake', ?)")) {
                    ps.setString(1, table);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) tracked.put(rs.getString(1), rs.getLong(2));
                    }
                } catch (SQLException e) {
                    log.warn("scanMissingFiles: ducklake_list_files failed for table '{}' ({}); skipping",
                            table, e.getMessage());
                    continue;
                }
            }
            if (tracked.isEmpty()) continue;

            java.util.Set<String> present = new java.util.HashSet<>();
            String glob = base + "main/" + table + "/**/*.parquet";
            synchronized (duckDB) {
                try (PreparedStatement ps = duckDB.prepareStatement("SELECT file FROM glob(?)")) {
                    ps.setString(1, glob);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) present.add(rs.getString(1));
                    }
                } catch (SQLException e) {
                    log.warn("scanMissingFiles: glob('{}') failed ({}); skipping table '{}'",
                            glob, e.getMessage(), table);
                    continue;
                }
            }

            for (var entry : tracked.entrySet()) {
                if (!present.contains(entry.getKey())) {
                    missing.add(new MissingFile(entry.getKey(), entry.getValue()));
                }
            }
        }
        return missing;
    }
```

Update `executeRun` to consume the new return type and populate `missingBytes` (replace the two `missing`-related lines):

```java
            List<MissingFile> missing = scanMissingFiles(tables);
            missingFiles = missing.size();
            missingBytes = missing.stream().mapToLong(MissingFile::lastKnownSizeBytes).sum();
```

Update `buildDetailsJson`'s signature and body to accept `List<MissingFile>` instead of `List<String>`:

```java
    private String buildDetailsJson(List<OrphanedFile> orphans, List<MissingFile> missing) {
        List<String> orphanPaths = orphans.stream().map(OrphanedFile::path)
                .limit(MAX_DETAILS_ENTRIES).toList();
        List<String> missingPaths = missing.stream().map(MissingFile::path)
                .limit(MAX_DETAILS_ENTRIES).toList();
        ReconciliationDetails details = new ReconciliationDetails(
                orphanPaths, orphans.size() > MAX_DETAILS_ENTRIES,
                missingPaths, missing.size() > MAX_DETAILS_ENTRIES);
        try {
            return objectMapper.writeValueAsString(details);
        } catch (Exception e) {
            log.warn("Failed to serialise reconciliation details JSON: {}", e.getMessage());
            return null;
        }
    }
```

And update the call site in `executeRun` from `buildDetailsJson(orphans, missing)` — unchanged call, only the parameter type changed, so no call-site edit is needed beyond what Step 3 already shows.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl joxette-service -am verify -Dit.test=ReconciliationMissingFilesIT -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false`
Expected: `Tests run: 1, Failures: 0, Errors: 0`

Also re-run Task 2's fast test to confirm the `List<String>` → `List<MissingFile>` change didn't break the stub-based lifecycle test (it calls `scanMissingFiles` transitively through `executeRun`, not directly, so it should be unaffected — verify):

Run: `mvn -pl joxette-service -am test -Dtest=ReconciliationServiceLifecycleTest,ReconciliationOrphanedFilesIT -Dsurefire.failIfNoSpecifiedTests=false`
Expected: all still pass.

- [ ] **Step 5: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/reconciliation/ReconciliationService.java \
        joxette-service/src/test/java/com/joxette/it/ReconciliationMissingFilesIT.java
git commit -m "feat(reconciliation): implement real missing-file detection via ducklake_list_files vs glob"
```

---

### Task 5: Opt-in orphan recovery (`ducklake_add_data_files`)

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/reconciliation/ReconciliationService.java` (`recoverOrphans` body only)
- Test: `joxette-service/src/test/java/com/joxette/it/ReconciliationRecoveryIT.java`

**Interfaces:**
- Consumes: `OrphanedFile(path, tableName, sizeBytes)` from Task 3.
- Produces: no signature change — `recoverOrphans(List<OrphanedFile>)` still returns `int`, now performs real registration.

- [ ] **Step 1: Write the failing test**

```java
package com.joxette.it;

import com.joxette.reconciliation.ReconciliationService;
import com.joxette.compaction.RunStatus;
import com.joxette.compaction.TriggerSource;
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
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class ReconciliationRecoveryIT {

    private static final String BUCKET = "joxette-recon-recovery-test";

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
    }

    @Autowired private Connection duckDB;
    @Autowired private ReconciliationService reconciliationService;

    @Test
    void reconciliation_recoverOrphanedFiles_registersFileAndMakesRowsQueryable() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS lake.main.general_orders_events (" +
                    "recorded_at TIMESTAMPTZ, kafka_offset BIGINT, kafka_partition INTEGER, " +
                    "kafka_timestamp TIMESTAMPTZ, kafka_key VARCHAR, kafka_value BLOB, " +
                    "metadata VARCHAR, headers STRUCT(key VARCHAR, value VARCHAR)[], message_type VARCHAR)");
            st.execute("COPY (SELECT now()::TIMESTAMPTZ AS recorded_at, 0::BIGINT AS kafka_offset, " +
                    "0::INTEGER AS kafka_partition, now()::TIMESTAMPTZ AS kafka_timestamp, " +
                    "'k'::VARCHAR AS kafka_key, 'v'::BLOB AS kafka_value, NULL::VARCHAR AS metadata, " +
                    "[]::STRUCT(key VARCHAR, value VARCHAR)[] AS headers, NULL::VARCHAR AS message_type) " +
                    "TO 's3://" + BUCKET + "/data/main/general_orders_events/stray.parquet' (FORMAT PARQUET)");
        }

        var run = reconciliationService.beginRun(TriggerSource.MANUAL, null, true);
        reconciliationService.executeRun(run.id(), null, true);

        var status = reconciliationService.getStatus();
        assertThat(status.lastRun().status()).isEqualTo(RunStatus.COMPLETED);
        assertThat(status.lastRun().recoveredFiles()).isEqualTo(1);

        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM lake.main.general_orders_events")) {
            rs.next();
            assertThat(rs.getLong(1)).isEqualTo(1L);
        }

        // A second run should find nothing left to recover.
        var second = reconciliationService.beginRun(TriggerSource.MANUAL, null, true);
        reconciliationService.executeRun(second.id(), null, true);
        assertThat(reconciliationService.getStatus().lastRun().orphanedFiles()).isZero();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl joxette-service -am verify -Dit.test=ReconciliationRecoveryIT -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false`
Expected: FAIL — `recoveredFiles` is `0` (stub still in place).

- [ ] **Step 3: Implement `recoverOrphans`**

Replace the stub in `ReconciliationService.java`:

```java
    int recoverOrphans(List<OrphanedFile> orphans) throws SQLException {
        int recovered = 0;
        for (OrphanedFile orphan : orphans) {
            if ("unknown".equals(orphan.tableName())) {
                log.warn("recoverOrphans: could not determine owning table for '{}'; skipping",
                        orphan.path());
                continue;
            }
            synchronized (duckDB) {
                try (Statement st = duckDB.createStatement()) {
                    st.execute(String.format(
                            "CALL ducklake_add_data_files('lake', '%s', '%s', " +
                                    "schema => 'main', ignore_extra_columns => true)",
                            orphan.tableName().replace("'", "''"),
                            orphan.path().replace("'", "''")));
                    recovered++;
                } catch (SQLException e) {
                    log.warn("recoverOrphans: failed to register '{}' into table '{}': {}",
                            orphan.path(), orphan.tableName(), e.getMessage());
                }
            }
        }
        return recovered;
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl joxette-service -am verify -Dit.test=ReconciliationRecoveryIT -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false`
Expected: `Tests run: 1, Failures: 0, Errors: 0`

- [ ] **Step 5: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/reconciliation/ReconciliationService.java \
        joxette-service/src/test/java/com/joxette/it/ReconciliationRecoveryIT.java
git commit -m "feat(reconciliation): implement opt-in orphan recovery via ducklake_add_data_files"
```

---

### Task 6: `ReconciliationScheduler` and real metric gauges

**Files:**
- Create: `joxette-service/src/main/java/com/joxette/reconciliation/ReconciliationScheduler.java`
- Modify: `joxette-service/src/main/java/com/joxette/metrics/JoxetteMetrics.java` (replace the Task 2 no-op stubs with real gauges)
- Test: `joxette-service/src/test/java/com/joxette/metrics/JoxetteMetricsReconciliationGaugeTest.java`

**Interfaces:**
- Consumes: nothing new.
- Produces: `JoxetteMetrics.registerReconciliationOrphanedFilesGauge(Supplier<Integer>)`, `registerReconciliationMissingFilesGauge(Supplier<Integer>)` — same names, real bodies. `ReconciliationScheduler` — no public API beyond the Spring `@Scheduled` method.

- [ ] **Step 1: Write the failing test**

```java
package com.joxette.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JoxetteMetricsReconciliationGaugeTest {

    @Test
    void reconciliationOrphanedFilesGauge_reflectsSupplierValue() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        JoxetteMetrics metrics = new JoxetteMetrics(registry);
        java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger(0);

        metrics.registerReconciliationOrphanedFilesGauge(count::get);
        count.set(7);

        assertThat(registry.get("joxette.reconciliation.orphaned.files").gauge().value()).isEqualTo(7.0);
    }

    @Test
    void reconciliationMissingFilesGauge_reflectsSupplierValue() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        JoxetteMetrics metrics = new JoxetteMetrics(registry);
        java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger(0);

        metrics.registerReconciliationMissingFilesGauge(count::get);
        count.set(3);

        assertThat(registry.get("joxette.reconciliation.missing.files").gauge().value()).isEqualTo(3.0);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl joxette-service -am test -Dtest=JoxetteMetricsReconciliationGaugeTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — no meter named `joxette.reconciliation.orphaned.files`/`joxette.reconciliation.missing.files` is registered (the Task 2 stubs are no-ops).

- [ ] **Step 3: Replace the gauge stubs with real registration**

In `joxette-service/src/main/java/com/joxette/metrics/JoxetteMetrics.java`, replace the two temporary stub methods added in Task 2 with:

```java
    public void registerReconciliationOrphanedFilesGauge(java.util.function.Supplier<Integer> countSupplier) {
        if (registeredGaugeIds.add("reconciliation:orphaned")) {
            retainedGaugeState.add(countSupplier);
            Gauge.builder("joxette.reconciliation.orphaned.files", countSupplier,
                          s -> { try { Integer v = s.get(); return v != null ? v.doubleValue() : 0.0; } catch (Exception e) { return 0.0; } })
                    .description("Files present in object storage but not tracked by the catalog, as of the most recent completed reconciliation run")
                    .register(registry);
        }
    }

    public void registerReconciliationMissingFilesGauge(java.util.function.Supplier<Integer> countSupplier) {
        if (registeredGaugeIds.add("reconciliation:missing")) {
            retainedGaugeState.add(countSupplier);
            Gauge.builder("joxette.reconciliation.missing.files", countSupplier,
                          s -> { try { Integer v = s.get(); return v != null ? v.doubleValue() : 0.0; } catch (Exception e) { return 0.0; } })
                    .description("Files the catalog's current snapshot references but that are no longer present in object storage, as of the most recent completed reconciliation run")
                    .register(registry);
        }
    }
```

Place both immediately after `registerInlinedDataGauge` in the existing `// Catalog sizes` section (or add a new `// Reconciliation` section comment above them — either is fine; use a new section comment for discoverability):

```java
    // =========================================================================
    // Reconciliation
    // =========================================================================
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl joxette-service -am test -Dtest=JoxetteMetricsReconciliationGaugeTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `Tests run: 2, Failures: 0, Errors: 0`

- [ ] **Step 5: Create `ReconciliationScheduler`** (no dedicated test — mirrors `RetentionScheduler`, which also has none in this codebase; it is a two-line `@Scheduled` pass-through)

```java
package com.joxette.reconciliation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives the periodic catalog/object-storage reconciliation audit using a Spring
 * {@code @Scheduled} cron job, independent of compaction and retention.
 *
 * <p>The cron expression is read from {@code joxette.reconciliation.schedule}
 * (default {@code "0 0 4 * * *"} — daily at 04:00:00 local time, after retention
 * at 01:00 and compaction at 03:00). Must use Spring's 6-field cron syntax:
 * {@code <sec> <min> <hour> <dom> <month> <dow>}.
 *
 * <p>The actual thread is provided by the {@code compactionTaskScheduler} bean
 * configured in {@link com.joxette.config.SchedulingConfig}. {@link ReconciliationService}
 * guards against overlapping runs with an {@link java.util.concurrent.atomic.AtomicBoolean}
 * plus the cross-instance {@code compaction_locks} distributed lock. Scheduled runs
 * never request orphan recovery — only a manual {@code POST /compaction/trigger-reconciliation}
 * call can opt into that.
 */
@Component
@ConditionalOnProperty(name = "joxette.reconciliation.enabled", matchIfMissing = true)
public class ReconciliationScheduler {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationScheduler.class);

    private final ReconciliationService reconciliationService;

    public ReconciliationScheduler(ReconciliationService reconciliationService) {
        this.reconciliationService = reconciliationService;
    }

    @Scheduled(cron = "${joxette.reconciliation.schedule:0 0 4 * * *}")
    public void runReconciliation() {
        log.info("Scheduled reconciliation starting");
        reconciliationService.runScheduled();
    }
}
```

- [ ] **Step 6: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/reconciliation/ReconciliationScheduler.java \
        joxette-service/src/main/java/com/joxette/metrics/JoxetteMetrics.java \
        joxette-service/src/test/java/com/joxette/metrics/JoxetteMetricsReconciliationGaugeTest.java
git commit -m "feat(reconciliation): add ReconciliationScheduler and real drift-count gauges"
```

---

### Task 7: REST endpoints on `CompactionController`

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/compaction/CompactionController.java`
- Modify: `joxette-service/src/test/java/com/joxette/api/error/CompactionControllerProblemDetailTest.java` (constructor signature changed — 3 call sites)
- Test: `joxette-service/src/test/java/com/joxette/compaction/CompactionControllerReconciliationTest.java`

**Interfaces:**
- Consumes: `com.joxette.reconciliation.ReconciliationService` (`getStatus()`, `getHistory(int)`, `beginRun(TriggerSource, List<String>, boolean)`), `com.joxette.reconciliation.ReconciliationRun`, `com.joxette.reconciliation.ReconciliationStatus`, `com.joxette.lifecycle.BackgroundTaskRegistry.submit(String, Runnable)`.
- Produces: `GET /compaction/reconciliation-status`, `GET /compaction/reconciliation-history?limit=`, `POST /compaction/trigger-reconciliation` (body `TriggerReconciliationRequest(List<String> targets, Boolean recoverOrphanedFiles)`).

- [ ] **Step 1: Write the failing test**

```java
package com.joxette.compaction;

import com.joxette.lifecycle.BackgroundTaskRegistry;
import com.joxette.reconciliation.ReconciliationRun;
import com.joxette.reconciliation.ReconciliationService;
import com.joxette.reconciliation.ReconciliationStatus;
import com.joxette.config.JoxetteProperties;
import com.joxette.api.error.GlobalExceptionHandler;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CompactionControllerReconciliationTest {

    @Mock CompactionService compactionService;
    @Mock RetentionService retentionService;
    @Mock ReconciliationService reconciliationService;
    @Mock JoxetteProperties props;
    @Mock CompactionLockManager lockManager;
    @Mock org.apache.pekko.actor.typed.ActorRef<CompactionSingletonActor.CompactionCommand> compactionSingleton;

    private static ActorSystem<Void> actorSystem;
    private MockMvc mvc;

    @BeforeAll
    static void startActorSystem() {
        actorSystem = ActorSystem.create(Behaviors.empty(), "compaction-recon-test");
    }

    @AfterAll
    static void stopActorSystem() {
        actorSystem.terminate();
    }

    @BeforeEach
    void setUp() {
        BackgroundTaskRegistry taskRegistry = new BackgroundTaskRegistry();
        taskRegistry.start();
        CompactionController controller = new CompactionController(
                compactionService, retentionService, reconciliationService,
                compactionSingleton, actorSystem, props, taskRegistry, lockManager);
        mvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    @Test
    void getReconciliationStatus_returnsServiceResult() throws Exception {
        ReconciliationStatus status = new ReconciliationStatus(null, Instant.parse("2026-08-07T04:00:00Z"), false);
        when(reconciliationService.getStatus()).thenReturn(status);

        mvc.perform(get("/compaction/reconciliation-status"))
           .andExpect(status().isOk())
           .andExpect(jsonPath("$.running").value(false))
           .andExpect(jsonPath("$.nextScheduledRun").value("2026-08-07T04:00:00Z"));
    }

    @Test
    void triggerReconciliation_returns202WithRun() throws Exception {
        ReconciliationRun run = new ReconciliationRun(1L, Instant.now(), null, RunStatus.RUNNING,
                TriggerSource.MANUAL, java.util.List.of(), 0, 0, 0, 0, 0, 0, false, null);
        when(reconciliationService.beginRun(TriggerSource.MANUAL, null, false)).thenReturn(run);

        mvc.perform(post("/compaction/trigger-reconciliation")
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content("{}"))
           .andExpect(status().isAccepted())
           .andExpect(jsonPath("$.id").value(1))
           .andExpect(jsonPath("$.status").value("running"));
    }

    @Test
    void triggerReconciliation_whileAlreadyRunning_returns409() throws Exception {
        when(reconciliationService.beginRun(TriggerSource.MANUAL, null, false))
                .thenThrow(com.joxette.api.error.ConflictException.reconciliationAlreadyRunning());

        mvc.perform(post("/compaction/trigger-reconciliation")
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content("{}"))
           .andExpect(status().isConflict())
           .andExpect(jsonPath("$.detail").value("Reconciliation run already in progress"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl joxette-service -am test -Dtest=CompactionControllerReconciliationTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL to compile — `CompactionController`'s constructor does not accept a `ReconciliationService` argument yet.

- [ ] **Step 3: Add the constructor parameter and field**

In `joxette-service/src/main/java/com/joxette/compaction/CompactionController.java`, add imports:

```java
import com.joxette.reconciliation.ReconciliationRun;
import com.joxette.reconciliation.ReconciliationService;
import com.joxette.reconciliation.ReconciliationStatus;
import java.util.List;
```
(`List` is already imported — skip if so; verify before adding a duplicate import.)

Add the field:

```java
    private final ReconciliationService reconciliationService;
```

Update the constructor signature and assignment (insert the new parameter right after `retentionService` to match the field's position, and keep every other parameter and assignment unchanged):

```java
    public CompactionController(
            CompactionService compactionService,
            RetentionService retentionService,
            ReconciliationService reconciliationService,
            ActorRef<CompactionSingletonActor.CompactionCommand> compactionSingleton,
            ActorSystem<?> system,
            JoxetteProperties props,
            BackgroundTaskRegistry taskRegistry,
            CompactionLockManager lockManager) {
        this.compactionService   = compactionService;
        this.retentionService    = retentionService;
        this.reconciliationService = reconciliationService;
        this.compactionSingleton = compactionSingleton;
        this.system              = system;
        this.props               = props;
        this.taskRegistry        = taskRegistry;
        this.lockManager         = lockManager;
    }
```

- [ ] **Step 4: Add the three endpoints**

Immediately after the existing `triggerRetention()` method and before the `// Response / request types` section comment:

```java
    @Operation(
        operationId = "getReconciliationStatus",
        summary = "Get catalog/object-storage reconciliation status",
        description = "Returns the current reconciliation status including the most-recent run summary " +
                      "(orphaned and missing file counts), the next scheduled cron fire time, and whether " +
                      "a run is currently active."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Current reconciliation status",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = ReconciliationStatus.class))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/reconciliation-status", produces = MediaType.APPLICATION_JSON_VALUE)
    public ReconciliationStatus getReconciliationStatus() throws SQLException {
        return reconciliationService.getStatus();
    }

    @Operation(
        operationId = "getReconciliationHistory",
        summary = "Get reconciliation run history",
        description = "Returns the most-recent reconciliation runs in descending chronological order."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Reconciliation run history",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/reconciliation-history", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<ReconciliationRun> getReconciliationHistory(
            @RequestParam(defaultValue = "20") int limit) throws SQLException {
        return reconciliationService.getHistory(limit);
    }

    @Operation(
        operationId = "triggerReconciliation",
        summary = "Trigger a catalog/object-storage reconciliation run",
        description = "Starts an immediate reconciliation run asynchronously. Returns 409 if a run is " +
                      "already in progress on this or another instance sharing the catalog. " +
                      "`recoverOrphanedFiles` is opt-in and defaults to false; scheduled runs never set it."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "202", description = "Reconciliation run started",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = ReconciliationRun.class))),
        @ApiResponse(responseCode = "409", description = "Reconciliation run already in progress",
            content = @Content(schema = @Schema(type = "string")))
    })
    @PostMapping(value = "/trigger-reconciliation", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<ReconciliationRun> triggerReconciliation(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                description = "Optional scoping/recovery options.",
                required = false,
                content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = TriggerReconciliationRequest.class)))
            @Valid @RequestBody(required = false) TriggerReconciliationRequest body) throws SQLException {
        List<String> targets = (body != null) ? body.targets() : null;
        boolean recoverOrphanedFiles = (body != null && body.recoverOrphanedFiles() != null)
                && body.recoverOrphanedFiles();
        ReconciliationRun run = reconciliationService.beginRun(TriggerSource.MANUAL, targets, recoverOrphanedFiles);
        taskRegistry.submit("reconciliation-manual-" + run.id(),
                () -> reconciliationService.executeRun(run.id(), targets, recoverOrphanedFiles));
        return ResponseEntity.accepted().body(run);
    }
```

Add the request-body record inside the existing `// Response / request types` section, after `TriggerRequest`:

```java
    @Schema(description = "Optional request body for POST /compaction/trigger-reconciliation",
            example = "{\"targets\": [\"orders.events\", \"entity:order\"], \"recoverOrphanedFiles\": false}")
    record TriggerReconciliationRequest(
            @Schema(description = "Table scoping, reusing compaction's format: bare topic name or " +
                    "'entity:{type}'. Null or absent means every cassette table.",
                    example = "[\"orders.events\", \"entity:order\"]")
            List<String> targets,
            @Schema(description = "Whether to register orphaned files back into the catalog via " +
                    "ducklake_add_data_files. Defaults to false.", example = "false")
            Boolean recoverOrphanedFiles) {}
```

- [ ] **Step 5: Fix the constructor call sites in `CompactionControllerProblemDetailTest`**

In `joxette-service/src/test/java/com/joxette/api/error/CompactionControllerProblemDetailTest.java`:

Add the import:

```java
import com.joxette.reconciliation.ReconciliationService;
```

Add the mock field alongside the existing `@Mock` fields:

```java
    @Mock ReconciliationService reconciliationService;
```

Update all three `new CompactionController(...)` call sites (lines ~111, ~134, ~156) from:

```java
        CompactionController controller = new CompactionController(
                compactionService, retentionService, busySingleton, actorSystem, props, taskRegistry(), lockManager);
```

to:

```java
        CompactionController controller = new CompactionController(
                compactionService, retentionService, reconciliationService, busySingleton, actorSystem, props, taskRegistry(), lockManager);
```

(and the equivalent two occurrences using `acceptingSingleton` in place of `busySingleton`).

- [ ] **Step 6: Run tests to verify they pass**

Run: `mvn -pl joxette-service -am test -Dtest=CompactionControllerReconciliationTest,CompactionControllerProblemDetailTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `Tests run: 6, Failures: 0, Errors: 0` (3 new + 3 existing).

- [ ] **Step 7: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/compaction/CompactionController.java \
        joxette-service/src/test/java/com/joxette/api/error/CompactionControllerProblemDetailTest.java \
        joxette-service/src/test/java/com/joxette/compaction/CompactionControllerReconciliationTest.java
git commit -m "feat(reconciliation): expose reconciliation-status/history/trigger REST endpoints"
```

---

### Task 8: End-to-end REST integration test

**Files:**
- Test: `joxette-service/src/test/java/com/joxette/it/ReconciliationEndToEndIT.java`

**Interfaces:**
- Consumes: the full REST surface added in Task 7, exercised over HTTP exactly as a real client would (not by calling `ReconciliationService` directly, unlike Tasks 3-5).

- [ ] **Step 1: Write the test** (this task has no separate red/green split — it is a pure verification task confirming the wiring from Tasks 1-7 works end-to-end over HTTP; it must pass on first run since every unit it exercises is already tested individually)

```java
package com.joxette.it;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.client.RestTemplate;
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
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class ReconciliationEndToEndIT {

    private static final String BUCKET = "joxette-recon-e2e-test";

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
    }

    @LocalServerPort private int port;
    @Autowired private Connection duckDB;
    private final RestTemplate restTemplate = new RestTemplate();

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    @Test
    void trigger_status_history_roundTripOverRest() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS lake.main.general_orders_events (" +
                    "recorded_at TIMESTAMPTZ, kafka_offset BIGINT, kafka_partition INTEGER, " +
                    "kafka_timestamp TIMESTAMPTZ, kafka_key VARCHAR, kafka_value BLOB, " +
                    "metadata VARCHAR, headers STRUCT(key VARCHAR, value VARCHAR)[], message_type VARCHAR)");
            st.execute("COPY (SELECT 1 AS id) TO " +
                    "'s3://" + BUCKET + "/data/main/general_orders_events/stray.parquet' (FORMAT PARQUET)");
        }

        ResponseEntity<Map> trigger = restTemplate.postForEntity(
                url("/compaction/trigger-reconciliation"), Map.of(), Map.class);
        assertThat(trigger.getStatusCode().value()).isEqualTo(202);
        assertThat(trigger.getBody().get("status")).isEqualTo("running");

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            ResponseEntity<Map> statusResp =
                    restTemplate.getForEntity(url("/compaction/reconciliation-status"), Map.class);
            assertThat(statusResp.getBody().get("running")).isEqualTo(false);
            Map<?, ?> lastRun = (Map<?, ?>) statusResp.getBody().get("lastRun");
            assertThat(lastRun).isNotNull();
            assertThat(lastRun.get("status")).isEqualTo("completed");
            assertThat(((Number) lastRun.get("orphanedFiles")).intValue()).isEqualTo(1);
        });

        ResponseEntity<java.util.List> history = restTemplate.getForEntity(
                url("/compaction/reconciliation-history?limit=5"), java.util.List.class);
        assertThat(history.getBody()).hasSize(1);
    }

    @Test
    void trigger_whileAlreadyRunning_returns409() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS lake.main.general_second_topic (" +
                    "recorded_at TIMESTAMPTZ, kafka_offset BIGINT, kafka_partition INTEGER, " +
                    "kafka_timestamp TIMESTAMPTZ, kafka_key VARCHAR, kafka_value BLOB, " +
                    "metadata VARCHAR, headers STRUCT(key VARCHAR, value VARCHAR)[], message_type VARCHAR)");
        }
        restTemplate.postForEntity(url("/compaction/trigger-reconciliation"), Map.of(), Map.class);

        org.springframework.web.client.HttpClientErrorException ex = null;
        try {
            restTemplate.postForEntity(url("/compaction/trigger-reconciliation"), Map.of(), Map.class);
        } catch (org.springframework.web.client.HttpClientErrorException e) {
            ex = e;
        }
        assertThat(ex).isNotNull();
        assertThat(ex.getStatusCode().value()).isEqualTo(409);

        // Let the first run finish so it doesn't leak into other test classes sharing MinIO/Spring context.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            ResponseEntity<Map> statusResp =
                    restTemplate.getForEntity(url("/compaction/reconciliation-status"), Map.class);
            assertThat(statusResp.getBody().get("running")).isEqualTo(false);
        });
    }
}
```

- [ ] **Step 2: Run test to verify it passes**

Run: `mvn -pl joxette-service -am verify -Dit.test=ReconciliationEndToEndIT -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false`
Expected: `Tests run: 2, Failures: 0, Errors: 0`

If `org.awaitility.Awaitility` is not already a test dependency, check `pom.xml` for an existing `awaitility` dependency (per project memory, it is already used elsewhere in this codebase for async test assertions — e.g. wherever `FollowModeIntegrationTest`/similar polls status) before adding one; only add `<dependency><groupId>org.awaitility</groupId><artifactId>awaitility</artifactId><scope>test</scope></dependency>` to `joxette-service/pom.xml` if it is genuinely missing.

- [ ] **Step 3: Run the full backend test suite to confirm no regressions**

Run: `mvn -pl joxette-service -am verify`
Expected: `BUILD SUCCESS`, all prior compaction/retention/reconciliation tests green.

- [ ] **Step 4: Commit**

```bash
git add joxette-service/src/test/java/com/joxette/it/ReconciliationEndToEndIT.java joxette-service/pom.xml
git commit -m "test(reconciliation): add end-to-end REST integration test for the full audit flow"
```

---

## Self-Review Notes

- **Spec coverage:** Schema/config (Task 1), DTOs/lifecycle/locking (Task 2), orphan detection (Task 3), missing detection (Task 4), recovery (Task 5), scheduler/metrics (Task 6), REST surface (Task 7), end-to-end wiring (Task 8) — every section of `docs/superpowers/specs/2026-08-06-catalog-reconciliation-design.md`'s Backend Design is covered. The spec's UI Design section is intentionally out of scope for this plan (see the plan header) and will get its own plan once these REST endpoints exist for real.
- **Type consistency verified:** `scanMissingFiles`'s return type change (`List<String>` → `List<MissingFile>`) from Task 2 to Task 4 is called out explicitly in Task 4's Interfaces block and Step 3, with the exact `executeRun`/`buildDetailsJson` call-site edits shown — not left implicit.
- **No placeholders:** every step has complete, runnable code; every SQL string, column name, and function signature was verified directly against a live DuckDB+DuckLake instance while writing this plan (see the design spec's revision history) rather than assumed from documentation prose alone.
