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

            List<MissingFile> missing = scanMissingFiles(tables);
            missingFiles = missing.size();
            missingBytes = missing.stream().mapToLong(MissingFile::lastKnownSizeBytes).sum();

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

    /** Represents one file the catalog's current snapshot references that no longer exists in storage. */
    record MissingFile(String path, long lastKnownSizeBytes) {}

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
        return com.joxette.db.SchemaManager.listCassetteTableNames(duckDB);
    }

    // =========================================================================
    // Details JSON
    // =========================================================================

    private record ReconciliationDetails(
            List<String> orphanedFiles, boolean orphanedTruncated,
            List<String> missingFiles, boolean missingTruncated) {}

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

    // =========================================================================
    // reconciliation_history CRUD
    // =========================================================================

    // Package-private (not private) so fast unit tests can drive the bookkeeping
    // layer directly without a real DuckLake scan — see ReconciliationServiceLifecycleTest.
    long insertRunRecord(TriggerSource triggeredBy, List<String> targets,
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

    // Package-private (not private) — see insertRunRecord's note above.
    void updateRunRecord(long runId, RunStatus status, int tablesScanned,
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
