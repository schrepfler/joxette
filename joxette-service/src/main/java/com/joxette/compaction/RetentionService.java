package com.joxette.compaction;

import com.joxette.config.JoxetteProperties;
import com.joxette.db.SchemaManager;
import com.joxette.metrics.JoxetteMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import com.joxette.management.ConfigRepository;
import com.joxette.management.EntityTypeConfig;
import com.joxette.management.TopicConfig;
import com.joxette.management.TopicMode;
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

/**
 * Enforces data retention policies by deleting DuckLake rows older than the
 * {@code retention_days} configured for each topic and entity type.
 *
 * <h2>Retention scope per topic</h2>
 * <ul>
 *   <li>{@code lake.main.general_<topic>} — rows deleted where
 *       {@code recorded_at < now() - retention_days}</li>
 * </ul>
 *
 * <h2>Retention scope per entity type</h2>
 * <ul>
 *   <li>{@code lake.main.entity_<type>} — rows deleted where
 *       {@code recorded_at < now() - retention_days}</li>
 *   <li>{@code known_entities} — rows deleted where
 *       {@code entity_type = ? AND last_seen < now() - retention_days}</li>
 * </ul>
 *
 * <p>Every run is recorded in {@code retention_history}.  Topics or entity
 * types with {@code retention_days IS NULL} are silently skipped.
 *
 * <p>An {@link AtomicBoolean} guard prevents overlapping runs.
 */
@Service
@DependsOn("dbSchemaManager")
public class RetentionService {

    private static final Logger log = LoggerFactory.getLogger(RetentionService.class);

    private final Connection duckDB;
    private final ConfigRepository configRepo;
    private final JoxetteProperties props;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Counter entityRowsCounter;
    private final Counter generalRowsCounter;
    private final Counter knownEntitiesRowsCounter;
    private final Timer retentionTimer;

    public RetentionService(Connection duckDB, ConfigRepository configRepo, JoxetteProperties props,
                             JoxetteMetrics joxetteMetrics) {
        this.duckDB                    = duckDB;
        this.configRepo                = configRepo;
        this.props                     = props;
        this.entityRowsCounter         = joxetteMetrics.retentionRowsDeleted("entity");
        this.generalRowsCounter        = joxetteMetrics.retentionRowsDeleted("general");
        this.knownEntitiesRowsCounter  = joxetteMetrics.retentionRowsDeleted("known_entities");
        this.retentionTimer            = joxetteMetrics.retentionDuration();
    }

    // =========================================================================
    // Public API
    // =========================================================================

    public boolean isRunning() {
        return running.get();
    }

    /**
     * Convenience entry point used by the scheduler.
     * Silently skips if a retention run is already in progress.
     */
    public void runScheduled() {
        if (!running.compareAndSet(false, true)) {
            log.warn("Skipping scheduled retention: a run is still in progress");
            return;
        }
        long runId;
        try {
            runId = insertRunRecord(TriggerSource.SCHEDULED);
        } catch (SQLException e) {
            running.set(false);
            log.error("Failed to insert retention run record", e);
            return;
        }
        executeRun(runId);
    }

    /**
     * Atomically marks a new run as started and inserts a {@code "running"} row
     * in {@code retention_history}.
     *
     * @throws com.joxette.api.error.ConflictException if a retention run is already in progress
     */
    public RetentionRun beginRun(TriggerSource triggeredBy) throws SQLException {
        if (!running.compareAndSet(false, true)) {
            throw com.joxette.api.error.ConflictException.retentionAlreadyRunning();
        }
        long id = insertRunRecord(triggeredBy);
        return getRunById(id);
    }

    /**
     * Performs the actual retention work for a run started with {@link #beginRun}.
     * Always resets the running flag on exit, even on error.
     */
    public void executeRun(long runId) {
        long entityRows = 0;
        long generalRows = 0;
        long knownEntitiesRows = 0;
        long start = System.nanoTime();
        try {
            generalRows = enforceTopicRetention();
            long[] entityCounts = enforceEntityRetention();
            entityRows        = entityCounts[0];
            knownEntitiesRows = entityCounts[1];
            checkpoint();
            generalRowsCounter.increment(generalRows);
            entityRowsCounter.increment(entityRows);
            knownEntitiesRowsCounter.increment(knownEntitiesRows);
            retentionTimer.record(System.nanoTime() - start, java.util.concurrent.TimeUnit.NANOSECONDS);
            updateRunRecord(runId, RunStatus.COMPLETED, entityRows, generalRows, knownEntitiesRows, null);
            log.info("Retention run {} completed: {} general rows, {} entity rows, {} known_entities rows deleted",
                    runId, generalRows, entityRows, knownEntitiesRows);
        } catch (Exception e) {
            log.error("Retention run {} failed", runId, e);
            retentionTimer.record(System.nanoTime() - start, java.util.concurrent.TimeUnit.NANOSECONDS);
            try {
                updateRunRecord(runId, RunStatus.FAILED, entityRows, generalRows, knownEntitiesRows, e.getMessage());
            } catch (SQLException se) {
                log.error("Failed to update retention_history for run {}", runId, se);
            }
        } finally {
            running.set(false);
        }
    }

    public List<RetentionRun> getHistory(int limit) throws SQLException {
        List<RetentionRun> result = new ArrayList<>();
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    SELECT id, started_at, completed_at, status, triggered_by,
                           entity_rows_deleted, general_rows_deleted, known_entities_deleted, error_message
                    FROM retention_history
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

    public RetentionRun getRunById(long id) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    SELECT id, started_at, completed_at, status, triggered_by,
                           entity_rows_deleted, general_rows_deleted, known_entities_deleted, error_message
                    FROM retention_history WHERE id = ?
                    """)) {
                ps.setLong(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) return mapRun(rs);
                    throw com.joxette.api.error.ResourceNotFoundException.retentionRun(id);
                }
            }
        }
    }

    public RetentionStatus getStatus() throws SQLException {
        RetentionRun lastRun = queryLastRun();
        Instant nextScheduledRun = computeNextScheduledRun();
        return new RetentionStatus(lastRun, nextScheduledRun, running.get());
    }

    // =========================================================================
    // Retention enforcement
    // =========================================================================

    private long enforceTopicRetention() throws SQLException {
        List<TopicConfig> topics = configRepo.listTopics();
        long total = 0;
        for (TopicConfig tc : topics) {
            if (tc.retentionDays() == null) continue;
            // Only general-mode topics have a cassette table; entity_only topics skip this
            if (tc.mode() == TopicMode.ENTITY_ONLY) continue;
            try {
                long deleted = deleteFromGeneralCassette(tc.topic(), tc.retentionDays());
                if (deleted > 0) {
                    log.debug("Retention: deleted {} rows from general cassette for topic '{}' (retention={} days)",
                            deleted, tc.topic(), tc.retentionDays());
                }
                total += deleted;
            } catch (Exception e) {
                log.warn("Retention: skipping topic '{}' due to error: {}", tc.topic(), e.getMessage());
                // Continue with remaining topics rather than aborting the entire run
            }
        }
        return total;
    }

    private long deleteFromGeneralCassette(String topic, int retentionDays) throws SQLException {
        String tableName = "lake.main.general_" + SchemaManager.normalize(topic);
        String cutoff    = "CURRENT_TIMESTAMP - INTERVAL '" + retentionDays + " days'";
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement()) {
                return st.executeUpdate(
                        "DELETE FROM " + tableName + " WHERE recorded_at < " + cutoff);
            } catch (SQLException e) {
                // Table may not exist if it was never created (e.g. entity_only reconfigured later)
                log.warn("Retention skipped for general cassette '{}': {}", tableName, e.getMessage());
                return 0;
            }
        }
    }

    /** @return {@code [entityRowsDeleted, knownEntitiesDeleted]} */
    private long[] enforceEntityRetention() throws SQLException {
        List<EntityTypeConfig> entities = configRepo.listEntityTypes();
        long entityRows = 0;
        long knownEntitiesRows = 0;
        for (EntityTypeConfig etc : entities) {
            if (etc.retentionDays() == null) continue;
            int days    = etc.retentionDays();
            String type = etc.entityType();
            SchemaManager.validateEntityType(type);
            try {
                long cassDeleted = deleteFromEntityCassette(type, days);
                long keDeleted   = deleteFromKnownEntities(type, days);
                if (cassDeleted > 0 || keDeleted > 0) {
                    log.debug("Retention: deleted {} entity rows, {} known_entities rows for type '{}' (retention={} days)",
                            cassDeleted, keDeleted, type, days);
                }
                entityRows        += cassDeleted;
                knownEntitiesRows += keDeleted;
            } catch (Exception e) {
                // Per-entity failure: log and continue so a transient S3 error on one
                // entity type does not cause the entire run to fail and restart from scratch.
                log.warn("Retention: skipping entity type '{}' due to error (will retry next schedule): {}",
                        type, e.getMessage());
            }
        }
        return new long[]{entityRows, knownEntitiesRows};
    }

    private long deleteFromEntityCassette(String type, int retentionDays) throws SQLException {
        String cutoff = "CURRENT_TIMESTAMP - INTERVAL '" + retentionDays + " days'";
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement()) {
                return st.executeUpdate(
                        "DELETE FROM lake.main.entity_" + type + " WHERE recorded_at < " + cutoff);
            }
        }
    }

    private long deleteFromKnownEntities(String type, int retentionDays) throws SQLException {
        String cutoff = "CURRENT_TIMESTAMP - INTERVAL '" + retentionDays + " days'";
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM known_entities WHERE entity_type = ? AND last_seen < " + cutoff)) {
                ps.setString(1, type);
                return ps.executeUpdate();
            }
        }
    }

    // =========================================================================
    // DuckDB helpers
    // =========================================================================

    private void checkpoint() throws SQLException {
        synchronized (duckDB) {
            // Commit any open transaction from the DELETE statements before checkpointing.
            // CHECKPOINT requires no active transaction; skipping this causes the WAL to
            // grow unbounded in native memory.
            try {
                duckDB.commit();
            } catch (SQLException e) {
                log.debug("commit before retention CHECKPOINT: {} (may be auto-commit mode)", e.getMessage());
            }
            try (Statement st = duckDB.createStatement()) {
                st.execute("CHECKPOINT");
                log.debug("Retention checkpoint complete");
            } catch (SQLException e) {
                // In-memory DuckDB (e.g. in tests) does not support CHECKPOINT — safe to ignore.
                log.warn("Retention CHECKPOINT failed ({}); catalog metadata may not be persisted", e.getMessage());
                throw e;
            }
        }
    }

    // =========================================================================
    // retention_history CRUD
    // =========================================================================

    private long insertRunRecord(TriggerSource triggeredBy) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    INSERT INTO retention_history
                        (started_at, status, triggered_by,
                         entity_rows_deleted, general_rows_deleted, known_entities_deleted)
                    VALUES (?, 'running', ?, 0, 0, 0)
                    RETURNING id
                    """)) {
                ps.setTimestamp(1, Timestamp.from(Instant.now()));
                ps.setString(2, triggeredBy.getValue());
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) return rs.getLong(1);
                    throw new SQLException("INSERT into retention_history returned no generated id");
                }
            }
        }
    }

    private void updateRunRecord(long runId, RunStatus status,
                                  long entityRows, long generalRows, long knownEntitiesRows,
                                  String errorMessage) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    UPDATE retention_history
                    SET completed_at           = ?,
                        status                 = ?,
                        entity_rows_deleted    = ?,
                        general_rows_deleted   = ?,
                        known_entities_deleted = ?,
                        error_message          = ?
                    WHERE id = ?
                    """)) {
                ps.setTimestamp(1, Timestamp.from(Instant.now()));
                ps.setString(2, status.getValue());
                ps.setLong(3, entityRows);
                ps.setLong(4, generalRows);
                ps.setLong(5, knownEntitiesRows);
                ps.setString(6, errorMessage);
                ps.setLong(7, runId);
                ps.executeUpdate();
            }
        }
    }

    private RetentionRun queryLastRun() throws SQLException {
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery("""
                    SELECT id, started_at, completed_at, status, triggered_by,
                           entity_rows_deleted, general_rows_deleted, known_entities_deleted, error_message
                    FROM retention_history
                    ORDER BY started_at DESC LIMIT 1
                    """)) {
                return rs.next() ? mapRun(rs) : null;
            }
        }
    }

    private Instant computeNextScheduledRun() {
        try {
            CronExpression expr = CronExpression.parse(props.getRetention().getSchedule());
            LocalDateTime next = expr.next(LocalDateTime.now());
            if (next == null) return null;
            return next.atZone(ZoneId.systemDefault()).toInstant();
        } catch (Exception e) {
            log.warn("Cannot parse retention cron '{}' to compute next run: {}",
                    props.getRetention().getSchedule(), e.getMessage());
            return null;
        }
    }

    private static RetentionRun mapRun(ResultSet rs) throws SQLException {
        Timestamp completedTs = rs.getTimestamp("completed_at");
        return new RetentionRun(
                rs.getLong("id"),
                rs.getTimestamp("started_at").toInstant(),
                completedTs != null ? completedTs.toInstant() : null,
                RunStatus.fromValue(rs.getString("status")),
                TriggerSource.fromValue(rs.getString("triggered_by")),
                rs.getLong("entity_rows_deleted"),
                rs.getLong("general_rows_deleted"),
                rs.getLong("known_entities_deleted"),
                rs.getString("error_message")
        );
    }
}
