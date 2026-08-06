package com.joxette.compaction;

import com.joxette.config.JoxetteProperties;
import com.joxette.db.DuckDbErrors;
import com.joxette.management.ConfigRepository;
import com.joxette.metrics.JoxetteMetrics;
import io.micrometer.core.instrument.Timer;
import com.joxette.management.TopicMode;
import com.joxette.db.SchemaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Compacts entity and general cassette tables in DuckLake using
 * {@code ducklake_merge_adjacent_files} — an idempotent maintenance function
 * introduced in DuckLake 1.0.
 *
 * <h2>Function signature</h2>
 * Source: <a href="https://ducklake.select/docs/stable/duckdb/maintenance/merge_adjacent_files">
 * DuckLake docs — Merge Files</a>
 * <pre>
 *   CALL ducklake_merge_adjacent_files(
 *       ducklake_name VARCHAR,
 *       [table_name    VARCHAR],
 *       [schema        =&gt; VARCHAR],
 *       [max_compacted_files =&gt; BIGINT],
 *       [min_file_size =&gt; BIGINT],
 *       [max_file_size =&gt; BIGINT]
 *   )
 *   → TABLE(schema_name VARCHAR, table_name VARCHAR,
 *            files_processed BIGINT, files_created BIGINT)
 * </pre>
 * One result row is returned per output file created ({@code files_created} is always 1
 * per row; {@code files_processed} shows how many input files were merged into it).
 *
 * <h2>Note on ducklake_rewrite_data_files</h2>
 * <p>{@code ducklake_rewrite_data_files} is a separate function that rewrites files
 * containing a high ratio of delete markers (controlled by {@code delete_threshold}).
 * It is <em>not</em> used here for general file merging; it would be appropriate to
 * call from {@link RetentionService} after bulk-deleting entity rows (e.g. GDPR wipes).
 *
 * <h2>Compaction strategy</h2>
 * <p>One {@code CALL ducklake_merge_adjacent_files} is issued per entity type and per
 * general-cassette topic.  DuckLake internally determines which files need merging based
 * on file-size thresholds ({@code max_file_size} from {@code target-file-size-mb} config).
 * Files already at or above the target size are left untouched.
 *
 * <h2>Run tracking</h2>
 * <p>Every run is recorded in {@code compaction_history}.  An
 * {@link AtomicBoolean} guard prevents overlapping runs within this process.
 *
 * <h2>Distributed locking</h2>
 * <p>When multiple Joxette instances share a catalog (PostgreSQL or Quack at Stage 2/3
 * of the scaling path), two instances must not compact the same target concurrently —
 * {@code ducklake_merge_adjacent_files} is not safe to call in parallel on the same
 * DuckLake table.  Each compaction target (entity type or general-cassette topic) is
 * therefore guarded by a row in {@code compaction_locks} via {@link CompactionLockManager}.
 *
 * <p>Lock acquisition, heartbeating, and release are transparent to the caller —
 * targets whose lock is held by another instance are silently skipped and counted
 * as skipped in the run result.  A {@link LockHeartbeat} refreshes the lock's
 * {@code expires_at} every {@link CompactionLockManager#HEARTBEAT_INTERVAL_MINUTES}
 * minutes from a dedicated virtual thread while the merge is in progress.
 *
 * <p><b>Known limitation:</b> the heartbeat's {@link CompactionLockManager#refresh}
 * call and the merge's own {@code executeQuery} both run under {@code synchronized(duckDB)}
 * on this instance's single shared connection, so a heartbeat tick that fires while the
 * merge statement is still executing simply blocks until the merge's synchronized block
 * exits — it cannot land concurrently with an in-flight merge. In practice this means the
 * heartbeat protects the common case (merges that finish well inside one TTL window, where
 * it is a no-op) but does not, by itself, prevent a cross-instance lock steal for a single
 * {@code ducklake_merge_adjacent_files} call that runs longer than {@code lockTtlMinutes}
 * end-to-end — closing that residual gap would need lock heartbeat I/O moved off the main
 * compaction connection (e.g. a small dedicated connection for {@code compaction_locks}
 * only). The documented mitigation in the meantime is the deployment guidance in
 * {@code docs/clustering-deployment.md} / {@code docs/operator-design.md}: run exactly one
 * compaction-enabled replica, which is what actually prevents the two-instance race this
 * whole mechanism exists as a backstop for.
 */
@Service
@DependsOn("dbSchemaManager")
public class CompactionService {

    private static final Logger log = LoggerFactory.getLogger(CompactionService.class);

    private final Connection           duckDB;
    private final JoxetteProperties    props;
    private final ConfigRepository     configRepo;
    private final AtomicBoolean        running = new AtomicBoolean(false);
    private final io.micrometer.core.instrument.Counter filesProcessedCounter;
    private final io.micrometer.core.instrument.Counter filesCreatedCounter;
    private final Timer compactionTimer;
    private final CompactionLockManager lockManager;

    public CompactionService(Connection duckDB, JoxetteProperties props, ConfigRepository configRepo,
                              JoxetteMetrics joxetteMetrics, CompactionLockManager lockManager) {
        this.duckDB                = duckDB;
        this.props                 = props;
        this.configRepo            = configRepo;
        this.filesProcessedCounter = joxetteMetrics.compactionFilesProcessed();
        this.filesCreatedCounter   = joxetteMetrics.compactionFilesCreated();
        this.compactionTimer       = joxetteMetrics.compactionDuration();
        this.lockManager           = lockManager;
    }

    // =========================================================================
    // Public API
    // =========================================================================

    public boolean isRunning() {
        return running.get();
    }

    /**
     * Atomically marks a new run as started and inserts a {@code "running"} row
     * in {@code compaction_history}.
     *
     * <p>The caller is responsible for submitting {@link #executeRun} to a
     * background thread after this method returns.
     *
     * @throws com.joxette.api.error.ConflictException if a compaction is already in progress
     */
    public CompactionRun beginRun(TriggerSource triggeredBy, List<String> targets) throws SQLException {
        if (!running.compareAndSet(false, true)) {
            throw com.joxette.api.error.ConflictException.compactionAlreadyRunning();
        }
        long id = insertRunRecord(triggeredBy, targets);
        return getRunById(id);
    }

    /**
     * Performs the actual compaction work for a run that was started with
     * {@link #beginRun}.  Always resets the running flag on exit, even on error.
     *
     * <p>This method is safe to call from any thread; DuckDB access is
     * serialised via {@code synchronized(duckDB)}.
     */
    public void executeRun(long runId, List<String> targets) {
        int entityTypes = 0;
        int generalTopics = 0;
        FileStats totalFileStats = FileStats.EMPTY;
        long start = System.nanoTime();
        try {
            // Opportunistic reclaim of locks orphaned by a crashed instance, once per
            // run rather than once per target — both sweeps below cover the whole
            // compaction_locks table in one pass, so running them once here up front
            // covers every target this run is about to attempt instead of re-scanning
            // the table per entity type / topic.
            reclaimOrphanedLocksIfPossible();

            CompactionResult entityResult = compactEntityTypes(targets);
            entityTypes    = entityResult.unitsProcessed();
            totalFileStats = totalFileStats.add(entityResult.fileStats());

            CompactionResult generalResult = compactGeneralIfEnabled(targets);
            generalTopics  = generalResult.unitsProcessed();
            totalFileStats = totalFileStats.add(generalResult.fileStats());

            checkpoint();
            filesProcessedCounter.increment(totalFileStats.filesProcessed());
            filesCreatedCounter.increment(totalFileStats.filesCreated());
            compactionTimer.record(System.nanoTime() - start, java.util.concurrent.TimeUnit.NANOSECONDS);
            updateRunRecord(runId, RunStatus.COMPLETED, entityTypes, generalTopics, totalFileStats, null);
            log.info("Compaction run {} completed: {} entity types, {} general topics, "
                            + "files_processed={} files_created={}",
                    runId, entityTypes, generalTopics,
                    totalFileStats.filesProcessed(), totalFileStats.filesCreated());
        } catch (Exception e) {
            log.error("Compaction run {} failed", runId, e);
            compactionTimer.record(System.nanoTime() - start, java.util.concurrent.TimeUnit.NANOSECONDS);
            try {
                updateRunRecord(runId, RunStatus.FAILED, entityTypes, generalTopics, totalFileStats, e.getMessage());
            } catch (SQLException se) {
                log.error("Failed to update compaction_history for run {}", runId, se);
            }
        } finally {
            running.set(false);
        }
    }

    /**
     * Convenience entry point used by the scheduler.
     * If a run is already in progress the call is silently skipped.
     */
    public void runScheduled() {
        if (!running.compareAndSet(false, true)) {
            log.warn("Skipping scheduled compaction: a run is still in progress");
            return;
        }
        long runId;
        try {
            runId = insertRunRecord(TriggerSource.SCHEDULED, null);
        } catch (SQLException e) {
            running.set(false);
            log.error("Failed to insert compaction run record", e);
            return;
        }
        executeRun(runId, null);
    }

    public CompactionStatus getStatus() throws SQLException {
        CompactionRun lastRun = queryLastRun();
        Instant nextScheduledRun = computeNextScheduledRun();
        return new CompactionStatus(lastRun, nextScheduledRun, running.get());
    }

    public List<CompactionRun> getHistory(int limit) throws SQLException {
        List<CompactionRun> result = new ArrayList<>();
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    SELECT id, started_at, completed_at, status, triggered_by, targets,
                           entity_buckets_compacted, general_partitions_compacted,
                           files_processed, files_created, error_message
                    FROM compaction_history
                    ORDER BY started_at DESC
                    LIMIT ?
                    """)) {
                ps.setInt(1, limit);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        result.add(mapRun(rs));
                    }
                }
            }
        }
        return result;
    }

    public CompactionRun getRunById(long id) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    SELECT id, started_at, completed_at, status, triggered_by, targets,
                           entity_buckets_compacted, general_partitions_compacted,
                           files_processed, files_created, error_message
                    FROM compaction_history WHERE id = ?
                    """)) {
                ps.setLong(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) return mapRun(rs);
                    throw com.joxette.api.error.ResourceNotFoundException.compactionRun(id);
                }
            }
        }
    }

    // =========================================================================
    // Distributed lock helpers (opportunistic cleanup + heartbeat)
    // =========================================================================

    /**
     * Sweeps {@code compaction_locks} once at the top of a run for two independent
     * hazards:
     * <ol>
     *   <li>{@link CompactionLockManager#cleanExpiredLocks()} — a lock whose owning
     *       instance is still alive but whose merge simply ran past
     *       {@code lock-ttl-minutes}.</li>
     *   <li>{@link CompactionLockManager#cleanLocksForDeadInstances()} — a lock whose
     *       owning instance crashed mid-merge (and so never reached its
     *       {@code finally}-block {@link CompactionLockManager#release}) and is no
     *       longer present in the live instance registry. This does not wait for that
     *       instance's own restart-time {@code @PostConstruct} cleanup — any live
     *       instance's run reclaims it, bounded by the registry's heartbeat/staleness
     *       cadence (~1-2 minutes) rather than the next restart.</li>
     * </ol>
     *
     * <p>Without either sweep, {@link CompactionLockManager#tryAcquire} only checks
     * whether a row exists, not whether it is expired or dead-owned, so a crashed
     * instance's lock would block that target forever. This is the same "log and
     * continue" error-handling style used for {@link #doCompactEntityType}'s and
     * {@link #doCompactGeneralTopic}'s {@code tryAcquire} calls — a cleanup failure
     * must not abort the run; it just means the stuck rows persist until the next
     * successful sweep.
     */
    private void reclaimOrphanedLocksIfPossible() {
        try {
            lockManager.cleanExpiredLocks();
        } catch (SQLException e) {
            log.warn("Could not clean expired compaction locks ({}); proceeding with this run — "
                    + "any expired lock rows will keep blocking their target until the next "
                    + "successful cleanup", e.getMessage());
        }
        lockManager.cleanLocksForDeadInstances();
    }

    /**
     * Starts a background heartbeat for {@code target}, refreshing its lock's
     * {@code expires_at} roughly every {@link CompactionLockManager#HEARTBEAT_INTERVAL_MINUTES}
     * minutes so a legitimately long merge does not let the TTL expire out from under it.
     *
     * <p>Caller must {@link LockHeartbeat#stop()} it once the merge finishes, in the same
     * {@code finally} block that calls {@link CompactionLockManager#release}.
     *
     * <p>See the class-level "Distributed locking" javadoc for a known limitation: each
     * {@link CompactionLockManager#refresh} tick shares {@code synchronized(duckDB)} with
     * the merge statement it is meant to protect, so it cannot execute while that
     * statement is still running — it is a best-effort backstop, not a guarantee, for
     * merges that individually outlive one TTL window.
     */
    private LockHeartbeat startHeartbeat(String target) {
        return new LockHeartbeat(lockManager, target,
                Duration.ofMinutes(CompactionLockManager.HEARTBEAT_INTERVAL_MINUTES));
    }

    /**
     * Periodic background virtual thread that calls {@link CompactionLockManager#refresh}
     * on a fixed interval until {@link #stop()} interrupts it — the same
     * sleep-loop-then-interrupt idiom {@code InstanceRegistry} uses for its 30-second
     * heartbeat thread. Package-visible (not {@code private}) so unit tests can drive the
     * mechanism directly with a short interval instead of waiting out a real
     * {@code HEARTBEAT_INTERVAL_MINUTES}.
     */
    static final class LockHeartbeat {
        private final Thread thread;

        LockHeartbeat(CompactionLockManager lockManager, String target, Duration interval) {
            this.thread = Thread.startVirtualThread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Thread.sleep(interval);
                        lockManager.refresh(target);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (SQLException e) {
                        log.warn("Heartbeat refresh failed for compaction lock '{}': {}",
                                target, e.getMessage());
                    }
                }
            });
        }

        /**
         * Interrupts the heartbeat loop and blocks until it has actually exited
         * (bounded by a generous timeout so a stuck refresh call can never hang
         * shutdown forever).
         *
         * <p>Joining rather than firing-and-forgetting closes a race: without it, a
         * heartbeat that woke from sleep and is mid-{@code refresh()} call when
         * {@link Thread#interrupt()} is invoked can complete that one call after
         * {@code stop()} has returned — which could otherwise land after the caller's
         * {@code finally} block has already called {@link CompactionLockManager#release}.
         * Idempotent — safe to call more than once.
         */
        void stop() {
            thread.interrupt();
            try {
                thread.join(Duration.ofSeconds(10));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // =========================================================================
    // Entity compaction
    // =========================================================================

    private CompactionResult compactEntityTypes(List<String> targets) throws SQLException {
        List<String> types = resolveEntityTargets(targets);
        CompactionResult total = CompactionResult.NONE;
        for (String type : types) {
            total = total.add(compactEntityType(type));
        }
        return total;
    }

    private List<String> resolveEntityTargets(List<String> targets) throws SQLException {
        if (targets == null || targets.isEmpty()) {
            return configRepo.listEntityTypes().stream()
                    .map(etc -> etc.entityType())
                    .toList();
        }
        return targets.stream().filter(t -> !t.equals("general")).toList();
    }

    private CompactionResult compactEntityType(String entityType) {
        return doCompactEntityType(entityType);
    }

    /**
     * Merges adjacent small files for one entity type using
     * {@code ducklake_merge_adjacent_files} — DuckLake 1.0.
     *
     * <p>Called once per entity type; DuckLake internally determines which files
     * need merging based on the {@code max_file_size} threshold.  Files already at or
     * above the target size are left untouched.
     *
     * <p>Before the merge call, {@code write_buffer_row_group_memory_limit} is set
     * (DuckDB 1.5.3+) so the Parquet writer flushes row groups based on memory rather
     * than row count — preventing OOM when a single bucket contains millions of rows.
     * The SET is issued inside the same {@code synchronized(duckDB)} block as the merge
     * and is wrapped in its own try/catch: a failure (e.g. DuckDB version &lt; 1.5.3)
     * is logged at DEBUG and never prevents the merge from running.
     *
     * <p>Idempotent — errors are logged at WARN and the caller receives
     * {@link CompactionResult#NONE}, consistent with the non-fatal compaction error policy.
     */
    private CompactionResult doCompactEntityType(String entityType) {
        SchemaManager.validateEntityType(entityType);
        String lockTarget = "entity:" + entityType;
        boolean acquired;
        try {
            acquired = lockManager.tryAcquire(lockTarget);
        } catch (SQLException e) {
            log.warn("Could not acquire compaction lock '{}' ({}); skipping this run", lockTarget, e.getMessage());
            return CompactionResult.NONE;
        }
        if (!acquired) {
            log.info("Compaction lock '{}' held by another instance — skipping (will retry next scheduled run)",
                    lockTarget);
            return CompactionResult.NONE;
        }
        LockHeartbeat heartbeat = startHeartbeat(lockTarget);
        try {
            long maxFileSizeBytes = (long) props.getCompaction().getEntity().getTargetFileSizeMb() * 1024L * 1024L;
            int rowGroupMemoryLimitMb = props.getCompaction().getEntity().getRowGroupMemoryLimitMb();
            String sql = "CALL ducklake_merge_adjacent_files('lake', 'entity_" + entityType + "',"
                       + " max_file_size => " + maxFileSizeBytes + ")";
            log.debug("Merging adjacent files for entity_type='{}'", entityType);
            synchronized (duckDB) {
                // Apply the Parquet row-group memory cap before the merge (DuckDB 1.5.3+).
                // A value of 0 means "use the DuckDB default" — skip the SET entirely.
                if (rowGroupMemoryLimitMb > 0) {
                    try (Statement st = duckDB.createStatement()) {
                        st.execute("SET write_buffer_row_group_memory_limit = '"
                                + rowGroupMemoryLimitMb + "MB'");
                        log.debug("write_buffer_row_group_memory_limit set to {} MB for entity_{} merge",
                                rowGroupMemoryLimitMb, entityType);
                    } catch (SQLException setEx) {
                        // Older DuckDB (< 1.5.3) does not support this setting — safe to continue.
                        log.debug("write_buffer_row_group_memory_limit not applied for entity_{} "
                                + "(DuckDB < 1.5.3?): {}", entityType, setEx.getMessage());
                    }
                }
                try (Statement st = duckDB.createStatement();
                     ResultSet rs = st.executeQuery(sql)) {
                    FileStats stats = FileStats.EMPTY;
                    while (rs.next()) {
                        stats = stats.add(new FileStats(
                                rs.getLong("files_processed"),
                                rs.getLong("files_created")));
                    }
                    log.debug("Merged adjacent files for entity_{}: files_processed={} files_created={}",
                            entityType, stats.filesProcessed(), stats.filesCreated());
                    return new CompactionResult(stats.filesProcessed() > 0 ? 1 : 0, stats);
                }
            }
        } catch (SQLException e) {
            if (DuckDbErrors.isTransient(e)) {
                log.warn("ducklake_merge_adjacent_files transient S3 failure for entity_type='{}' (will retry next run): {}",
                        entityType, e.getMessage());
            } else {
                log.warn("ducklake_merge_adjacent_files failed for entity_type='{}': {}", entityType, e.getMessage());
            }
            return CompactionResult.NONE;
        } finally {
            heartbeat.stop();
            lockManager.release(lockTarget);
        }
    }

    // =========================================================================
    // General cassette compaction
    // =========================================================================

    private CompactionResult compactGeneralIfEnabled(List<String> targets) throws SQLException {
        boolean doGeneral = props.getCompaction().getGeneral().isEnabled()
                && (targets == null || targets.contains("general"));
        return doGeneral ? compactGeneralCassette() : CompactionResult.NONE;
    }

    private CompactionResult compactGeneralCassette() throws SQLException {
        List<String> topics = configRepo.listTopics().stream()
                .filter(tc -> tc.mode() != null && tc.mode().writesGeneral())
                .map(tc -> tc.topic())
                .toList();

        CompactionResult total = CompactionResult.NONE;
        for (String topic : topics) {
            total = total.add(compactGeneralTopic(topic));
        }
        return total;
    }

    private CompactionResult compactGeneralTopic(String topic) {
        return doCompactGeneralTopic(topic);
    }

    /**
     * Merges adjacent small files for one general cassette topic using
     * {@code ducklake_merge_adjacent_files} — DuckLake 1.0.
     *
     * <p>See {@link #doCompactEntityType} for the function signature reference.
     * Idempotent — errors are logged at WARN and do not rethrow.
     */
    private CompactionResult doCompactGeneralTopic(String topic) {
        String tableName = "general_" + normalizeTopicName(topic);
        String lockTarget = "topic:" + tableName;
        boolean acquired;
        try {
            acquired = lockManager.tryAcquire(lockTarget);
        } catch (SQLException e) {
            log.warn("Could not acquire compaction lock '{}' ({}); skipping this run", lockTarget, e.getMessage());
            return CompactionResult.NONE;
        }
        if (!acquired) {
            log.info("Compaction lock '{}' held by another instance — skipping (will retry next scheduled run)",
                    lockTarget);
            return CompactionResult.NONE;
        }
        LockHeartbeat heartbeat = startHeartbeat(lockTarget);
        try {
            long maxFileSizeBytes = (long) props.getCompaction().getGeneral().getTargetFileSizeMb() * 1024L * 1024L;
            String sql = "CALL ducklake_merge_adjacent_files('lake', '" + tableName + "',"
                       + " max_file_size => " + maxFileSizeBytes + ")";
            log.debug("Merging adjacent files for general cassette topic='{}'", topic);
            synchronized (duckDB) {
                try (Statement st = duckDB.createStatement();
                     ResultSet rs = st.executeQuery(sql)) {
                    FileStats stats = FileStats.EMPTY;
                    while (rs.next()) {
                        stats = stats.add(new FileStats(
                                rs.getLong("files_processed"),
                                rs.getLong("files_created")));
                    }
                    log.debug("Merged adjacent files for general cassette topic='{}': "
                                    + "files_processed={} files_created={}",
                            topic, stats.filesProcessed(), stats.filesCreated());
                    return new CompactionResult(stats.filesProcessed() > 0 ? 1 : 0, stats);
                }
            }
        } catch (SQLException e) {
            if (DuckDbErrors.isTransient(e)) {
                log.warn("ducklake_merge_adjacent_files transient S3 failure for general cassette topic='{}' (will retry next run): {}",
                        topic, e.getMessage());
            } else {
                log.warn("ducklake_merge_adjacent_files failed for general cassette topic='{}': {}",
                        topic, e.getMessage());
            }
            return CompactionResult.NONE;
        } finally {
            heartbeat.stop();
            lockManager.release(lockTarget);
        }
    }

    /** Normalises a topic name to {@code [a-z0-9_]}, matching {@code SchemaManager.normalize}. */
    private static String normalizeTopicName(String topic) {
        return topic.toLowerCase().replaceAll("[^a-z0-9_]", "_");
    }

    // =========================================================================
    // DuckDB / DuckLake helpers
    // =========================================================================

    /**
     * Flushes inlined DuckLake data to Parquet on object storage, then
     * issues a DuckDB CHECKPOINT to persist the updated catalog metadata.
     *
     * <p>{@code CALL ducklake_flush_inlined_data('lake')} triggers DuckLake to write all
     * buffered inline rows as Parquet files to the configured DATA_PATH (S3).
     * Since DuckLake 1.0 / PR #734 the call returns result rows indicating how many
     * rows were flushed; the count is captured and logged at DEBUG level.
     * The subsequent CHECKPOINT ensures the catalog SQLite/DuckDB file is
     * durable on local disk as well.
     */
    private void checkpoint() throws SQLException {
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery("CALL ducklake_flush_inlined_data('lake')")) {
                // Log column names once at DEBUG so the schema can be confirmed from logs.
                ResultSetMetaData meta = rs.getMetaData();
                if (log.isDebugEnabled()) {
                    StringBuilder cols = new StringBuilder("ducklake_flush_inlined_data result columns:");
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        cols.append(' ').append(meta.getColumnName(i))
                            .append('(').append(meta.getColumnTypeName(i)).append(')');
                    }
                    log.debug("{}", cols);
                }
                // Pick the first column whose name contains "row", "count", or "flush";
                // fall back to column 1 if none match (schema may evolve).
                int countCol = 1;
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    String name = meta.getColumnName(i).toLowerCase();
                    if (name.contains("row") || name.contains("count") || name.contains("flush")) {
                        countCol = i;
                        break;
                    }
                }
                long totalFlushed = 0;
                while (rs.next()) {
                    totalFlushed += rs.getLong(countCol);
                }
                log.debug("Flushed {} inlined rows to Parquet for lake 'lake'", totalFlushed);
            } catch (SQLException e) {
                log.warn("ducklake_flush failed ({}); data may remain inlined", e.getMessage());
            }
            // ducklake_flush_inlined_data leaves an implicit transaction open with the
            // catalog updates for the flushed files. CHECKPOINT requires no active
            // transaction — commit first so the WAL can be flushed to disk.
            // A skipped commit here causes CHECKPOINT to fail every run, leaving the
            // WAL to grow unbounded in native memory (visible as rising process RSS).
            try {
                duckDB.commit();
            } catch (SQLException e) {
                log.debug("commit before CHECKPOINT: {} (may be auto-commit mode)", e.getMessage());
            }
            try (Statement st = duckDB.createStatement()) {
                st.execute("CHECKPOINT");
                log.debug("CHECKPOINT complete — WAL flushed to disk");
            } catch (SQLException e) {
                // In-memory DuckDB (e.g. in tests) does not support CHECKPOINT — safe to ignore.
                log.warn("CHECKPOINT failed ({}); catalog metadata may not be persisted", e.getMessage());
            }
        }
    }

    /**
     * Expires snapshots older than {@code snapshot-retention-hours} and deletes the
     * now-unreferenced files those snapshots were the last reference to. Runs once per
     * compaction run (see caller), after every {@code ducklake_merge_adjacent_files}
     * call — merging alone never deletes the files it replaces; DuckLake keeps them for
     * time travel until their snapshot is explicitly expired.
     *
     * <p>Failure here follows the same "log and continue" pattern as
     * {@code ducklake_merge_adjacent_files} failures elsewhere in this class: a missed
     * cleanup this run just means the space is reclaimed on the next successful run
     * instead of this one.
     */
    private void expireSnapshotsAndCleanup() {
        int retentionHours = props.getCompaction().getSnapshotRetentionHours();
        try {
            synchronized (duckDB) {
                try (Statement st = duckDB.createStatement()) {
                    log.debug("Expiring snapshots older than {}h and cleaning up old files", retentionHours);
                    st.execute("CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL '"
                            + retentionHours + "' HOUR)");
                    st.execute("CALL ducklake_cleanup_old_files('lake', cleanup_all => true)");
                }
            }
        } catch (SQLException e) {
            if (DuckDbErrors.isTransient(e)) {
                log.warn("Transient S3 failure during snapshot expiry/cleanup (will retry next run): {}",
                        e.getMessage());
            } else {
                log.warn("Snapshot expiry/cleanup failed: {}", e.getMessage());
            }
        }
    }

    // =========================================================================
    // compaction_history CRUD
    // =========================================================================

    private long insertRunRecord(TriggerSource triggeredBy, List<String> targets) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    INSERT INTO compaction_history
                        (started_at, status, triggered_by, targets,
                         entity_buckets_compacted, general_partitions_compacted,
                         files_processed, files_created)
                    VALUES (?, 'running', ?, ?, 0, 0, 0, 0)
                    RETURNING id
                    """)) {
                ps.setTimestamp(1, Timestamp.from(Instant.now()));
                ps.setString(2, triggeredBy.getValue());
                if (targets == null) {
                    ps.setObject(3, null);
                } else {
                    ps.setArray(3, duckDB.createArrayOf("VARCHAR", targets.toArray(new String[0])));
                }
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) return rs.getLong(1);
                    throw new SQLException("INSERT into compaction_history returned no generated id");
                }
            }
        }
    }

    private void updateRunRecord(long runId, RunStatus status,
                                  int entityTypes, int generalTopics,
                                  FileStats fileStats, String errorMessage) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    UPDATE compaction_history
                    SET completed_at = ?,
                        status = ?,
                        entity_buckets_compacted = ?,
                        general_partitions_compacted = ?,
                        files_processed = ?,
                        files_created = ?,
                        error_message = ?
                    WHERE id = ?
                    """)) {
                ps.setTimestamp(1, Timestamp.from(Instant.now()));
                ps.setString(2, status.getValue());
                ps.setInt(3, entityTypes);
                ps.setInt(4, generalTopics);
                ps.setLong(5, fileStats.filesProcessed());
                ps.setLong(6, fileStats.filesCreated());
                ps.setString(7, errorMessage);
                ps.setLong(8, runId);
                ps.executeUpdate();
            }
        }
    }

    private CompactionRun queryLastRun() throws SQLException {
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery("""
                    SELECT id, started_at, completed_at, status, triggered_by, targets,
                           entity_buckets_compacted, general_partitions_compacted,
                           files_processed, files_created, error_message
                    FROM compaction_history
                    ORDER BY started_at DESC LIMIT 1
                    """)) {
                return rs.next() ? mapRun(rs) : null;
            }
        }
    }

    private static CompactionRun mapRun(ResultSet rs) throws SQLException {
        Timestamp completedTs = rs.getTimestamp("completed_at");

        java.sql.Array targetsArr = rs.getArray("targets");
        List<String> targets = null;
        if (targetsArr != null) {
            Object[] vals = (Object[]) targetsArr.getArray();
            targets = Arrays.stream(vals).map(Object::toString).toList();
        }

        return new CompactionRun(
                rs.getLong("id"),
                rs.getTimestamp("started_at").toInstant(),
                completedTs != null ? completedTs.toInstant() : null,
                RunStatus.fromValue(rs.getString("status")),
                TriggerSource.fromValue(rs.getString("triggered_by")),
                targets,
                rs.getInt("entity_buckets_compacted"),
                rs.getInt("general_partitions_compacted"),
                rs.getLong("files_processed"),
                rs.getLong("files_created"),
                rs.getString("error_message")
        );
    }

    // =========================================================================
    // Scheduling helpers
    // =========================================================================

    private Instant computeNextScheduledRun() {
        try {
            CronExpression expr = CronExpression.parse(props.getCompaction().getSchedule());
            LocalDateTime next = expr.next(LocalDateTime.now());
            if (next == null) return null;
            return next.atZone(ZoneId.systemDefault()).toInstant();
        } catch (Exception e) {
            log.warn("Cannot parse cron '{}' to compute next run: {}",
                    props.getCompaction().getSchedule(), e.getMessage());
            return null;
        }
    }

    // =========================================================================
    // Internal types
    // =========================================================================

    /**
     * Aggregated stats from one or more {@code ducklake_merge_adjacent_files} calls.
     * Column names match the function's result table:
     * {@code files_processed BIGINT} (input files merged), {@code files_created BIGINT}
     * (output files written).
     */
    private record FileStats(long filesProcessed, long filesCreated) {
        static final FileStats EMPTY = new FileStats(0, 0);

        FileStats add(FileStats other) {
            return new FileStats(
                    filesProcessed + other.filesProcessed,
                    filesCreated + other.filesCreated);
        }
    }

    /** Aggregated result of compacting a set of entity types or cassette topics. */
    private record CompactionResult(int unitsProcessed, FileStats fileStats) {
        static final CompactionResult NONE = new CompactionResult(0, FileStats.EMPTY);

        CompactionResult add(CompactionResult other) {
            return new CompactionResult(
                    unitsProcessed + other.unitsProcessed,
                    fileStats.add(other.fileStats));
        }
    }
}
