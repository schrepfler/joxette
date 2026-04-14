package com.joxette.compaction;

import com.joxette.config.JoxetteProperties;
import com.joxette.management.ConfigRepository;
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
 * {@link AtomicBoolean} guard prevents overlapping runs.
 */
@Service
@DependsOn("dbSchemaManager")
public class CompactionService {

    private static final Logger log = LoggerFactory.getLogger(CompactionService.class);

    private final Connection duckDB;
    private final JoxetteProperties props;
    private final ConfigRepository configRepo;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public CompactionService(Connection duckDB, JoxetteProperties props, ConfigRepository configRepo) {
        this.duckDB    = duckDB;
        this.props     = props;
        this.configRepo = configRepo;
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
     * @throws IllegalStateException if a compaction is already in progress
     */
    public CompactionRun beginRun(String triggeredBy, List<String> targets) throws SQLException {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("Compaction already in progress");
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
        try {
            CompactionResult entityResult = compactEntityTypes(targets);
            entityTypes    = entityResult.unitsProcessed();
            totalFileStats = totalFileStats.add(entityResult.fileStats());

            CompactionResult generalResult = compactGeneralIfEnabled(targets);
            generalTopics  = generalResult.unitsProcessed();
            totalFileStats = totalFileStats.add(generalResult.fileStats());

            checkpoint();
            updateRunRecord(runId, "completed", entityTypes, generalTopics, totalFileStats, null);
            log.info("Compaction run {} completed: {} entity types, {} general topics, "
                            + "files_processed={} files_created={}",
                    runId, entityTypes, generalTopics,
                    totalFileStats.filesProcessed(), totalFileStats.filesCreated());
        } catch (Exception e) {
            log.error("Compaction run {} failed", runId, e);
            try {
                updateRunRecord(runId, "failed", entityTypes, generalTopics, totalFileStats, e.getMessage());
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
            runId = insertRunRecord("scheduled", null);
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
                    throw new NoSuchElementException("No compaction run with id " + id);
                }
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

    /**
     * Merges adjacent small files for one entity type using
     * {@code ducklake_merge_adjacent_files} — DuckLake 1.0.
     *
     * <p>Called once per entity type; DuckLake internally determines which files
     * need merging based on the {@code max_file_size} threshold.  Files already at or
     * above the target size are left untouched.
     *
     * <p>Idempotent — errors are logged at WARN and the caller receives
     * {@link CompactionResult#NONE}, consistent with the non-fatal compaction error policy.
     */
    private CompactionResult compactEntityType(String entityType) {
        SchemaManager.validateEntityType(entityType);
        long maxFileSizeBytes = (long) props.getCompaction().getEntity().getTargetFileSizeMb() * 1024L * 1024L;
        String sql = "CALL ducklake_merge_adjacent_files('lake', 'entity_" + entityType + "',"
                   + " max_file_size => " + maxFileSizeBytes + ")";
        log.debug("Merging adjacent files for entity_type='{}'", entityType);
        try {
            synchronized (duckDB) {
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
            log.warn("ducklake_merge_adjacent_files failed for entity_type='{}': {}", entityType, e.getMessage());
            return CompactionResult.NONE;
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
                .filter(tc -> "general".equals(tc.mode()) || "both".equals(tc.mode()))
                .map(tc -> tc.topic())
                .toList();

        CompactionResult total = CompactionResult.NONE;
        for (String topic : topics) {
            total = total.add(compactGeneralTopic(topic));
        }
        return total;
    }

    /**
     * Merges adjacent small files for one general cassette topic using
     * {@code ducklake_merge_adjacent_files} — DuckLake 1.0.
     *
     * <p>See {@link #compactEntityType} for the function signature reference.
     * Idempotent — errors are logged at WARN and do not rethrow.
     */
    private CompactionResult compactGeneralTopic(String topic) {
        String tableName = "general_" + normalizeTopicName(topic);
        long maxFileSizeBytes = (long) props.getCompaction().getGeneral().getTargetFileSizeMb() * 1024L * 1024L;
        String sql = "CALL ducklake_merge_adjacent_files('lake', '" + tableName + "',"
                   + " max_file_size => " + maxFileSizeBytes + ")";
        log.debug("Merging adjacent files for general cassette topic='{}'", topic);
        try {
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
            log.warn("ducklake_merge_adjacent_files failed for general cassette topic='{}': {}",
                    topic, e.getMessage());
            return CompactionResult.NONE;
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
            try (Statement st = duckDB.createStatement()) {
                // Persist catalog metadata to local disk
                st.execute("CHECKPOINT");
            } catch (SQLException e) {
                // In-memory DuckDB (e.g. in tests) does not support CHECKPOINT — safe to ignore.
                log.warn("CHECKPOINT failed ({}); catalog metadata may not be persisted", e.getMessage());
            }
        }
    }

    // =========================================================================
    // compaction_history CRUD
    // =========================================================================

    private long insertRunRecord(String triggeredBy, List<String> targets) throws SQLException {
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
                ps.setString(2, triggeredBy);
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

    private void updateRunRecord(long runId, String status,
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
                ps.setString(2, status);
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
                rs.getString("status"),
                rs.getString("triggered_by"),
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
