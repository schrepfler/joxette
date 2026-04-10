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
 * Compacts entity and general cassette tables in DuckLake by re-writing small
 * row groups per bucket/partition into fewer, larger, sort-ordered files.
 *
 * <h2>Entity compaction strategy</h2>
 * <ol>
 *   <li>For each configured entity type (or the requested subset), count cold
 *       rows (older than {@code lookback-days}) per bucket.</li>
 *   <li>Estimate the number of row groups for each bucket by distributing the
 *       table-wide row-group count proportionally.  Buckets whose estimate
 *       exceeds {@code min-files-per-bucket} are selected for compaction.</li>
 *   <li>For each selected bucket: copy cold rows into a temp table sorted by
 *       {@code (entity_id, timestamp, recorded_at)}, delete the originals, and
 *       re-insert from the temp table.  DuckLake writes the re-inserted rows as
 *       new, larger data files.</li>
 * </ol>
 *
 * <h2>General cassette compaction</h2>
 * <p>Disabled by default ({@code joxette.compaction.general.enabled=false}).
 * When enabled, applies the same strategy per {@code (topic, partition)} pair,
 * with rows sorted by {@code (timestamp, partition, offset)}.
 *
 * <h2>Hot-data protection</h2>
 * <p>Only rows with {@code recorded_at < now() - lookback_days} are touched;
 * recently-written ("hot") data is left in place.
 *
 * <h2>Run tracking</h2>
 * <p>Every run is recorded in {@code lake.compaction_history}.  An
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
     * in {@code lake.compaction_history}.
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
        int entityBuckets = 0;
        int generalPartitions = 0;
        try {
            entityBuckets    = compactEntityTypes(targets);
            generalPartitions = compactGeneralIfEnabled(targets);
            checkpoint();
            updateRunRecord(runId, "completed", entityBuckets, generalPartitions, null);
            log.info("Compaction run {} completed: {} entity buckets, {} general partitions",
                    runId, entityBuckets, generalPartitions);
        } catch (Exception e) {
            log.error("Compaction run {} failed", runId, e);
            try {
                updateRunRecord(runId, "failed", entityBuckets, generalPartitions, e.getMessage());
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
                           entity_buckets_compacted, general_partitions_compacted, error_message
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
                           entity_buckets_compacted, general_partitions_compacted, error_message
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

    private int compactEntityTypes(List<String> targets) throws SQLException {
        List<String> types = resolveEntityTargets(targets);
        int total = 0;
        for (String type : types) {
            total += compactEntityType(type);
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

    private int compactEntityType(String entityType) throws SQLException {
        SchemaManager.validateEntityType(entityType);
        var opt = configRepo.findEntityType(entityType);
        if (opt.isEmpty()) {
            log.warn("Entity type '{}' not found in config, skipping", entityType);
            return 0;
        }
        int lookbackDays = props.getCompaction().getEntity().getLookbackDays();
        int minFiles     = props.getCompaction().getEntity().getMinFilesPerBucket();

        List<Integer> qualifying = findEntityBucketsNeedingCompaction(entityType, minFiles, lookbackDays);
        log.debug("Entity type '{}': {}/{} buckets qualify for compaction",
                entityType, qualifying.size(), opt.get().buckets());

        for (int bucket : qualifying) {
            compactEntityBucket(entityType, bucket, lookbackDays);
        }
        return qualifying.size();
    }

    /**
     * Identifies entity buckets with enough cold row groups to warrant compaction.
     *
     * <p>Row groups in DuckDB are the closest proxy for Parquet data files.  The
     * table-wide row-group count (from {@code duckdb_storage_info()}) is
     * distributed proportionally to each bucket using its share of cold rows.
     * Buckets whose estimated group count meets the {@code minFiles} threshold
     * are returned.
     */
    private List<Integer> findEntityBucketsNeedingCompaction(
            String entityType, int minFiles, int lookbackDays) throws SQLException {

        long totalRowGroups = countTableRowGroups("entity_" + entityType);
        if (totalRowGroups == 0) return List.of();

        String coldSql = "SELECT bucket, COUNT(*) AS cnt"
                + " FROM lake.main.entity_" + entityType
                + " WHERE recorded_at < CURRENT_TIMESTAMP - INTERVAL '" + lookbackDays + " days'"
                + " GROUP BY bucket ORDER BY bucket";

        Map<Integer, Long> bucketCounts = new LinkedHashMap<>();
        long totalColdRows = 0;
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery(coldSql)) {
                while (rs.next()) {
                    long cnt = rs.getLong("cnt");
                    bucketCounts.put(rs.getInt("bucket"), cnt);
                    totalColdRows += cnt;
                }
            }
        }
        if (totalColdRows == 0) return List.of();

        List<Integer> result = new ArrayList<>();
        for (Map.Entry<Integer, Long> entry : bucketCounts.entrySet()) {
            long estimated = Math.round((double) entry.getValue() / totalColdRows * totalRowGroups);
            if (estimated >= minFiles) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    /**
     * Re-writes cold data for a single entity bucket, sorted by
     * {@code (entity_id, timestamp, recorded_at)}.
     *
     * <p>Algorithm:
     * <ol>
     *   <li>Create a temp table with the sorted cold rows for this bucket.</li>
     *   <li>Delete the cold rows from the live table.</li>
     *   <li>Re-insert from the temp table — DuckLake writes new, merged files.</li>
     *   <li>Drop the temp table.</li>
     * </ol>
     */
    private void compactEntityBucket(String entityType, int bucket, int lookbackDays) throws SQLException {
        String src     = "lake.main.entity_" + entityType;
        String tmp     = "lake.main._cmp_" + entityType + "_b" + bucket;
        String cutoff  = "CURRENT_TIMESTAMP - INTERVAL '" + lookbackDays + " days'";
        log.debug("Compacting entity_type='{}' bucket={}", entityType, bucket);

        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement()) {
                // 1. Empty temp table matching the source schema
                st.execute("CREATE OR REPLACE TABLE " + tmp + " AS SELECT * FROM " + src + " LIMIT 0");

                // 2. Insert cold, sorted rows into temp
                try (PreparedStatement ps = duckDB.prepareStatement(
                        "INSERT INTO " + tmp
                        + " SELECT * FROM " + src
                        + " WHERE bucket = ? AND recorded_at < " + cutoff
                        + " ORDER BY entity_id, kafka_timestamp, recorded_at")) {
                    ps.setInt(1, bucket);
                    ps.executeUpdate();
                }

                // 3. Delete cold rows from source
                try (PreparedStatement ps = duckDB.prepareStatement(
                        "DELETE FROM " + src
                        + " WHERE bucket = ? AND recorded_at < " + cutoff)) {
                    ps.setInt(1, bucket);
                    ps.executeUpdate();
                }

                // 4. Re-insert sorted data; DuckLake writes consolidated files
                st.execute("INSERT INTO " + src + " SELECT * FROM " + tmp);

                // 5. Drop temp
                st.execute("DROP TABLE IF EXISTS " + tmp);
            }
        }
    }

    // =========================================================================
    // General cassette compaction
    // =========================================================================

    private int compactGeneralIfEnabled(List<String> targets) throws SQLException {
        boolean doGeneral = props.getCompaction().getGeneral().isEnabled()
                && (targets == null || targets.contains("general"));
        return doGeneral ? compactGeneralCassette() : 0;
    }

    private int compactGeneralCassette() throws SQLException {
        int lookbackDays = props.getCompaction().getGeneral().getLookbackDays();
        int minFiles     = props.getCompaction().getGeneral().getMinFilesPerPartition();

        // Collect all configured topics that have general cassette tables
        List<String> topics = configRepo.listTopics().stream()
                .filter(tc -> "general".equals(tc.mode()) || "both".equals(tc.mode()))
                .map(tc -> tc.topic())
                .toList();

        int compacted = 0;
        for (String topic : topics) {
            String tableName = "general_" + normalizeTopicName(topic);
            long totalRowGroups = countTableRowGroups(tableName);
            if (totalRowGroups == 0) continue;

            String coldSql = "SELECT kafka_partition AS partition, COUNT(*) AS cnt"
                    + " FROM lake.main." + tableName
                    + " WHERE recorded_at < CURRENT_TIMESTAMP - INTERVAL '" + lookbackDays + " days'"
                    + " GROUP BY kafka_partition"
                    + " ORDER BY kafka_partition";

            Map<Integer, Long> partitionCounts = new LinkedHashMap<>();
            long totalColdRows = 0;
            synchronized (duckDB) {
                try (Statement st = duckDB.createStatement();
                     ResultSet rs = st.executeQuery(coldSql)) {
                    while (rs.next()) {
                        long cnt = rs.getLong("cnt");
                        partitionCounts.put(rs.getInt("partition"), cnt);
                        totalColdRows += cnt;
                    }
                }
            }
            if (totalColdRows == 0) continue;

            for (Map.Entry<Integer, Long> entry : partitionCounts.entrySet()) {
                long estimated = Math.round((double) entry.getValue() / totalColdRows * totalRowGroups);
                if (estimated >= minFiles) {
                    compactGeneralPartition(topic, tableName, entry.getKey(), lookbackDays);
                    compacted++;
                }
            }
        }
        return compacted;
    }

    /**
     * Re-writes cold data for one {@code (kafka_partition)} slice of a
     * general cassette table, sorted by {@code (kafka_timestamp, kafka_partition, kafka_offset)}.
     */
    private void compactGeneralPartition(String topic, String tableName, int partition, int lookbackDays)
            throws SQLException {
        String src    = "lake.main." + tableName;
        String hex    = Integer.toHexString(Math.abs((topic + partition).hashCode()));
        String tmp    = "lake.main._cmp_general_" + hex;
        String cutoff = "CURRENT_TIMESTAMP - INTERVAL '" + lookbackDays + " days'";
        log.debug("Compacting general cassette topic='{}' partition={}", topic, partition);

        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement()) {
                st.execute("CREATE OR REPLACE TABLE " + tmp + " AS SELECT * FROM " + src + " LIMIT 0");

                try (PreparedStatement ps = duckDB.prepareStatement(
                        "INSERT INTO " + tmp
                        + " SELECT * FROM " + src
                        + " WHERE kafka_partition = ? AND recorded_at < " + cutoff
                        + " ORDER BY kafka_timestamp, kafka_partition, kafka_offset")) {
                    ps.setInt(1, partition);
                    ps.executeUpdate();
                }

                try (PreparedStatement ps = duckDB.prepareStatement(
                        "DELETE FROM " + src
                        + " WHERE kafka_partition = ? AND recorded_at < " + cutoff)) {
                    ps.setInt(1, partition);
                    ps.executeUpdate();
                }

                st.execute("INSERT INTO " + src + " SELECT * FROM " + tmp);
                st.execute("DROP TABLE IF EXISTS " + tmp);
            }
        }
    }

    /** Normalises a topic name to {@code [a-z0-9_]}, matching {@code SchemaManager.normalize}. */
    private static String normalizeTopicName(String topic) {
        return topic.toLowerCase().replaceAll("[^a-z0-9_]", "_");
    }

    // =========================================================================
    // DuckDB storage helpers
    // =========================================================================

    /**
     * Returns the number of distinct row groups for a table in the {@code lake}
     * schema using {@code pragma_storage_info()}.
     *
     * <p>Row groups are the closest DuckDB proxy for DuckLake Parquet data files.
     * The count is used to estimate per-bucket / per-partition file density.
     */
    private long countTableRowGroups(String tableName) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    SELECT COUNT(DISTINCT row_group_id) AS rg_count
                    FROM pragma_storage_info('lake.main.' || ?)
                    """)) {
                ps.setString(1, tableName);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getLong("rg_count") : 0;
                }
            }
        }
    }

    /**
     * Flushes inlined DuckLake data to Parquet on object storage, then
     * issues a DuckDB CHECKPOINT to persist the updated catalog metadata.
     *
     * <p>{@code CALL ducklake_flush_inlined_data('lake')} triggers DuckLake to write all
     * buffered inline rows as Parquet files to the configured DATA_PATH (S3).
     * The subsequent CHECKPOINT ensures the catalog SQLite/DuckDB file is
     * durable on local disk as well.
     */
    private void checkpoint() throws SQLException {
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement()) {
                // Flush inlined rows → Parquet on S3 (the key step for DuckLake)
                st.execute("CALL ducklake_flush_inlined_data('lake')");
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
                         entity_buckets_compacted, general_partitions_compacted)
                    VALUES (?, 'running', ?, ?, 0, 0)
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
                                  int entityBuckets, int generalPartitions,
                                  String errorMessage) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    UPDATE compaction_history
                    SET completed_at = ?,
                        status = ?,
                        entity_buckets_compacted = ?,
                        general_partitions_compacted = ?,
                        error_message = ?
                    WHERE id = ?
                    """)) {
                ps.setTimestamp(1, Timestamp.from(Instant.now()));
                ps.setString(2, status);
                ps.setInt(3, entityBuckets);
                ps.setInt(4, generalPartitions);
                ps.setString(5, errorMessage);
                ps.setLong(6, runId);
                ps.executeUpdate();
            }
        }
    }

    private CompactionRun queryLastRun() throws SQLException {
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery("""
                    SELECT id, started_at, completed_at, status, triggered_by, targets,
                           entity_buckets_compacted, general_partitions_compacted, error_message
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

    private record TopicPartitionKey(String topic, int partition) {}
}
