package com.joxette.replay;

import com.joxette.config.JoxetteProperties;
import com.joxette.management.ConfigRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.joxette.replay.TopicReplayService.normalizeTopicName;

/**
 * Cassette lifecycle operations: statistics, compaction, truncation,
 * GDPR entity deletion, and snapshot management.
 *
 * <h2>Snapshot implementation</h2>
 * <p>Snapshots use DuckDB's {@code EXPORT DATABASE} / {@code IMPORT DATABASE}
 * commands, which serialise all tables to Parquet files plus a {@code schema.sql}
 * inside a named directory under {@code <catalog-parent>/snapshots/}.  Snapshot
 * metadata is tracked in {@code lake.snapshots}.
 *
 * <p><strong>Restore note:</strong> {@code IMPORT DATABASE} re-creates all
 * tables from the Parquet export.  Existing tables with the same names are
 * replaced.  The operation is performed while the DuckDB connection is held,
 * so no other in-flight queries should be running concurrently; callers should
 * stop recording before restoring.
 */
@Service
public class CassetteLifecycleService {

    private static final Logger log = LoggerFactory.getLogger(CassetteLifecycleService.class);

    /** Allowed characters in a snapshot name — no slashes, no SQL meta-characters. */
    private static final Pattern SNAPSHOT_NAME = Pattern.compile("[a-zA-Z0-9_-]+");

    private final Connection duckDB;
    private final Path snapshotsBase;
    /** Object-storage root, e.g. {@code s3://joxette-data/} — used for DuckLake DATA_PATH. */
    private final String objectStoragePath;
    private final ConfigRepository configRepo;
    private final JoxetteProperties properties;
    private final S3Client s3Client;

    public CassetteLifecycleService(Connection duckDB, JoxetteProperties properties,
                                    ConfigRepository configRepo, Optional<S3Client> s3Client) {
        this.duckDB            = duckDB;
        this.configRepo        = configRepo;
        this.properties        = properties;
        this.s3Client          = s3Client.orElse(null);
        this.objectStoragePath = properties.getCatalog().getObjectStoragePath();
        Path catalogPath = Path.of(properties.getCatalog().getPath());
        Path parent = catalogPath.getParent();
        this.snapshotsBase = (parent != null ? parent : Path.of(".")).resolve("snapshots");
    }

    // -------------------------------------------------------------------------
    // Statistics
    // -------------------------------------------------------------------------

    /**
     * Returns row count and estimated table size for the given topic's general
     * cassette table ({@code lake.main.general_{topic}}).
     */
    public CassetteStats getTopicCassetteStats(String topic) throws SQLException {
        String tableName = "general_" + normalizeTopicName(topic);
        String qualifiedTable = "lake.main." + tableName;
        long rowCount;
        long estimatedSize;
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + qualifiedTable)) {
                rowCount = rs.next() ? rs.getLong(1) : 0;
            }
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "SELECT COALESCE(estimated_size, 0) AS sz " +
                    "FROM duckdb_tables() WHERE database_name='lake' AND schema_name='main' AND table_name=?")) {
                ps.setString(1, tableName);
                try (ResultSet rs = ps.executeQuery()) {
                    estimatedSize = rs.next() ? rs.getLong("sz") : 0;
                }
            }
        }
        return new CassetteStats(topic, qualifiedTable, rowCount, estimatedSize);
    }

    /**
     * Returns per-bucket row counts and proportional size estimates for the given
     * entity type's cassette table ({@code lake.main.entity_{entityType}}).
     *
     * <p>Row counts are exact; per-bucket size estimates are proportionally
     * distributed from the DuckDB-reported table estimate and are approximate.
     */
    public EntityStorageStats getEntityTypeStorageStats(String entityType) throws SQLException {
        com.joxette.db.SchemaManager.validateEntityType(entityType);
        String tableName = "entity_" + entityType;
        String qualifiedTable = "lake.main." + tableName;

        Map<Integer, Long> bucketRows = new LinkedHashMap<>();
        long estimatedSize;

        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT bucket, COUNT(*) AS row_count FROM " + qualifiedTable +
                         " GROUP BY bucket ORDER BY bucket")) {
                while (rs.next()) {
                    bucketRows.put(rs.getInt("bucket"), rs.getLong("row_count"));
                }
            }
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "SELECT COALESCE(estimated_size, 0) AS sz " +
                    "FROM duckdb_tables() WHERE database_name='lake' AND schema_name='main' AND table_name=?")) {
                ps.setString(1, tableName);
                try (ResultSet rs = ps.executeQuery()) {
                    estimatedSize = rs.next() ? rs.getLong("sz") : 0;
                }
            }
        }

        long totalRows = bucketRows.values().stream().mapToLong(Long::longValue).sum();
        final long total = totalRows;
        final long size  = estimatedSize;

        List<EntityStorageStats.BucketStats> buckets = bucketRows.entrySet().stream()
                .map(e -> new EntityStorageStats.BucketStats(
                        e.getKey(),
                        e.getValue(),
                        total > 0 ? (size * e.getValue() / total) : 0L))
                .toList();

        return new EntityStorageStats(entityType, qualifiedTable, totalRows, estimatedSize, buckets);
    }

    // -------------------------------------------------------------------------
    // Compaction
    // -------------------------------------------------------------------------

    /**
     * Flushes inlined DuckLake data to Parquet on object storage, then
     * issues a DuckDB CHECKPOINT to persist catalog metadata to disk.
     *
     * <p>{@code CALL ducklake_flush_inlined_data('lake')} writes all buffered inline rows
     * as Parquet files to the configured DATA_PATH (S3/RustFS).  Without this
     * call, data stays inlined in the catalog SQLite file and never appears
     * as objects in the bucket.
     *
     * @param topic present for API symmetry; not used in the flush command
     */
    public void compactTopicCassette(@SuppressWarnings("unused") String topic) throws SQLException {
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement()) {
                st.execute("CALL ducklake_flush_inlined_data('lake')");
            } catch (SQLException e) {
                // Log but don't abort — fall through to CHECKPOINT
                log.warn("ducklake_flush failed ({}); data may remain inlined", e.getMessage());
            }
            try (Statement st = duckDB.createStatement()) {
                st.execute("CHECKPOINT");
            }
        }
    }

    // -------------------------------------------------------------------------
    // Truncation
    // -------------------------------------------------------------------------

    /**
     * Deletes all rows for {@code topic} from its general cassette table
     * ({@code lake.main.general_{topic}}).
     *
     * @return number of rows deleted
     */
    public long truncateTopicCassette(String topic) throws SQLException {
        String qualifiedTable = "lake.main.general_" + normalizeTopicName(topic);
        log.info("Truncating general cassette for topic '{}'", topic);
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement()) {
                // DELETE FROM with no WHERE clause truncates the entire table.
                long deleted = st.executeUpdate("DELETE FROM " + qualifiedTable);
                log.info("Truncated general cassette for topic '{}': {} rows deleted", topic, deleted);
                return deleted;
            }
        }
    }

    /**
     * Deletes all rows from the entity cassette table
     * ({@code lake.main.entity_{entityType}}).
     *
     * @return number of rows deleted
     */
    public long truncateEntityCassette(String entityType) throws SQLException {
        com.joxette.db.SchemaManager.validateEntityType(entityType);
        String qualifiedTable = "lake.main.entity_" + entityType;
        log.info("Truncating entity cassette for type '{}'", entityType);
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement()) {
                long deleted = st.executeUpdate("DELETE FROM " + qualifiedTable);
                log.info("Truncated entity cassette for type '{}': {} rows deleted", entityType, deleted);
                return deleted;
            }
        }
    }

    // -------------------------------------------------------------------------
    // GDPR entity deletion
    // -------------------------------------------------------------------------

    /**
     * Removes all occurrences of an entity from its type cassette and from the
     * known-entities registry (right-to-erasure / GDPR deletion).
     *
     * @return total number of rows deleted across both tables
     */
    public long deleteEntityFromCassette(String entityType, String entityId) throws SQLException {
        com.joxette.db.SchemaManager.validateEntityType(entityType);
        log.info("GDPR delete: removing entity type='{}' id='{}' from cassette and known_entities",
                entityType, entityId);
        long deleted = 0;
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM lake.main.entity_" + entityType + " WHERE entity_id = ?")) {
                ps.setString(1, entityId);
                deleted += ps.executeUpdate();
            }
            // known_entities is plain DuckDB (main schema), not DuckLake
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM known_entities WHERE entity_type = ? AND entity_id = ?")) {
                ps.setString(1, entityType);
                ps.setString(2, entityId);
                deleted += ps.executeUpdate();
            }
        }
        log.info("GDPR delete complete: type='{}' id='{}' — {} total rows removed", entityType, entityId, deleted);
        return deleted;
    }

    // -------------------------------------------------------------------------
    // Snapshots
    // -------------------------------------------------------------------------

    /**
     * Creates a named DuckDB {@code EXPORT DATABASE} snapshot and records its
     * metadata in {@code lake.snapshots}.
     *
     * @param name must match {@code [a-zA-Z0-9_-]+}
     */
    public SnapshotInfo createSnapshot(String name) throws SQLException {
        validateSnapshotName(name);
        Path snapshotDir = snapshotsBase.resolve(name);
        if (Files.exists(snapshotDir)) {
            throw com.joxette.api.error.ConflictException.snapshotAlreadyExists(name);
        }
        try {
            Files.createDirectories(snapshotsBase);
        } catch (IOException e) {
            throw com.joxette.api.error.UpstreamUnavailableException.objectStore(
                    "Cannot create snapshots directory: " + snapshotsBase, e);
        }
        log.info("Creating snapshot '{}' at {}", name, snapshotDir);
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement()) {
                // Path is validated to contain only [a-zA-Z0-9_/-] — no SQL injection risk.
                st.execute("EXPORT DATABASE '" + snapshotDir + "'");
            }
            long sizeBytes = directorySize(snapshotDir);
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    INSERT INTO snapshots (name, created_at, size_bytes)
                    VALUES (?, now(), ?)
                    ON CONFLICT (name) DO UPDATE SET created_at = now(), size_bytes = excluded.size_bytes
                    """)) {
                ps.setString(1, name);
                ps.setLong(2, sizeBytes);
                ps.executeUpdate();
            }
            log.info("Snapshot '{}' created: {} bytes", name, sizeBytes);
            return new SnapshotInfo(name, Instant.now(), sizeBytes);
        }
    }

    /** Returns all known snapshots, newest first. */
    public List<SnapshotInfo> listSnapshots() throws SQLException {
        List<SnapshotInfo> result = new ArrayList<>();
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT name, created_at, size_bytes FROM snapshots ORDER BY created_at DESC")) {
                while (rs.next()) {
                    Instant createdAt = rs.getTimestamp("created_at").toInstant();
                    result.add(new SnapshotInfo(rs.getString("name"), createdAt, rs.getLong("size_bytes")));
                }
            }
        }
        return result;
    }

    /**
     * Restores the database from a named snapshot via {@code IMPORT DATABASE}.
     * All existing tables in the export are replaced.
     *
     * <p><strong>Warning:</strong> stop all active recorders before calling
     * this method to avoid concurrent writes during restore.
     */
    public void restoreSnapshot(String name) throws SQLException {
        validateSnapshotName(name);
        Path snapshotDir = snapshotsBase.resolve(name);
        if (!Files.exists(snapshotDir)) {
            throw com.joxette.api.error.ResourceNotFoundException.snapshot(name);
        }
        log.warn("Restoring snapshot '{}' from {} — all existing tables will be replaced", name, snapshotDir);
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement()) {
                st.execute("IMPORT DATABASE '" + snapshotDir + "'");
            }
        }
        log.info("Snapshot '{}' restored", name);
    }

    /**
     * Creates a local {@code EXPORT DATABASE} snapshot, uploads all its files to an
     * S3-compatible object store, records the snapshot in {@code lake.snapshots}, and
     * then removes the local directory (it's now safely in object storage).
     *
     * <p>Requires {@code joxette.object-store.bucket} to be configured; throws
     * {@link IllegalStateException} otherwise.
     *
     * @param name snapshot name — must match {@code [a-zA-Z0-9_-]+}
     * @return metadata including the S3 base URI
     */
    public ObjectStoreSnapshotInfo exportSnapshotToObjectStore(String name) throws SQLException {
        if (s3Client == null) {
            throw com.joxette.api.error.UpstreamUnavailableException.objectStoreNotConfigured();
        }
        // Create the local snapshot first (validates name, runs EXPORT DATABASE, inserts metadata row).
        SnapshotInfo snapshot = createSnapshot(name);
        Path snapshotDir = snapshotsBase.resolve(name);

        var os = properties.getObjectStore();
        String keyPrefix = (os.getPrefix() != null && !os.getPrefix().isBlank()
                ? os.getPrefix() + "/" : "") + name + "/";

        // Collect all files before streaming to avoid interleaving with directory walk.
        List<Path> files;
        try (Stream<Path> walk = Files.walk(snapshotDir)) {
            files = walk.filter(Files::isRegularFile).toList();
        } catch (IOException e) {
            throw com.joxette.api.error.UpstreamUnavailableException.objectStore(
                    "Cannot read snapshot directory: " + snapshotDir, e);
        }

        log.info("Uploading snapshot '{}' to s3://{}/{} ({} files)", name, os.getBucket(), keyPrefix, files.size());
        for (Path file : files) {
            String relPath = snapshotDir.relativize(file).toString().replace('\\', '/');
            String key = keyPrefix + relPath;
            log.debug("Uploading snapshot file: {}", relPath);
            s3Client.putObject(req -> req.bucket(os.getBucket()).key(key),
                    RequestBody.fromFile(file));
        }
        log.info("Snapshot '{}' uploaded: {} files to s3://{}/{}", name, files.size(), os.getBucket(), keyPrefix);

        // Local directory is no longer needed — the object store is the source of truth.
        try {
            deleteDirectory(snapshotDir);
        } catch (IOException e) {
            // Non-fatal: stale local directory, but the upload succeeded.
            log.warn("Failed to delete local snapshot directory '{}' after upload: {}", snapshotDir, e.getMessage());
        }

        String uri = "s3://" + os.getBucket() + "/" + keyPrefix;
        return new ObjectStoreSnapshotInfo(snapshot.name(), snapshot.createdAt(), snapshot.sizeBytes(), uri);
    }

    // -------------------------------------------------------------------------
    // Rebuild known_entities from cassette data
    // -------------------------------------------------------------------------

    /**
     * Rebuilds the {@code known_entities} registry from scratch by scanning all
     * configured entity-cassette tables ({@code lake.main.entity_*}).
     *
     * <p>For each entity type, computes the earliest and most-recent
     * {@code recorded_at} timestamps per {@code (entity_type, entity_id)} pair and
     * upserts them into {@code known_entities}.  Existing rows for entity IDs that
     * still have cassette data are updated; orphan rows for entity IDs that no
     * longer exist in the cassette are removed.
     *
     * <p>This is a recovery operation — use it after losing the local
     * {@code .ducklake} catalog file while the cassette Parquet data remains
     * intact on object storage.
     *
     * @return number of entity rows upserted
     * @throws SQLException if any DB operation fails
     */
    public long rebuildKnownEntities() throws SQLException {
        List<String> entityTypes = configRepo.listEntityTypes().stream()
                .map(etc -> etc.entityType())
                .toList();

        long total = 0;
        synchronized (duckDB) {
            // Flush any DuckLake inline-buffer data to Parquet so the entity-cassette
            // tables are fully readable before we scan them.
            try (Statement st = duckDB.createStatement()) {
                st.execute("CALL ducklake_flush_inlined_data('lake')");
            } catch (SQLException e) {
                // Non-fatal: inline data is still queryable even without a flush.
                // Log and continue so the rebuild proceeds with whatever is visible.
                log.warn("rebuildKnownEntities: ducklake_flush_inlined_data failed ({}); " +
                         "proceeding – inline data should still be readable", e.getMessage());
            }
            try (Statement st = duckDB.createStatement()) {
                st.execute("CHECKPOINT");
            }

            // Wipe the registry first so stale (deleted) entries are removed
            try (Statement st = duckDB.createStatement()) {
                st.execute("DELETE FROM known_entities");
            }

            for (String entityType : entityTypes) {
                com.joxette.db.SchemaManager.validateEntityType(entityType);
                String src = "lake.main.entity_" + entityType;

                // Check the table exists before querying it
                boolean exists;
                try (PreparedStatement ps = duckDB.prepareStatement(
                        "SELECT 1 FROM duckdb_tables() WHERE database_name='lake' AND schema_name='main' AND table_name=?")) {
                    ps.setString(1, "entity_" + entityType);
                    try (ResultSet rs = ps.executeQuery()) {
                        exists = rs.next();
                    }
                }
                if (!exists) {
                    log.warn("rebuildKnownEntities: table {} not found, skipping", src);
                    continue;
                }

                // Determine the data source for this entity type.
                // Primary: the DuckLake-backed catalog table (fast, structured).
                // Fallback: read_parquet() glob directly from object storage when the catalog
                // table is empty because the .ducklake file was lost/reset — the Parquet files
                // on S3 may still contain all the data.
                String dataSource = resolveEntityDataSource(src, entityType);
                if (dataSource == null) {
                    // No data found either through the catalog or directly from object storage.
                    continue;
                }

                // entity_type is validated against [a-z][a-z0-9_]* — safe to inline.
                // A bound ? in the SELECT list of INSERT...SELECT...GROUP BY confuses DuckDB's
                // query planner (especially when the source is read_parquet), causing it to
                // collapse all rows into a single group instead of grouping by entity_id.
                String sql = """
                        INSERT INTO known_entities (entity_type, entity_id, first_seen, last_seen)
                        SELECT '%s', entity_id, MIN(recorded_at), MAX(recorded_at)
                        FROM %s
                        WHERE entity_id IS NOT NULL AND recorded_at IS NOT NULL
                        GROUP BY entity_id
                        ON CONFLICT (entity_type, entity_id)
                        DO UPDATE SET
                            first_seen = LEAST(known_entities.first_seen, excluded.first_seen),
                            last_seen  = GREATEST(known_entities.last_seen, excluded.last_seen)
                        """.formatted(entityType, dataSource);

                try (Statement st = duckDB.createStatement()) {
                    st.execute(sql);
                }

                // Count how many rows were actually written for this entity type.
                // known_entities was wiped above, so every row here came from this INSERT.
                long rows;
                try (PreparedStatement countPs = duckDB.prepareStatement(
                        "SELECT COUNT(*) FROM known_entities WHERE entity_type = ?")) {
                    countPs.setString(1, entityType);
                    try (ResultSet rs = countPs.executeQuery()) {
                        rows = rs.next() ? rs.getLong(1) : 0L;
                    }
                }
                log.info("rebuildKnownEntities: upserted {} rows for entity type '{}'", rows, entityType);
                total += rows;
            }
        }
        log.info("rebuildKnownEntities complete: {} total rows upserted across {} entity type(s)",
                total, entityTypes.size());
        return total;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Returns the SQL table expression to use as the data source when rebuilding
     * {@code known_entities} for one entity type.
     *
     * <ol>
     *   <li><b>Primary path:</b> {@code lake.main.entity_{type}} — the DuckLake-backed
     *       catalog table.  Used when it contains at least one row.</li>
     *   <li><b>Fallback path:</b> {@code read_parquet('objectStoragePath/**\/*.parquet',
     *       union_by_name=true)} — used when the catalog table is empty (e.g. after the
     *       {@code .ducklake} catalog file was deleted/reset and the Parquet files on S3
     *       are no longer referenced by any catalog snapshot).  Only entity-type Parquet
     *       files contain an {@code entity_id} column; the glob is filtered via
     *       {@code WHERE entity_id IS NOT NULL} in the caller.</li>
     * </ol>
     *
     * <p>Returns {@code null} when both paths yield no data (nothing to rebuild).
     *
     * <p><b>Must be called inside {@code synchronized(duckDB)}.</b>
     *
     * @param catalogSrc  fully-qualified DuckLake table name, e.g. {@code lake.main.entity_fixture}
     * @param entityType  validated entity type name
     * @return SQL expression to FROM-from, or {@code null} if no data found
     */
    private String resolveEntityDataSource(String catalogSrc, String entityType) throws SQLException {
        // --- Primary: check catalog table row count ---
        long srcCount;
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + catalogSrc)) {
            srcCount = rs.next() ? rs.getLong(1) : 0L;
        }
        if (srcCount > 0) {
            log.info("rebuildKnownEntities: scanning {} rows from {} (catalog)", srcCount, catalogSrc);
            return catalogSrc;
        }

        // --- Fallback: try reading Parquet files directly from object storage ---
        if (objectStoragePath == null || objectStoragePath.isBlank()) {
            log.warn("rebuildKnownEntities: catalog table {} is empty and no object-storage-path " +
                     "is configured — cannot fall back to direct Parquet scan for type '{}'",
                     catalogSrc, entityType);
            return null;
        }

        // Build a glob that covers all Parquet files under the object-storage root.
        // DuckLake writes entity-cassette files under a path that contains the table name
        // (e.g. entity_fixture/), but we use **/*.parquet to be robust against path changes.
        // The WHERE entity_id IS NOT NULL filter (in the caller) eliminates general-cassette
        // rows that do not have an entity_id column.
        String base = objectStoragePath.endsWith("/")
                ? objectStoragePath : objectStoragePath + "/";
        String glob = base + "**/*.parquet";
        String parquetSrc = "read_parquet('" + glob + "', union_by_name=true, hive_partitioning=false)";

        log.warn("rebuildKnownEntities: catalog table {} is empty — " +
                 "falling back to direct Parquet scan at '{}' for type '{}'",
                 catalogSrc, glob, entityType);

        long parquetCount;
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT COUNT(*) FROM " + parquetSrc +
                     " WHERE try_cast(entity_id AS VARCHAR) IS NOT NULL")) {
            parquetCount = rs.next() ? rs.getLong(1) : 0L;
        } catch (SQLException e) {
            log.warn("rebuildKnownEntities: Parquet glob scan failed for type '{}' ({}); " +
                     "skipping — check object-storage credentials and path", entityType, e.getMessage());
            return null;
        }

        if (parquetCount == 0) {
            log.warn("rebuildKnownEntities: no entity rows found via Parquet glob '{}' for type '{}'; " +
                     "skipping", glob, entityType);
            return null;
        }

        log.info("rebuildKnownEntities: found {} entity rows via Parquet glob for type '{}'",
                 parquetCount, entityType);

        // Re-populate the empty catalog table from the orphaned Parquet files.
        // The catalog was reset, so lake.main.entity_{type} exists but is empty.
        // Copying the data back makes all downstream queries (replay, stats, compaction)
        // work normally without needing any further changes.
        //
        // Select only the 13 fixed entity-cassette columns by name to avoid column-count
        // mismatches when the Parquet files were written with a different (e.g. newer) schema.
        // union_by_name=true in the read_parquet() call ensures extra Parquet columns are
        // silently ignored and any missing columns are NULL-filled.
        log.info("rebuildKnownEntities: re-populating {} from orphaned Parquet files " +
                 "(catalog was reset; {} rows will be re-inserted)", catalogSrc, parquetCount);
        boolean repopulated = false;
        try (Statement st = duckDB.createStatement()) {
            st.execute(
                "INSERT INTO " + catalogSrc +
                " (recorded_at, entity_id, bucket, message_type, topic," +
                "  kafka_offset, kafka_partition, kafka_timestamp," +
                "  kafka_key, kafka_value, kafka_value_str, metadata, headers)" +
                " SELECT recorded_at, entity_id, bucket, message_type, topic," +
                "        kafka_offset, kafka_partition, kafka_timestamp," +
                "        kafka_key, kafka_value, kafka_value_str, metadata, headers" +
                " FROM " + parquetSrc +
                " WHERE entity_id IS NOT NULL AND recorded_at IS NOT NULL"
            );
            log.info("rebuildKnownEntities: catalog table {} re-populated from Parquet glob", catalogSrc);
            repopulated = true;
        } catch (SQLException e) {
            // Log but don't abort — known_entities rebuild will still proceed using
            // the parquetSrc directly.
            log.warn("rebuildKnownEntities: failed to re-populate {} from Parquet glob ({}); " +
                     "known_entities rebuild will use Parquet source but replay queries may still be empty",
                     catalogSrc, e.getMessage());
            // Emit a detailed schema diff so it is easy to diagnose which columns are
            // present in the Parquet files but missing in the current catalog table schema
            // (or vice-versa) — useful when old cassettes were written with a different
            // schema version.
            logParquetSchemaDiff(catalogSrc, parquetSrc);
        }

        // If re-population succeeded, use the catalog table as the data source so that
        // the GROUP BY in the subsequent known_entities INSERT works correctly through the
        // DuckLake-backed table (avoids DuckDB planner issues with read_parquet() in GROUP BY).
        return repopulated ? catalogSrc : parquetSrc;
    }

    /**
     * Logs a schema diff between the current catalog table and the Parquet files
     * when a re-population INSERT fails due to a column-count or type mismatch.
     *
     * <p>Emits three log lines:
     * <ol>
     *   <li>Catalog table columns (from {@code duckdb_columns()}).</li>
     *   <li>Parquet file columns (from a zero-row {@code DESCRIBE} of the glob).</li>
     *   <li>Columns present in Parquet but absent from the catalog table.</li>
     * </ol>
     *
     * <p>Failures inside this method are swallowed — it is called from an error
     * handler and must not raise a secondary exception.
     *
     * <p><b>Must be called inside {@code synchronized(duckDB)}.</b>
     */
    private void logParquetSchemaDiff(String catalogSrc, String parquetSrc) {
        try {
            // Catalog table columns: database_name='lake', schema_name='main', table_name='entity_...'
            // catalogSrc is e.g. lake.main.entity_fixture → split on '.'
            String[] parts = catalogSrc.split("\\.");
            String catalogDb    = parts.length > 0 ? parts[0] : "";
            String catalogSchema = parts.length > 1 ? parts[1] : "main";
            String catalogTable  = parts.length > 2 ? parts[2] : catalogSrc;

            List<String> catalogCols = new ArrayList<>();
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "SELECT column_name, data_type FROM duckdb_columns()" +
                    " WHERE database_name = ? AND schema_name = ? AND table_name = ?" +
                    " ORDER BY column_index")) {
                ps.setString(1, catalogDb);
                ps.setString(2, catalogSchema);
                ps.setString(3, catalogTable);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        catalogCols.add(rs.getString("column_name") + "(" + rs.getString("data_type") + ")");
                    }
                }
            }
            log.warn("rebuildKnownEntities schema-diff: catalog table {} columns [{}]: {}",
                     catalogSrc, catalogCols.size(), String.join(", ", catalogCols));

            // Parquet file columns: use DESCRIBE on a zero-row SELECT to get schema without scanning all data
            List<String> parquetCols = new ArrayList<>();
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery(
                         "DESCRIBE SELECT * FROM " + parquetSrc + " LIMIT 0")) {
                while (rs.next()) {
                    parquetCols.add(rs.getString("column_name") + "(" + rs.getString("column_type") + ")");
                }
            }
            log.warn("rebuildKnownEntities schema-diff: Parquet glob columns [{}]: {}",
                     parquetCols.size(), String.join(", ", parquetCols));

            // Columns in Parquet but not in catalog (extra columns that caused the mismatch)
            List<String> parquetColNames = parquetCols.stream()
                    .map(c -> c.replaceAll("\\(.*\\)$", "")).toList();
            List<String> catalogColNames = catalogCols.stream()
                    .map(c -> c.replaceAll("\\(.*\\)$", "")).toList();
            List<String> extraInParquet = parquetColNames.stream()
                    .filter(c -> !catalogColNames.contains(c)).toList();
            List<String> missingFromParquet = catalogColNames.stream()
                    .filter(c -> !parquetColNames.contains(c)).toList();
            if (!extraInParquet.isEmpty()) {
                log.warn("rebuildKnownEntities schema-diff: columns in Parquet but NOT in catalog (extra): {}",
                         extraInParquet);
            }
            if (!missingFromParquet.isEmpty()) {
                log.warn("rebuildKnownEntities schema-diff: columns in catalog but NOT in Parquet (missing): {}",
                         missingFromParquet);
            }
            if (extraInParquet.isEmpty() && missingFromParquet.isEmpty()) {
                log.warn("rebuildKnownEntities schema-diff: column names match — mismatch may be positional " +
                         "(column count or type difference)");
            }
        } catch (Exception diagEx) {
            log.debug("rebuildKnownEntities: schema-diff diagnostic failed ({})", diagEx.getMessage());
        }
    }

    private static void validateSnapshotName(String name) {
        if (name == null || !SNAPSHOT_NAME.matcher(name).matches()) {
            throw com.joxette.api.error.ValidationException.field("name",
                    "must match [a-zA-Z0-9_-]+ (got '%s')".formatted(name));
        }
    }

    private static void deleteDirectory(Path dir) throws IOException {
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }

    private static long directorySize(Path dir) {
        try (Stream<Path> walk = Files.walk(dir)) {
            return walk.filter(Files::isRegularFile)
                    .mapToLong(f -> {
                        try { return Files.size(f); }
                        catch (IOException e) { return 0L; }
                    })
                    .sum();
        } catch (IOException e) {
            return -1;
        }
    }
}
