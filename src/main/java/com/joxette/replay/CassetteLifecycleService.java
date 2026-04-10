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
import java.util.List;
import java.util.NoSuchElementException;
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
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement()) {
                // DELETE FROM with no WHERE clause truncates the entire table.
                return st.executeUpdate("DELETE FROM " + qualifiedTable);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Known-entity registry rebuild
    // -------------------------------------------------------------------------

    /**
     * Rebuilds {@code lake.known_entities} from all {@code lake.entity_*} cassette tables.
     *
     * <p>Deletes every existing known-entity row, then re-inserts one row per
     * distinct {@code (entity_id, entity_bucket)} pair found in each entity cassette,
     * deriving {@code first_seen = MIN(timestamp)} and {@code last_seen = MAX(timestamp)}.
     *
     * <p>Use this operation to recover the registry after a GDPR wipe, corruption,
     * or initial bootstrap from a pre-existing DuckLake dataset.
     *
     * @return total number of rows inserted across all entity types
     */
    public long rebuildKnownEntities() throws SQLException {
        synchronized (duckDB) {
            List<String> entityTables = new ArrayList<>();
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery("""
                     SELECT DISTINCT table_name
                     FROM duckdb_tables()
                     WHERE table_name LIKE 'entity_%'
                       AND (schema_name = 'lake'
                            OR (database_name = 'lake' AND schema_name = 'main'))
                     ORDER BY table_name
                     """)) {
                while (rs.next()) {
                    entityTables.add(rs.getString("table_name"));
                }
            }

            try (Statement st = duckDB.createStatement()) {
                st.execute("DELETE FROM lake.known_entities");
            }

            long totalInserted = 0;
            for (String tableName : entityTables) {
                String entityType = tableName.substring("entity_".length());
                try (PreparedStatement ps = duckDB.prepareStatement("""
                        INSERT INTO lake.known_entities
                            (entity_type, entity_id, entity_bucket, first_seen, last_seen)
                        SELECT ?, entity_id, entity_bucket, MIN(timestamp), MAX(timestamp)
                        FROM lake.%s
                        GROUP BY entity_id, entity_bucket
                        """.formatted(tableName))) {
                    ps.setString(1, entityType);
                    totalInserted += ps.executeUpdate();
                }
            }
            return totalInserted;
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
            throw new IllegalArgumentException("Snapshot '" + name + "' already exists");
        }
        try {
            Files.createDirectories(snapshotsBase);
        } catch (IOException e) {
            throw new RuntimeException("Cannot create snapshots directory: " + snapshotsBase, e);
        }
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
            throw new NoSuchElementException("Snapshot not found: " + name);
        }
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement()) {
                st.execute("IMPORT DATABASE '" + snapshotDir + "'");
            }
        }
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
            throw new IllegalStateException(
                    "Object store not configured — set joxette.object-store.bucket to enable exports");
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
            throw new RuntimeException("Cannot read snapshot directory: " + snapshotDir, e);
        }

        for (Path file : files) {
            String relPath = snapshotDir.relativize(file).toString().replace('\\', '/');
            String key = keyPrefix + relPath;
            s3Client.putObject(req -> req.bucket(os.getBucket()).key(key),
                    RequestBody.fromFile(file));
        }

        // Local directory is no longer needed — the object store is the source of truth.
        try {
            deleteDirectory(snapshotDir);
        } catch (IOException e) {
            // Non-fatal: stale local directory, but the upload succeeded.
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

                String sql = """
                        INSERT INTO known_entities (entity_type, entity_id, first_seen, last_seen)
                        SELECT ?, entity_id, MIN(recorded_at), MAX(recorded_at)
                        FROM %s
                        GROUP BY entity_id
                        ON CONFLICT (entity_type, entity_id)
                        DO UPDATE SET
                            first_seen = LEAST(known_entities.first_seen, excluded.first_seen),
                            last_seen  = GREATEST(known_entities.last_seen, excluded.last_seen)
                        """.formatted(src);

                try (PreparedStatement ps = duckDB.prepareStatement(sql)) {
                    ps.setString(1, entityType);
                    int rows = ps.executeUpdate();
                    log.info("rebuildKnownEntities: upserted {} rows for entity type '{}'", rows, entityType);
                    total += rows;
                }
            }
        }
        log.info("rebuildKnownEntities complete: {} total rows upserted across {} entity type(s)",
                total, entityTypes.size());
        return total;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static void validateSnapshotName(String name) {
        if (name == null || !SNAPSHOT_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException(
                    "Invalid snapshot name '%s': must match [a-zA-Z0-9_-]+".formatted(name));
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
