package com.joxette.replay;

import com.joxette.config.JoxetteProperties;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;
import java.util.stream.Stream;

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

    /** Allowed characters in a snapshot name — no slashes, no SQL meta-characters. */
    private static final Pattern SNAPSHOT_NAME = Pattern.compile("[a-zA-Z0-9_-]+");

    private final Connection duckDB;
    private final Path snapshotsBase;

    public CassetteLifecycleService(Connection duckDB, JoxetteProperties properties) {
        this.duckDB = duckDB;
        Path catalogPath = Path.of(properties.getCatalog().getPath());
        Path parent = catalogPath.getParent();
        this.snapshotsBase = (parent != null ? parent : Path.of(".")).resolve("snapshots");
    }

    // -------------------------------------------------------------------------
    // Statistics
    // -------------------------------------------------------------------------

    /**
     * Returns row count and estimated table size for the given topic's slice of
     * {@code lake.cassette}.
     */
    public CassetteStats getTopicCassetteStats(String topic) throws SQLException {
        long rowCount;
        long estimatedSize;
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "SELECT COUNT(*) FROM lake.cassette WHERE topic = ?")) {
                ps.setString(1, topic);
                try (ResultSet rs = ps.executeQuery()) {
                    rowCount = rs.next() ? rs.getLong(1) : 0;
                }
            }
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT COALESCE(estimated_size, 0) AS sz " +
                         "FROM duckdb_tables() WHERE schema_name='lake' AND table_name='cassette'")) {
                estimatedSize = rs.next() ? rs.getLong("sz") : 0;
            }
        }
        return new CassetteStats(topic, "lake.cassette", rowCount, estimatedSize);
    }

    // -------------------------------------------------------------------------
    // Compaction
    // -------------------------------------------------------------------------

    /**
     * Flushes the DuckDB WAL to the main catalog file (equivalent to
     * {@code CHECKPOINT}).  For DuckLake deployments this reduces the amount of
     * data held in the inline buffer and triggers a data-file flush.
     *
     * @param topic present for API symmetry; not used in the checkpoint command
     */
    public void compactTopicCassette(@SuppressWarnings("unused") String topic) throws SQLException {
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement()) {
                st.execute("CHECKPOINT");
            }
        }
    }

    // -------------------------------------------------------------------------
    // Truncation
    // -------------------------------------------------------------------------

    /**
     * Deletes all rows for {@code topic} from {@code lake.cassette}.
     *
     * @return number of rows deleted
     */
    public long truncateTopicCassette(String topic) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM lake.cassette WHERE topic = ?")) {
                ps.setString(1, topic);
                return ps.executeUpdate();
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
        SchemaManager.validateEntityType(entityType);
        long deleted = 0;
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM lake.entity_" + entityType + " WHERE entity_id = ?")) {
                ps.setString(1, entityId);
                deleted += ps.executeUpdate();
            }
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM lake.known_entities WHERE entity_type = ? AND entity_id = ?")) {
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
                    INSERT INTO lake.snapshots (name, created_at, size_bytes)
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
                         "SELECT name, created_at, size_bytes FROM lake.snapshots ORDER BY created_at DESC")) {
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

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static void validateSnapshotName(String name) {
        if (name == null || !SNAPSHOT_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException(
                    "Invalid snapshot name '%s': must match [a-zA-Z0-9_-]+".formatted(name));
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
