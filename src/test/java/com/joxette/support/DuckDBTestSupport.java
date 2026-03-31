package com.joxette.support;

import com.joxette.replay.HeadersHelper;

import java.sql.*;
import java.time.Instant;

/**
 * Utility for creating and seeding an in-memory DuckDB connection in tests.
 *
 * <p>Each call to {@link #newConnection()} returns a fresh, isolated in-memory
 * database with the full schema already applied.  Tests should close the
 * returned connection in {@code @AfterEach} / try-with-resources to release
 * DuckDB resources promptly.
 */
public final class DuckDBTestSupport {

    private DuckDBTestSupport() {}

    // -------------------------------------------------------------------------
    // Connection factory
    // -------------------------------------------------------------------------

    /**
     * Opens a new in-memory DuckDB connection, registers header macros, and
     * creates the full lake schema.
     */
    public static Connection newConnection() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        HeadersHelper.registerMacros(conn);
        initSchema(conn);
        return conn;
    }

    // -------------------------------------------------------------------------
    // Schema DDL
    // -------------------------------------------------------------------------

    /** Creates all lake.* tables in the supplied connection. */
    public static void initSchema(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS lake");

            // General cassette — no PRIMARY KEY so deduplication tests can insert
            // duplicate (topic, partition, offset) rows, matching DuckDB's behaviour
            // in older versions where PK uniqueness was not enforced.
            // Deduplication is handled at query time via QUALIFY ROW_NUMBER().
            st.execute("""
                    CREATE TABLE IF NOT EXISTS lake.cassette (
                        topic        VARCHAR      NOT NULL,
                        partition    INTEGER      NOT NULL,
                        "offset"     BIGINT       NOT NULL,
                        timestamp    TIMESTAMPTZ  NOT NULL,
                        recorded_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
                        key          VARCHAR,
                        value        BLOB,
                        headers      STRUCT(key VARCHAR, value BLOB)[]
                    )""");

            // Entity registry
            st.execute("""
                    CREATE TABLE IF NOT EXISTS lake.known_entities (
                        entity_type   VARCHAR      NOT NULL,
                        entity_id     VARCHAR      NOT NULL,
                        entity_bucket INTEGER      NOT NULL,
                        first_seen    TIMESTAMPTZ  NOT NULL,
                        last_seen     TIMESTAMPTZ  NOT NULL,
                        PRIMARY KEY (entity_type, entity_id)
                    )""");

            // Config tables
            st.execute("""
                    CREATE TABLE IF NOT EXISTS lake.config_topics (
                        topic  VARCHAR NOT NULL PRIMARY KEY,
                        mode   VARCHAR NOT NULL DEFAULT 'general',
                        paused BOOLEAN NOT NULL DEFAULT FALSE
                    )""");
            st.execute("""
                    CREATE TABLE IF NOT EXISTS lake.config_entities (
                        entity_type VARCHAR  NOT NULL PRIMARY KEY,
                        buckets     INTEGER  NOT NULL DEFAULT 256
                    )""");
            st.execute("""
                    CREATE TABLE IF NOT EXISTS lake.config_entity_sources (
                        entity_type   VARCHAR NOT NULL,
                        topic         VARCHAR NOT NULL,
                        id_source     VARCHAR NOT NULL DEFAULT 'value',
                        id_expression VARCHAR,
                        PRIMARY KEY (entity_type, topic)
                    )""");

            // Snapshot and compaction tables
            st.execute("""
                    CREATE TABLE IF NOT EXISTS lake.snapshots (
                        name        VARCHAR     NOT NULL PRIMARY KEY,
                        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                        size_bytes  BIGINT
                    )""");
            st.execute("CREATE SEQUENCE IF NOT EXISTS lake.compaction_history_id_seq START 1");
            st.execute("""
                    CREATE TABLE IF NOT EXISTS lake.compaction_history (
                        id              BIGINT      DEFAULT nextval('lake.compaction_history_id_seq') PRIMARY KEY,
                        started_at      TIMESTAMPTZ NOT NULL,
                        completed_at    TIMESTAMPTZ,
                        status          VARCHAR     NOT NULL,
                        triggered_by    VARCHAR     NOT NULL,
                        targets         VARCHAR[],
                        entity_buckets_compacted     INTEGER NOT NULL DEFAULT 0,
                        general_partitions_compacted INTEGER NOT NULL DEFAULT 0,
                        error_message   VARCHAR
                    )""");
        }
    }

    /**
     * Creates the {@code lake.entity_{type}} cassette table.
     * No PRIMARY KEY so deduplication tests can insert duplicate source offsets;
     * deduplication happens at query time via QUALIFY ROW_NUMBER().
     */
    public static void createEntityTable(Connection conn, String type) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("""
                    CREATE TABLE IF NOT EXISTS lake.entity_%s (
                        entity_id     VARCHAR      NOT NULL,
                        entity_bucket INTEGER      NOT NULL,
                        topic         VARCHAR      NOT NULL,
                        partition     INTEGER      NOT NULL,
                        "offset"      BIGINT       NOT NULL,
                        timestamp     TIMESTAMPTZ  NOT NULL,
                        recorded_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
                        key           VARCHAR,
                        value         BLOB,
                        headers       STRUCT(key VARCHAR, value BLOB)[]
                    )""".formatted(type));
        }
    }

    // -------------------------------------------------------------------------
    // Data helpers
    // -------------------------------------------------------------------------

    /**
     * Inserts a row into {@code lake.cassette}.
     * DuckDB does not enforce PK uniqueness, so the same
     * {@code (topic, partition, offset)} can be inserted multiple times
     * (useful for deduplication tests).
     */
    public static void insertCassetteRow(Connection conn,
            String topic, int partition, long offset,
            Instant timestamp, Instant recordedAt,
            String key, byte[] value) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO lake.cassette
                    (topic, partition, "offset", timestamp, recorded_at, key, value, headers)
                VALUES (?, ?, ?, ?, ?, ?, ?, [])
                """)) {
            ps.setString(1, topic);
            ps.setInt(2, partition);
            ps.setLong(3, offset);
            ps.setTimestamp(4, Timestamp.from(timestamp));
            ps.setTimestamp(5, Timestamp.from(recordedAt));
            ps.setString(6, key);
            ps.setBytes(7, value);
            ps.executeUpdate();
        }
    }

    /**
     * Inserts a row into {@code lake.entity_{type}}.
     */
    public static void insertEntityRow(Connection conn,
            String entityType, String entityId, int entityBucket,
            String topic, int partition, long offset,
            Instant timestamp, Instant recordedAt,
            String key, byte[] value) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO lake.entity_%s
                    (entity_id, entity_bucket, topic, partition, "offset",
                     timestamp, recorded_at, key, value, headers)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, [])
                """.formatted(entityType))) {
            ps.setString(1, entityId);
            ps.setInt(2, entityBucket);
            ps.setString(3, topic);
            ps.setInt(4, partition);
            ps.setLong(5, offset);
            ps.setTimestamp(6, Timestamp.from(timestamp));
            ps.setTimestamp(7, Timestamp.from(recordedAt));
            ps.setString(8, key);
            ps.setBytes(9, value);
            ps.executeUpdate();
        }
    }

    /** Counts rows in any lake.* table by name. */
    public static long countRows(Connection conn, String qualifiedTable) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + qualifiedTable)) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }
}
