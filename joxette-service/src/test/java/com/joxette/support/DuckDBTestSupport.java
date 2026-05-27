package com.joxette.support;

import com.joxette.replay.HeadersHelper;

import java.sql.*;
import java.time.Instant;

/**
 * Utility for creating and seeding an in-memory DuckDB connection in tests.
 *
 * <h2>Schema layout</h2>
 * <p>Matches production exactly:
 * <ul>
 *   <li>A secondary in-memory DuckDB database is ATTACHed as {@code lake}, giving
 *       three-part name support: {@code lake.main.<table>}.</li>
 *   <li>Config and compaction tables ({@code topic_configs}, {@code entity_type_configs},
 *       {@code entity_source_mappings}, {@code entity_source_matchers},
 *       {@code known_entities}, {@code compaction_history}, {@code snapshots}) live in
 *       the primary connection's {@code main} schema — matching
 *       {@code SchemaManager.createConfigTables()}.</li>
 *   <li>Cassette tables ({@code general_{topic}}, {@code entity_{type}}) live in
 *       {@code lake.main} — matching {@code SchemaManager.createGeneralCassetteTable()}
 *       and {@code SchemaManager.createEntityCassetteTable()}.</li>
 * </ul>
 *
 * <h2>Column types</h2>
 * <ul>
 *   <li>{@code headers} is {@code STRUCT(key VARCHAR, value VARCHAR)[]} — UTF-8 strings,
 *       matching the production write path in {@code CassetteBatchWriter}.</li>
 *   <li>{@code kafka_value} / {@code kafka_key} are {@code BLOB} / {@code VARCHAR} as
 *       in production.</li>
 * </ul>
 *
+nor * <p>Each call to {@link #newConnection()} returns a fresh, isolated pair of
 * in-memory databases.  Close the connection in {@code @AfterEach} to release resources.
 */
public final class DuckDBTestSupport {

    private DuckDBTestSupport() {}

    // -------------------------------------------------------------------------
    // Connection factory
    // -------------------------------------------------------------------------

    /**
     * Opens a new in-memory DuckDB connection, attaches a second in-memory database
     * as {@code lake}, registers header macros, and creates all tables matching the
     * production schema.
     */
    public static Connection newConnection() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        // Attach a second in-memory DB as 'lake' so three-part names (lake.main.*)
        // resolve correctly — mirroring how DuckLake is attached in production.
        try (Statement st = conn.createStatement()) {
            st.execute("ATTACH ':memory:' AS lake");
        }
        HeadersHelper.registerMacros(conn);
        initSchema(conn);
        return conn;
    }

    // -------------------------------------------------------------------------
    // Schema DDL
    // -------------------------------------------------------------------------

    /**
     * Creates all tables in the supplied connection, matching the production schema
     * created by {@code SchemaManager}.
     *
     * <p>Config tables go in {@code main} (plain DuckDB).
     * Cassette tables go in {@code lake.main} (simulates DuckLake).
     */
    public static void initSchema(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {

            // -----------------------------------------------------------------
            // Config tables — main schema (plain DuckDB, unqualified)
            // -----------------------------------------------------------------

            st.execute("""
                    CREATE TABLE IF NOT EXISTS topic_configs (
                        topic           VARCHAR PRIMARY KEY,
                        mode            VARCHAR NOT NULL
                                          CHECK (mode IN ('general', 'entity_only', 'both')),
                        paused          BOOLEAN NOT NULL DEFAULT false,
                        start_from      VARCHAR NOT NULL DEFAULT 'latest',
                        retention_days  INTEGER,
                        broker_id       VARCHAR,
                        created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
                    )""");

            st.execute("""
                    CREATE TABLE IF NOT EXISTS entity_type_configs (
                        entity_type    VARCHAR PRIMARY KEY,
                        bucket_count   INTEGER NOT NULL DEFAULT 256,
                        retention_days INTEGER,
                        created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
                    )""");

            st.execute("CREATE SEQUENCE IF NOT EXISTS seq_entity_source_mappings START 1");
                    st.execute("""
                    CREATE TABLE IF NOT EXISTS entity_source_mappings (
                        id          INTEGER PRIMARY KEY
                                      DEFAULT nextval('seq_entity_source_mappings'),
                        entity_type VARCHAR NOT NULL,
                        topic       VARCHAR NOT NULL,
                        mode        VARCHAR NOT NULL DEFAULT 'entity_only',
                        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                        UNIQUE (entity_type, topic)
                    )""");

            st.execute("CREATE SEQUENCE IF NOT EXISTS seq_entity_source_matchers START 1");
            st.execute("""
                    CREATE TABLE IF NOT EXISTS entity_source_matchers (
                        id           INTEGER PRIMARY KEY
                                       DEFAULT nextval('seq_entity_source_matchers'),
                        entity_type  VARCHAR NOT NULL,
                        topic        VARCHAR NOT NULL,
                        message_type VARCHAR NOT NULL,
                        id_source    VARCHAR NOT NULL DEFAULT 'value'
                                       CHECK (id_source IN ('key', 'value', 'header')),
                        id_expression VARCHAR,
                        created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                        UNIQUE (entity_type, topic, message_type)
                    )""");

            // Known-entities registry: plain DuckDB in main so ON CONFLICT is enforced
            st.execute("""
                    CREATE TABLE IF NOT EXISTS known_entities (
                        entity_type       VARCHAR   NOT NULL,
                        entity_id         VARCHAR   NOT NULL,
                        first_seen        TIMESTAMPTZ NOT NULL,
                        last_seen         TIMESTAMPTZ NOT NULL,
                        message_count     BIGINT    NOT NULL DEFAULT 0,
                        source_topics     VARCHAR[] NOT NULL DEFAULT [],
                        last_message_type VARCHAR,
                        PRIMARY KEY (entity_type, entity_id)
                    )""");

            st.execute("CREATE SEQUENCE IF NOT EXISTS seq_compaction_history START 1");
            st.execute("""
                    CREATE TABLE IF NOT EXISTS compaction_history (
                        id              INTEGER PRIMARY KEY
                                          DEFAULT nextval('seq_compaction_history'),
                        started_at      TIMESTAMPTZ NOT NULL,
                        completed_at    TIMESTAMPTZ,
                        status          VARCHAR NOT NULL,
                        triggered_by    VARCHAR NOT NULL DEFAULT 'unknown',
                        targets         VARCHAR[],
                        entity_buckets_compacted     INTEGER NOT NULL DEFAULT 0,
                        general_partitions_compacted INTEGER NOT NULL DEFAULT 0,
                        files_processed BIGINT  NOT NULL DEFAULT 0,
                        files_created   BIGINT  NOT NULL DEFAULT 0,
                        error_message   VARCHAR
                    )""");

            st.execute("""
                    CREATE TABLE IF NOT EXISTS snapshots (
                        name        VARCHAR     NOT NULL PRIMARY KEY,
                        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                        size_bytes  BIGINT
                    )""");

            st.execute("""
                    CREATE TABLE IF NOT EXISTS compaction_locks (
                        target      VARCHAR     NOT NULL PRIMARY KEY,
                        instance_id VARCHAR     NOT NULL,
                        acquired_at TIMESTAMPTZ NOT NULL,
                        expires_at  TIMESTAMPTZ NOT NULL
                    )""");

            st.execute("""
                    CREATE TABLE IF NOT EXISTS topic_message_type_matchers (
                        topic           VARCHAR NOT NULL,
                        message_type    VARCHAR NOT NULL,
                        id_source       VARCHAR NOT NULL,
                        id_expression   VARCHAR NOT NULL,
                        PRIMARY KEY (topic, message_type)
                    )""");

            st.execute("""
                    CREATE TABLE IF NOT EXISTS broker_configs (
                        broker_id               VARCHAR PRIMARY KEY,
                        bootstrap_servers       VARCHAR NOT NULL,
                        security_protocol       VARCHAR NOT NULL DEFAULT 'PLAINTEXT',
                        sasl_mechanism          VARCHAR,
                        sasl_username           VARCHAR,
                        sasl_password           VARCHAR,
                        ssl_truststore_path     VARCHAR,
                        ssl_truststore_password VARCHAR,
                        ssl_keystore_path       VARCHAR,
                        ssl_keystore_password   VARCHAR,
                        created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
                    )""");

            // Instance registry — tracks running Joxette processes.
            st.execute("""
                    CREATE TABLE IF NOT EXISTS joxette_instances (
                        instance_id      VARCHAR     NOT NULL PRIMARY KEY,
                        roles            VARCHAR[]   NOT NULL,
                        catalog_backend  VARCHAR     NOT NULL,
                        started_at       TIMESTAMPTZ NOT NULL,
                        last_heartbeat   TIMESTAMPTZ NOT NULL,
                        kafka_assignments JSON
                    )""");

            st.execute("CREATE SEQUENCE IF NOT EXISTS seq_retention_history START 1");
            st.execute("""
                    CREATE TABLE IF NOT EXISTS retention_history (
                        id                     INTEGER     PRIMARY KEY
                                                 DEFAULT nextval('seq_retention_history'),
                        started_at             TIMESTAMPTZ NOT NULL,
                        completed_at           TIMESTAMPTZ,
                        status                 VARCHAR     NOT NULL
                                                 CHECK (status IN ('running', 'completed', 'failed')),
                        triggered_by           VARCHAR     NOT NULL,
                        entity_rows_deleted    BIGINT      NOT NULL DEFAULT 0,
                        general_rows_deleted   BIGINT      NOT NULL DEFAULT 0,
                        known_entities_deleted BIGINT      NOT NULL DEFAULT 0,
                        error_message          VARCHAR
                    )""");

            st.execute("""
                    CREATE TABLE IF NOT EXISTS transform_presets (
                        name        VARCHAR     NOT NULL PRIMARY KEY,
                        description VARCHAR,
                        steps       JSON        NOT NULL,
                        fragments   JSON,
                        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
                    )""");

            st.execute("""
                    CREATE TABLE IF NOT EXISTS stream_definitions (
                        id          VARCHAR     NOT NULL PRIMARY KEY
                                      CHECK (id ~ '^[a-z][a-z0-9_-]*$'),
                        name        VARCHAR     NOT NULL,
                        entity_type VARCHAR     NOT NULL,
                        entity_id   VARCHAR,
                        definition  JSON        NOT NULL,
                        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
                    )""");
        }
    }

    // -------------------------------------------------------------------------
    // DuckLake-schema cassette tables (lake.main.*)
    // -------------------------------------------------------------------------

    /**
     * Creates the general cassette table {@code lake.main.general_{normalizedTopic}}.
     *
     * <p>Column names and types match {@code SchemaManager.createGeneralCassetteTable()}.
     * No PRIMARY KEY — deduplication is handled at query time via QUALIFY ROW_NUMBER().
     */
    public static void createGeneralCassetteTable(Connection conn, String topic) throws SQLException {
        String tableName = "general_" + normalizeTopicName(topic);
        try (Statement st = conn.createStatement()) {
            st.execute(String.format("""
                    CREATE TABLE IF NOT EXISTS lake.main.%s (
                        recorded_at     TIMESTAMPTZ NOT NULL,
                        kafka_offset    BIGINT      NOT NULL,
                        kafka_partition INTEGER     NOT NULL,
                        kafka_timestamp TIMESTAMPTZ NOT NULL,
                        kafka_key       VARCHAR,
                        kafka_value     BLOB,
                        kafka_value_str VARCHAR,
                        metadata        VARCHAR,
                        headers         STRUCT(key VARCHAR, value VARCHAR)[],
                        message_type    VARCHAR
                    )""", tableName));
        }
    }

    /**
     * Creates the entity cassette table {@code lake.main.entity_{type}}.
     *
     * <p>Column names and types match {@code SchemaManager.createEntityCassetteTable()}.
     * No PRIMARY KEY — deduplication at query time via QUALIFY ROW_NUMBER().
     */
    public static void createEntityTable(Connection conn, String type) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(String.format("""
                    CREATE TABLE IF NOT EXISTS lake.main.entity_%s (
                        recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
                        entity_id       VARCHAR     NOT NULL,
                        bucket          INTEGER     NOT NULL,
                        message_type    VARCHAR,
                        topic           VARCHAR     NOT NULL,
                        kafka_offset    BIGINT      NOT NULL,
                        kafka_partition INTEGER     NOT NULL,
                        kafka_timestamp TIMESTAMPTZ NOT NULL,
                        kafka_key       VARCHAR,
                        kafka_value     BLOB,
                        kafka_value_str VARCHAR,
                        metadata        VARCHAR,
                        headers         STRUCT(key VARCHAR, value VARCHAR)[]
                    )""", type));
        }
    }

    // -------------------------------------------------------------------------
    // Data helpers
    // -------------------------------------------------------------------------

    /**
     * Inserts a row into {@code lake.main.general_{normalizedTopic}}.
     *
     * <p>Column names match the production schema:
     * {@code kafka_partition}, {@code kafka_offset}, {@code kafka_timestamp},
     * {@code kafka_key}, {@code kafka_value}.
     */
    public static void insertCassetteRow(Connection conn,
            String topic, int partition, long offset,
            Instant timestamp, Instant recordedAt,
            String key, byte[] value) throws SQLException {
        insertCassetteRow(conn, topic, partition, offset, timestamp, recordedAt, key, value, null);
    }

    /** Overload that also sets {@code message_type}. */
    public static void insertCassetteRow(Connection conn,
            String topic, int partition, long offset,
            Instant timestamp, Instant recordedAt,
            String key, byte[] value, String messageType) throws SQLException {
        String tableName = "lake.main.general_" + normalizeTopicName(topic);
        try (PreparedStatement ps = conn.prepareStatement(String.format("""
                INSERT INTO %s
                    (recorded_at, kafka_offset, kafka_partition, kafka_timestamp,
                     kafka_key, kafka_value, kafka_value_str, metadata, headers, message_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, NULL, [], ?)
                """, tableName))) {
            ps.setTimestamp(1, Timestamp.from(recordedAt));
            ps.setLong(2, offset);
            ps.setInt(3, partition);
            ps.setTimestamp(4, Timestamp.from(timestamp));
            ps.setString(5, key);
            ps.setBytes(6, value);
            ps.setString(7, value != null ? new String(value) : null);
            ps.setString(8, messageType);
            ps.executeUpdate();
        }
    }

    /**
     * Inserts a row into {@code lake.main.entity_{type}}.
     */
    public static void insertEntityRow(Connection conn,
            String entityType, String entityId, int entityBucket, String messageType,
            String topic, int partition, long offset,
            Instant timestamp, Instant recordedAt,
            String key, byte[] value) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(String.format("""
                INSERT INTO lake.main.entity_%s
                    (recorded_at, entity_id, bucket, message_type, topic,
                     kafka_offset, kafka_partition, kafka_timestamp,
                     kafka_key, kafka_value, kafka_value_str, metadata, headers)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, [])
                """, entityType))) {
            ps.setTimestamp(1, Timestamp.from(recordedAt));
            ps.setString(2, entityId);
            ps.setInt(3, entityBucket);
            ps.setString(4, messageType);
            ps.setString(5, topic);
            ps.setLong(6, offset);
            ps.setInt(7, partition);
            ps.setTimestamp(8, Timestamp.from(timestamp));
            ps.setString(9, key);
            ps.setBytes(10, value);
            ps.setString(11, value != null ? new String(value) : null);
            ps.executeUpdate();
        }
    }

    /** Counts rows in any table by fully-qualified name (e.g. {@code lake.main.entity_order}). */
    public static long countRows(Connection conn, String qualifiedTable) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + qualifiedTable)) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Normalises a topic name to {@code [a-z0-9_]}. */
    private static String normalizeTopicName(String topic) {
        return topic.toLowerCase().replaceAll("[^a-z0-9_]", "_");
    }
}
