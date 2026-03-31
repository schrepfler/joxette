package com.joxette.replay;

import com.joxette.config.JoxetteProperties;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Initialises the DuckDB schema on application startup.
 *
 * <p>Runs once, after Spring has wired all beans, via {@link PostConstruct}.
 * Any {@link SQLException} propagates as an unchecked wrapper so that the
 * application context fails fast rather than silently starting with an
 * incomplete schema.
 *
 * <h2>Schema layout</h2>
 * <pre>
 * lake.cassette               – general-mode messages (all topics that include general routing)
 * lake.entity_{type}          – per-entity-type cassette (one table per configured entity)
 * lake.known_entities         – registry of observed (entity_type, entity_id) pairs
 * lake.config_topics          – runtime topic configuration (mode, paused flag)
 * lake.config_entities        – runtime entity-type configuration (buckets)
 * lake.config_entity_sources  – entity-type ↔ source-topic mappings
 * lake.snapshots              – metadata for EXPORT DATABASE snapshots
 * </pre>
 */
@Component
public class SchemaManager {

    /** Only lower-case letters, digits, and underscores are valid in entity type names. */
    public static final Pattern SAFE_IDENTIFIER = Pattern.compile("[a-z][a-z0-9_]*");

    private static final String HEADERS_TYPE = "STRUCT(key VARCHAR, value BLOB)[]";

    private final Connection duckDB;
    private final JoxetteProperties properties;

    public SchemaManager(Connection duckDB, JoxetteProperties properties) {
        this.duckDB = duckDB;
        this.properties = properties;
    }

    @PostConstruct
    public void initialize() throws SQLException {
        HeadersHelper.registerMacros(duckDB);
        createSchema();
    }

    private void createSchema() throws SQLException {
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement()) {
                st.execute("CREATE SCHEMA IF NOT EXISTS lake");
                createGeneralCassette(st);
                createKnownEntitiesRegistry(st);
                createEntityCassettes(st);
                createConfigTables(st);
                createSnapshotsTable(st);
                createCompactionHistoryTable(st);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Cassette tables
    // -------------------------------------------------------------------------

    private void createGeneralCassette(Statement st) throws SQLException {
        st.execute("""
                CREATE TABLE IF NOT EXISTS lake.cassette (
                    topic        VARCHAR      NOT NULL,
                    partition    INTEGER      NOT NULL,
                    "offset"     BIGINT       NOT NULL,
                    timestamp    TIMESTAMPTZ  NOT NULL,
                    recorded_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
                    key          VARCHAR,
                    value        BLOB,
                    headers      %s,
                    PRIMARY KEY (topic, partition, "offset")
                )
                """.formatted(HEADERS_TYPE));
    }

    private void createKnownEntitiesRegistry(Statement st) throws SQLException {
        st.execute("""
                CREATE TABLE IF NOT EXISTS lake.known_entities (
                    entity_type   VARCHAR      NOT NULL,
                    entity_id     VARCHAR      NOT NULL,
                    entity_bucket INTEGER      NOT NULL,
                    first_seen    TIMESTAMPTZ  NOT NULL,
                    last_seen     TIMESTAMPTZ  NOT NULL,
                    PRIMARY KEY (entity_type, entity_id)
                )
                """);
    }

    private void createEntityCassettes(Statement st) throws SQLException {
        List<JoxetteProperties.Bootstrap.EntityEntry> entities =
                properties.getBootstrap().getEntities();
        for (JoxetteProperties.Bootstrap.EntityEntry entity : entities) {
            createSingleEntityTable(st, entity.getType());
        }
    }

    private void createSingleEntityTable(Statement st, String type) throws SQLException {
        validateEntityType(type);
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
                    headers       %s,
                    PRIMARY KEY (entity_id, timestamp, recorded_at)
                )
                """.formatted(type, HEADERS_TYPE));
    }

    // -------------------------------------------------------------------------
    // Config & snapshot tables
    // -------------------------------------------------------------------------

    private static void createConfigTables(Statement st) throws SQLException {
        st.execute("""
                CREATE TABLE IF NOT EXISTS lake.config_topics (
                    topic  VARCHAR NOT NULL PRIMARY KEY,
                    mode   VARCHAR NOT NULL DEFAULT 'general',
                    paused BOOLEAN NOT NULL DEFAULT FALSE
                )
                """);
        st.execute("""
                CREATE TABLE IF NOT EXISTS lake.config_entities (
                    entity_type VARCHAR  NOT NULL PRIMARY KEY,
                    buckets     INTEGER  NOT NULL DEFAULT 256
                )
                """);
        st.execute("""
                CREATE TABLE IF NOT EXISTS lake.config_entity_sources (
                    entity_type   VARCHAR NOT NULL,
                    topic         VARCHAR NOT NULL,
                    id_source     VARCHAR NOT NULL DEFAULT 'value',
                    id_expression VARCHAR,
                    PRIMARY KEY (entity_type, topic)
                )
                """);
    }

    private static void createSnapshotsTable(Statement st) throws SQLException {
        st.execute("""
                CREATE TABLE IF NOT EXISTS lake.snapshots (
                    name        VARCHAR     NOT NULL PRIMARY KEY,
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                    size_bytes  BIGINT
                )
                """);
    }

    private static void createCompactionHistoryTable(Statement st) throws SQLException {
        // DuckDB 1.5 does not implement GENERATED ALWAYS AS IDENTITY; use a sequence instead.
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
                )
                """);
    }

    // -------------------------------------------------------------------------
    // Public DDL helpers for dynamic entity management
    // -------------------------------------------------------------------------

    /**
     * Creates the {@code lake.entity_{type}} cassette table if it does not yet
     * exist. Safe to call when the table already exists (idempotent).
     */
    public synchronized void createEntityTable(String type) throws SQLException {
        validateEntityType(type);
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement()) {
                createSingleEntityTable(st, type);
            }
        }
    }

    /**
     * Drops the {@code lake.entity_{type}} cassette table. This is a
     * destructive, irreversible operation — callers must confirm intent before
     * invoking it.
     */
    public synchronized void dropEntityTable(String type) throws SQLException {
        validateEntityType(type);
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement()) {
                st.execute("DROP TABLE IF EXISTS lake.entity_" + type);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Validation
    // -------------------------------------------------------------------------

    /**
     * Guards against SQL injection in dynamically constructed table names.
     * Entity type names must match {@code [a-z][a-z0-9_]*}.
     */
    public static void validateEntityType(String type) {
        if (type == null || !SAFE_IDENTIFIER.matcher(type).matches()) {
            throw new IllegalArgumentException(
                    "Invalid entity type name '%s': must match [a-z][a-z0-9_]*".formatted(type));
        }
    }
}
