package com.joxette.db;

import com.joxette.config.JoxetteProperties;
import com.joxette.config.JoxetteProperties.Bootstrap.EntityEntry;
import com.joxette.config.JoxetteProperties.Bootstrap.TopicEntry;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Creates all application tables and DuckDB macros at startup.
 *
 * <p>Execution order (all idempotent):
 * <ol>
 *   <li>Probe VARIANT support through a DuckLake round-trip; fall back to JSON.</li>
 *   <li>Create plain-DuckDB config tables in the {@code main} schema.</li>
 *   <li>Register DuckDB scalar macros for Kafka header manipulation.</li>
 *   <li>Create DuckLake-backed tables (general cassettes, entity cassettes,
 *       {@code known_entities}) in {@code lake.main}.</li>
 * </ol>
 *
 * <p>This bean depends on {@link DuckLakeManager}, so Spring guarantees that the
 * DuckLake catalog is fully attached before any table DDL runs.
 *
 * <h3>Table naming</h3>
 * <ul>
 *   <li>General cassette: {@code lake.main.general_<normalized_topic>}</li>
 *   <li>Entity cassette:  {@code lake.main.entity_<normalized_type>}</li>
 *   <li>known_entities:   {@code lake.main.known_entities}</li>
 * </ul>
 * Topic and entity-type names are normalised to {@code [a-z0-9_]} before use.
 */
@Component("dbSchemaManager")
public class SchemaManager {

    private static final Logger log = LoggerFactory.getLogger(SchemaManager.class);

    private final DuckLakeManager duckLakeManager;
    private final JoxetteProperties properties;

    /** Set during init; other beans may read this after the context is ready. */
    private boolean variantSupported;

    public SchemaManager(DuckLakeManager duckLakeManager, JoxetteProperties properties) {
        this.duckLakeManager = duckLakeManager;
        this.properties      = properties;
    }

    @PostConstruct
    public void initialize() throws SQLException {
        Connection conn    = duckLakeManager.getConnection();
        String    catalog  = duckLakeManager.getCatalogName();

        variantSupported = probeVariant(conn, catalog);
        log.info("VARIANT type through DuckLake Parquet: {}", variantSupported ? "supported" : "not supported (using JSON)");

        createConfigTables(conn);
        registerMacros(conn);

        if (!isCatalogAttached(conn, catalog)) {
            throw new SQLException(
                "DuckLake catalog '" + catalog + "' is not attached. " +
                "Check that the ducklake extension loaded successfully and that the ATTACH statement " +
                "in DuckLakeManager did not fail silently (look for earlier WARN/ERROR log lines).");
        }
        createLakeTables(conn, catalog);
        migrateGeneralCassetteTables(conn, catalog);

        log.info("Schema initialisation complete");
    }

    // -------------------------------------------------------------------------
    // VARIANT probe
    // -------------------------------------------------------------------------

    /**
     * Tests whether VARIANT survives a full DuckLake write/read round-trip.
     * Creates a temporary DuckLake probe table, inserts one row, reads it back,
     * and drops the table.  Returns {@code false} on any failure.
     *
     * <p>The catalog is verified to exist before any DDL is attempted.  If the catalog
     * is absent (e.g. ATTACH failed silently), the method returns {@code false}
     * immediately without issuing any SQL against the catalog, avoiding the DuckDB
     * "pending query" connection-corruption that occurs when a statement against a
     * non-existent catalog fails.
     *
     * <p>On any other failure the connection is reset via {@link Connection#rollback()}
     * to clear the pending-query flag before returning.
     */
    private boolean probeVariant(Connection conn, String catalog) {
        // Guard: verify the catalog is actually attached before issuing any DDL against it.
        if (!isCatalogAttached(conn, catalog)) {
            log.warn("DuckLake catalog '{}' is not attached; skipping VARIANT probe, falling back to JSON", catalog);
            return false;
        }

        String probeTable = catalog + ".main.__variant_probe";
        try {
            exec(conn, "DROP TABLE IF EXISTS " + probeTable);
            exec(conn, "CREATE TABLE " + probeTable + " (v VARIANT)");
            exec(conn, "INSERT INTO " + probeTable + " VALUES ('{\"probe\":true}'::VARIANT)");
            try (Statement stmt = conn.createStatement();
                 var rs = stmt.executeQuery("SELECT v FROM " + probeTable)) {
                // consume result set so the connection is not left in a pending state
                rs.next();
            }
            exec(conn, "DROP TABLE IF EXISTS " + probeTable);
            return true;
        } catch (SQLException e) {
            log.warn("VARIANT probe failed ({}); falling back to JSON for flexible columns", e.getMessage());
            // Reset DuckDB connection state – a failed statement leaves a "pending query"
            // error on the connection that will break every subsequent execute() call.
            try {
                conn.rollback();
            } catch (SQLException rollbackEx) {
                log.debug("Rollback after VARIANT probe failed: {}", rollbackEx.getMessage());
            }
            return false;
        }
    }

    /**
     * Returns {@code true} if {@code catalogName} appears in {@code duckdb_databases()}.
     * Uses a fresh {@link Statement} so a failure here cannot leave a pending-query state
     * that would corrupt subsequent DDL on the shared connection.
     */
    private boolean isCatalogAttached(Connection conn, String catalogName) {
        try (Statement stmt = conn.createStatement();
             var rs = stmt.executeQuery(
                 "SELECT database_name FROM duckdb_databases() WHERE database_name = '" + catalogName + "'")) {
            return rs.next();
        } catch (SQLException e) {
            log.debug("Could not query duckdb_databases(): {}", e.getMessage());
            return false;
        }
    }

    // -------------------------------------------------------------------------
    // Config tables (plain DuckDB, main schema)
    // -------------------------------------------------------------------------

    private void createConfigTables(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS topic_configs (
                    topic          VARCHAR PRIMARY KEY,
                    mode           VARCHAR NOT NULL
                                     CHECK (mode IN ('general', 'entity_only', 'both')),
                    paused         BOOLEAN NOT NULL DEFAULT false,
                    start_from     VARCHAR NOT NULL DEFAULT 'latest',
                    retention_days INTEGER,
                    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """);
            stmt.execute("ALTER TABLE topic_configs ADD COLUMN IF NOT EXISTS retention_days INTEGER");
            stmt.execute("ALTER TABLE topic_configs ADD COLUMN IF NOT EXISTS start_from VARCHAR DEFAULT 'latest'");

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS entity_type_configs (
                    entity_type    VARCHAR PRIMARY KEY,
                    bucket_count   INTEGER NOT NULL DEFAULT 256,
                    retention_days INTEGER,
                    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """);
            stmt.execute("ALTER TABLE entity_type_configs ADD COLUMN IF NOT EXISTS retention_days INTEGER");

            stmt.execute("CREATE SEQUENCE IF NOT EXISTS seq_entity_source_mappings START 1");

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS entity_source_mappings (
                    id          INTEGER PRIMARY KEY
                                  DEFAULT nextval('seq_entity_source_mappings'),
                    entity_type VARCHAR NOT NULL,
                    topic       VARCHAR NOT NULL,
                    mode        VARCHAR NOT NULL DEFAULT 'entity_only'
                                  CHECK (mode IN ('entity_only', 'both')),
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (entity_type, topic)
                )
                """);

            stmt.execute("CREATE SEQUENCE IF NOT EXISTS seq_entity_source_matchers START 1");

            // Each source mapping can have multiple matchers — one per message variant
            // that carries the entity ID (e.g. marketSet, resultSet, coverage all carry
            // fixtureId for the same logical fixture entity).
            stmt.execute("""
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
                )
                """);

            // Known-entities registry: plain DuckDB so ON CONFLICT is enforced.
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS known_entities (
                    entity_type  VARCHAR NOT NULL,
                    entity_id    VARCHAR NOT NULL,
                    first_seen   TIMESTAMPTZ NOT NULL,
                    last_seen    TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (entity_type, entity_id)
                )
                """);

            stmt.execute("CREATE SEQUENCE IF NOT EXISTS seq_compaction_history START 1");

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS compaction_history (
                    id              INTEGER PRIMARY KEY
                                      DEFAULT nextval('seq_compaction_history'),
                    started_at      TIMESTAMPTZ NOT NULL,
                    completed_at    TIMESTAMPTZ,
                    status          VARCHAR NOT NULL
                                      CHECK (status IN ('running', 'completed', 'failed')),
                    triggered_by    VARCHAR NOT NULL DEFAULT 'unknown',
                    targets         VARCHAR[],
                    entity_buckets_compacted     INTEGER NOT NULL DEFAULT 0,
                    general_partitions_compacted INTEGER NOT NULL DEFAULT 0,
                    error_message   VARCHAR
                )
                """);

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS snapshots (
                    name        VARCHAR     NOT NULL PRIMARY KEY,
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                    size_bytes  BIGINT
                )
                """);

            // Message-type matchers for general cassettes.
            // Semantics: first matcher whose id_source/id_expression extracts a non-null
            // value from a message wins; its message_type is stored in the cassette row.
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS topic_message_type_matchers (
                    topic           VARCHAR NOT NULL,
                    message_type    VARCHAR NOT NULL,
                    id_source       VARCHAR NOT NULL
                                      CHECK (id_source IN ('key', 'value', 'header')),
                    id_expression   VARCHAR NOT NULL,
                    PRIMARY KEY (topic, message_type)
                )
                """);

            stmt.execute("CREATE SEQUENCE IF NOT EXISTS seq_retention_history START 1");

            stmt.execute("""
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
                )
                """);

            // Migration: add columns introduced after the initial lake-schema version.
            // ALTER TABLE ADD COLUMN IF NOT EXISTS is idempotent.
            migrateCompactionHistory(conn);

            log.debug("Config tables ready");
        }
    }

    // -------------------------------------------------------------------------
    // DuckDB scalar macros
    // -------------------------------------------------------------------------

    /**
     * Registers four scalar macros for working with Kafka headers.
     *
     * <p>Headers are stored as {@code STRUCT(key VARCHAR, value BLOB)[]}.
     *
     * <ul>
     *   <li>{@code headers_get(headers, key)}      – first value for {@code key}, or NULL</li>
     *   <li>{@code headers_get_all(headers, key)}  – all values for {@code key} as BLOB[]</li>
     *   <li>{@code headers_put(headers, key, val)} – appends a new header entry</li>
     *   <li>{@code headers_to_map(headers)}        – converts to MAP(VARCHAR, BLOB);
     *       last value wins for duplicate keys</li>
     * </ul>
     */
    private void registerMacros(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {

            stmt.execute("""
                CREATE OR REPLACE MACRO headers_get(headers, hkey) AS
                    list_filter(headers, h -> h.key = hkey)[1].value
                """);

            stmt.execute("""
                CREATE OR REPLACE MACRO headers_get_all(headers, hkey) AS
                    list_transform(list_filter(headers, h -> h.key = hkey), h -> h.value)
                """);

            stmt.execute("""
                CREATE OR REPLACE MACRO headers_put(headers, hkey, hvalue) AS
                    list_append(headers, {'key': hkey, 'value': hvalue})
                """);

            stmt.execute("""
                CREATE OR REPLACE MACRO headers_to_map(headers) AS
                    MAP(
                        list_transform(headers, h -> h.key),
                        list_transform(headers, h -> h.value)
                    )
                """);

            log.debug("DuckDB macros registered: headers_get, headers_get_all, headers_put, headers_to_map");
        }
    }

    // -------------------------------------------------------------------------
    // DuckLake-backed tables (cassettes + known_entities)
    // -------------------------------------------------------------------------

    private void createLakeTables(Connection conn, String catalog) throws SQLException {
        String flexType = variantSupported ? "VARIANT" : "JSON";

        List<TopicEntry> topics = properties.getBootstrap().getTopics();
        if (topics != null) {
            for (TopicEntry t : topics) {
                String mode = t.getMode();
                if ("general".equals(mode) || "both".equals(mode)) {
                    String tbl = "general_" + normalize(t.getTopic());
                    createGeneralCassetteTable(conn, catalog, tbl, flexType);
                }
            }
        }

        List<EntityEntry> entities = properties.getBootstrap().getEntities();
        if (entities != null) {
            for (EntityEntry e : entities) {
                String tbl = "entity_" + normalize(e.getType());
                createEntityCassetteTable(conn, catalog, tbl, flexType);
            }
        }
    }

    /**
     * Records every raw Kafka message from a topic verbatim.
     * {@code metadata} holds any decoded JSON/VARIANT envelope for ad-hoc queries.
     */
    private void createGeneralCassetteTable(Connection conn, String catalog,
                                             String tableName, String flexType)
            throws SQLException {
        exec(conn, String.format("""
            CREATE TABLE IF NOT EXISTS %s.main.%s (
                recorded_at     TIMESTAMPTZ NOT NULL,
                kafka_offset    BIGINT      NOT NULL,
                kafka_partition INTEGER     NOT NULL,
                kafka_timestamp TIMESTAMPTZ NOT NULL,
                kafka_key       BLOB,
                kafka_value     BLOB,
                kafka_value_str VARCHAR,
                metadata        %s,
                headers         STRUCT(key VARCHAR, value VARCHAR)[],
                message_type    VARCHAR
            )
            """, catalog, tableName, flexType));
        log.debug("DuckLake table ready: {}.main.{}", catalog, tableName);
    }

    /**
     * Records Kafka messages for a specific entity type, partitioned by bucket.
     * {@code bucket} = {@code hash(entity_id) mod bucket_count} for even distribution.
     */
    private void createEntityCassetteTable(Connection conn, String catalog,
                                            String tableName, String flexType)
            throws SQLException {
        exec(conn, String.format("""
            CREATE TABLE IF NOT EXISTS %s.main.%s (
                recorded_at     TIMESTAMPTZ NOT NULL,
                entity_id       VARCHAR     NOT NULL,
                bucket          INTEGER     NOT NULL,
                message_type    VARCHAR,
                topic           VARCHAR     NOT NULL,
                kafka_offset    BIGINT      NOT NULL,
                kafka_partition INTEGER     NOT NULL,
                kafka_timestamp TIMESTAMPTZ NOT NULL,
                kafka_key       BLOB,
                kafka_value     BLOB,
                kafka_value_str VARCHAR,
                metadata        %s,
                headers         STRUCT(key VARCHAR, value VARCHAR)[]
            )
            """, catalog, tableName, flexType));
        log.debug("DuckLake table ready: {}.main.{}", catalog, tableName);
    }

    // -------------------------------------------------------------------------
    // Dynamic entity table management (called at runtime from EntityController)
    // -------------------------------------------------------------------------

    /**
     * Creates the {@code <catalog>.main.entity_{type}} cassette table if it does not
     * yet exist. Idempotent – safe to call when the table already exists.
     */
    public void createEntityTable(String type) throws SQLException {
        validateEntityType(type);
        Connection conn   = duckLakeManager.getConnection();
        String    catalog = duckLakeManager.getCatalogName();
        String    flexType = variantSupported ? "VARIANT" : "JSON";
        createEntityCassetteTable(conn, catalog, "entity_" + type, flexType);
        log.info("Entity cassette table created/verified: {}.main.entity_{}", catalog, type);
    }

    /**
     * Drops the {@code <catalog>.main.entity_{type}} cassette table.
     * Destructive and irreversible – callers must confirm intent before invoking.
     */
    public void dropEntityTable(String type) throws SQLException {
        validateEntityType(type);
        Connection conn   = duckLakeManager.getConnection();
        String    catalog = duckLakeManager.getCatalogName();
        exec(conn, "DROP TABLE IF EXISTS " + catalog + ".main.entity_" + type);
        log.info("Entity cassette table dropped: {}.main.entity_{}", catalog, type);
    }

    /**
     * Guards against SQL injection in dynamically constructed table names.
     * Entity type names must match {@code [a-z][a-z0-9_]*}.
     */
    public static void validateEntityType(String type) {
        if (type == null || !type.matches("[a-z][a-z0-9_]*")) {
            throw new IllegalArgumentException(
                "Invalid entity type name '%s': must match [a-z][a-z0-9_]*".formatted(type));
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Idempotent migration: adds columns to {@code compaction_history} that did
     * not exist in the original lake-schema version of the table.
     * Each ALTER TABLE is attempted individually; failures are swallowed so a
     * column that already exists does not abort startup.
     */
    private void migrateCompactionHistory(Connection conn) {
        String[][] migrations = {
            { "triggered_by",    "ALTER TABLE compaction_history ADD COLUMN triggered_by VARCHAR DEFAULT 'unknown'" },
            { "targets",         "ALTER TABLE compaction_history ADD COLUMN targets VARCHAR[]" },
            { "entity_buckets_compacted",     "ALTER TABLE compaction_history ADD COLUMN entity_buckets_compacted INTEGER DEFAULT 0" },
            { "general_partitions_compacted", "ALTER TABLE compaction_history ADD COLUMN general_partitions_compacted INTEGER DEFAULT 0" },
            { "error_message",   "ALTER TABLE compaction_history ADD COLUMN error_message VARCHAR" },
        };
        for (String[] m : migrations) {
            try (Statement st = conn.createStatement()) {
                st.execute(m[1]);
                log.debug("compaction_history migration applied: {}", m[0]);
            } catch (SQLException e) {
                // Column already exists or DDL not supported — safe to skip
                log.debug("compaction_history migration skipped ({}): {}", m[0], e.getMessage());
            }
        }
    }

    /**
     * Adds {@code message_type VARCHAR} to every existing
     * {@code <catalog>.main.general_*} DuckLake table that was created before
     * this column was introduced.
     *
     * <p>Uses {@code duckdb_tables()} to discover tables, then issues
     * {@code ALTER TABLE … ADD COLUMN IF NOT EXISTS} for each. Errors per table
     * are swallowed (column may already exist, or DuckLake version may differ).
     */
    private void migrateGeneralCassetteTables(Connection conn, String catalog) {
        List<String> tableNames = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
             var rs = stmt.executeQuery(
                     "SELECT table_name FROM duckdb_tables()" +
                     " WHERE database_name = '" + catalog + "'" +
                     " AND schema_name = 'main'" +
                     " AND table_name LIKE 'general_%'")) {
            while (rs.next()) {
                tableNames.add(rs.getString("table_name"));
            }
        } catch (SQLException e) {
            log.warn("Could not list general cassette tables for migration: {}", e.getMessage());
            return;
        }

        for (String tableName : tableNames) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ALTER TABLE " + catalog + ".main." + tableName +
                             " ADD COLUMN IF NOT EXISTS message_type VARCHAR");
                log.debug("Migrated general cassette table {}.main.{}: added message_type", catalog, tableName);
            } catch (SQLException e) {
                log.debug("Migration skipped for {}.main.{} ({})", catalog, tableName, e.getMessage());
            }
        }
    }

    private void exec(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    /** Converts a topic/entity name to a safe SQL identifier: {@code [a-z0-9_]}. */
    public static String normalize(String name) {
        return name.toLowerCase().replaceAll("[^a-z0-9_]", "_");
    }

    /** Whether VARIANT type was confirmed to round-trip through DuckLake Parquet. */
    public boolean isVariantSupported() {
        return variantSupported;
    }
}
