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
@Component
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
        createLakeTables(conn, catalog);

        log.info("Schema initialisation complete");
    }

    // -------------------------------------------------------------------------
    // VARIANT probe
    // -------------------------------------------------------------------------

    /**
     * Tests whether VARIANT survives a full DuckLake write/read round-trip.
     * Creates a temporary DuckLake probe table, inserts one row, reads it back,
     * and drops the table.  Returns {@code false} on any failure.
     */
    private boolean probeVariant(Connection conn, String catalog) {
        String probeTable = catalog + ".main.__variant_probe";
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + probeTable + " (v VARIANT)");
            stmt.execute("INSERT INTO " + probeTable + " VALUES ('{\"probe\":true}'::VARIANT)");
            stmt.execute("SELECT v FROM " + probeTable);
            stmt.execute("DROP TABLE " + probeTable);
            return true;
        } catch (SQLException e) {
            log.warn("VARIANT probe failed ({}); falling back to JSON for flexible columns", e.getMessage());
            try (Statement cleanup = conn.createStatement()) {
                cleanup.execute("DROP TABLE IF EXISTS " + probeTable);
            } catch (SQLException ignored) {}
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
                    topic      VARCHAR PRIMARY KEY,
                    mode       VARCHAR NOT NULL
                                 CHECK (mode IN ('general', 'entity_only', 'both')),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """);

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS entity_type_configs (
                    entity_type  VARCHAR PRIMARY KEY,
                    bucket_count INTEGER NOT NULL DEFAULT 256,
                    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """);

            stmt.execute("CREATE SEQUENCE IF NOT EXISTS seq_entity_source_mappings START 1");

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS entity_source_mappings (
                    id                   INTEGER PRIMARY KEY
                                           DEFAULT nextval('seq_entity_source_mappings'),
                    entity_type          VARCHAR NOT NULL,
                    topic                VARCHAR NOT NULL,
                    entity_id_source     VARCHAR NOT NULL
                                           CHECK (entity_id_source IN ('key', 'value', 'headers')),
                    entity_id_expression VARCHAR NOT NULL,
                    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """);

            stmt.execute("CREATE SEQUENCE IF NOT EXISTS seq_compaction_history START 1");

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS compaction_history (
                    id           INTEGER PRIMARY KEY
                                   DEFAULT nextval('seq_compaction_history'),
                    table_name   VARCHAR NOT NULL,
                    started_at   TIMESTAMPTZ NOT NULL,
                    completed_at TIMESTAMPTZ,
                    files_before INTEGER,
                    files_after  INTEGER,
                    bytes_before BIGINT,
                    bytes_after  BIGINT,
                    status       VARCHAR NOT NULL
                                   CHECK (status IN ('running', 'completed', 'failed'))
                )
                """);

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

        createKnownEntitiesTable(conn, catalog);

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
     * Stores all entities seen across all entity types.
     * Append-only (no primary key) because DuckLake tables are columnar.
     * Deduplication is handled at query time or via compaction.
     */
    private void createKnownEntitiesTable(Connection conn, String catalog) throws SQLException {
        exec(conn, String.format("""
            CREATE TABLE IF NOT EXISTS %s.main.known_entities (
                entity_type VARCHAR NOT NULL,
                entity_id   VARCHAR NOT NULL,
                bucket      INTEGER NOT NULL,
                first_seen  TIMESTAMPTZ NOT NULL,
                last_seen   TIMESTAMPTZ NOT NULL
            )
            """, catalog));
        log.debug("DuckLake table ready: {}.main.known_entities", catalog);
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
                headers         STRUCT(key VARCHAR, value BLOB)[]
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
                topic           VARCHAR     NOT NULL,
                kafka_offset    BIGINT      NOT NULL,
                kafka_partition INTEGER     NOT NULL,
                kafka_timestamp TIMESTAMPTZ NOT NULL,
                kafka_key       BLOB,
                kafka_value     BLOB,
                kafka_value_str VARCHAR,
                metadata        %s,
                headers         STRUCT(key VARCHAR, value BLOB)[]
            )
            """, catalog, tableName, flexType));
        log.debug("DuckLake table ready: {}.main.{}", catalog, tableName);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void exec(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    /** Converts a topic/entity name to a safe SQL identifier: {@code [a-z0-9_]}. */
    static String normalize(String name) {
        return name.toLowerCase().replaceAll("[^a-z0-9_]", "_");
    }

    /** Whether VARIANT type was confirmed to round-trip through DuckLake Parquet. */
    public boolean isVariantSupported() {
        return variantSupported;
    }
}
