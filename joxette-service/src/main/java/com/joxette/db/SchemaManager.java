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
     * Tests whether VARIANT survives a full DuckLake write/read round-trip through Parquet.
     *
     * <p>Steps:
     * <ol>
     *   <li>Verify the catalog is attached (guard against silent ATTACH failures).</li>
     *   <li>Disable DuckLake data inlining ({@code data_inlining_row_limit=0}) so the probe
     *       row is serialised to a Parquet file, not buffered in the catalog database.
     *       This is the critical difference from a plain in-memory test: the VARIANT binary
     *       encoding must survive the Parquet write/read cycle.</li>
     *   <li>Create a one-column probe table, insert a JSON value cast to {@code VARIANT},
     *       read it back, and assert that the payload is intact.</li>
     *   <li>Always drop the probe table and restore the original inlining limit in
     *       {@code finally}, even when an exception aborts earlier steps.</li>
     * </ol>
     *
     * <p>Returns {@code false} on any failure — including VARIANT not being recognised,
     * Parquet serialisation failing, or the value changing during the round-trip.
     *
     * <p><b>Observed behaviour (duckdb_jdbc 1.5.3.0 + current ducklake extension):</b>
     * VARIANT is supported and survives the Parquet round-trip correctly, including the
     * two fixes shipped in 1.5.3: correct VARIANT selection-vector indexing and correct
     * small-decimal Parquet encoding (e.g. {@code 0.01}, {@code 99.50}, {@code 1234567.89}).
     * The {@code metadata} column in cassette tables is therefore created as {@code VARIANT},
     * enabling DuckDB's shredded JSON encoding for up to 100× faster analytical queries.
     * Should a future DuckLake build regress on Parquet VARIANT serialisation, the probe
     * will automatically fall back to the {@code JSON} type with no schema change required.
     */
    private boolean probeVariant(Connection conn, String catalog) {
        // Guard: verify the catalog is actually attached before issuing any DDL against it.
        if (!isCatalogAttached(conn, catalog)) {
            log.warn("DuckLake catalog '{}' is not attached; skipping VARIANT probe, falling back to JSON", catalog);
            return false;
        }

        String probeTable = catalog + ".main.__variant_probe";
        boolean inliningDisabled = false;
        try {
            exec(conn, "DROP TABLE IF EXISTS " + probeTable);

            // Disable inlining so the probe row goes to Parquet rather than the catalog DB.
            // This is a DuckLake 1.0+ feature; on older builds the call fails silently and
            // the probe runs against inlined data (still catches VARIANT parse errors).
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CALL " + catalog + ".set_option('data_inlining_row_limit', 0)");
                inliningDisabled = true;
                log.debug("VARIANT probe: inlining disabled to force Parquet serialisation path");
            } catch (SQLException e) {
                log.debug("VARIANT probe: could not disable inlining ({}); probe uses inlined path", e.getMessage());
            }

            exec(conn, "CREATE TABLE " + probeTable + " (v VARIANT)");
            exec(conn, "INSERT INTO " + probeTable + " VALUES ('{\"probe\":true,\"x\":1}'::VARIANT)");

            String roundTripped;
            try (Statement stmt = conn.createStatement();
                 var rs = stmt.executeQuery("SELECT v::VARCHAR FROM " + probeTable)) {
                if (!rs.next()) {
                    log.warn("VARIANT probe: no row returned after INSERT; falling back to JSON");
                    return false;
                }
                roundTripped = rs.getString(1);
            }

            // Assert that the JSON payload survives the round-trip intact.
            if (roundTripped == null || !roundTripped.contains("probe")) {
                log.warn("VARIANT probe: value did not round-trip correctly (got: {}); falling back to JSON",
                         roundTripped);
                return false;
            }

            log.debug("VARIANT probe: round-trip value = {}", roundTripped);
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
        } finally {
            // Drop the probe table regardless of outcome — DDL in DuckDB auto-commits,
            // so it persists even after a rollback() in the catch block above.
            try {
                exec(conn, "DROP TABLE IF EXISTS " + probeTable);
            } catch (SQLException e) {
                log.debug("Could not drop VARIANT probe table on cleanup: {}", e.getMessage());
            }
            // Vacuum to physically delete the probe's Parquet files from object storage.
            // Without this, DROP TABLE only removes the catalog entry; the files accumulate
            // across restarts until the next scheduled compaction/vacuum run.
            try {
                exec(conn, "CALL " + catalog + ".ducklake_vacuum()");
                log.debug("VARIANT probe: vacuum complete, Parquet files removed");
            } catch (SQLException e) {
                log.debug("Could not vacuum after VARIANT probe drop ({}); files will be cleaned by next compaction", e.getMessage());
            }
            // Restore inlining to the user-configured limit (or DuckLake's default of 10).
            if (inliningDisabled) {
                restoreInliningAfterProbe(conn, catalog);
            }
        }
    }

    /**
     * Restores {@code data_inlining_row_limit} after the VARIANT probe's temporary override.
     * Re-applies {@code joxette.catalog.inlining-row-limit} when explicitly configured, or
     * falls back to DuckLake's built-in default of 10 rows.
     */
    private void restoreInliningAfterProbe(Connection conn, String catalog) {
        Integer configured = properties.getCatalog().getInliningRowLimit();
        int restoreTo = configured != null ? configured : 10;
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CALL " + catalog + ".set_option('data_inlining_row_limit', " + restoreTo + ")");
            log.debug("VARIANT probe: inlining row limit restored to {}", restoreTo);
        } catch (SQLException e) {
            log.debug("Could not restore inlining limit after VARIANT probe ({})", e.getMessage());
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
                    files_processed BIGINT  NOT NULL DEFAULT 0,
                    files_created   BIGINT  NOT NULL DEFAULT 0,
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

            // Distributed compaction lock table — one row per in-progress target.
            // Shared across Joxette instances when using an external catalog (PostgreSQL, Quack).
            // In embedded-DuckDB mode it provides intra-process protection as a bonus.
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS compaction_locks (
                    target      VARCHAR     NOT NULL PRIMARY KEY,
                    instance_id VARCHAR     NOT NULL,
                    acquired_at TIMESTAMPTZ NOT NULL,
                    expires_at  TIMESTAMPTZ NOT NULL
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

            // Named transform pipeline presets (plain DuckDB, not DuckLake).
            // Steps are stored as a JSON array that is deserialised at read time.
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS transform_presets (
                    name        VARCHAR     NOT NULL PRIMARY KEY,
                    description VARCHAR,
                    steps       JSON        NOT NULL,
                    fragments   JSON,
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """);
            stmt.execute("ALTER TABLE transform_presets ADD COLUMN IF NOT EXISTS fragments JSON");

            // Named derived stream definitions (plain DuckDB, not DuckLake).
            // The full StreamDefinition is stored as a JSON blob in `definition`
            // so the schema stays stable as new fields are added.
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS stream_definitions (
                    id          VARCHAR     NOT NULL PRIMARY KEY
                                  CHECK (id ~ '^[a-z][a-z0-9_-]*$'),
                    name        VARCHAR     NOT NULL,
                    entity_type VARCHAR     NOT NULL,
                    entity_id   VARCHAR,
                    definition  JSON        NOT NULL,
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """);

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS export_jobs (
                    id              VARCHAR     NOT NULL PRIMARY KEY,
                    entity_type     VARCHAR     NOT NULL,
                    entity_ids      VARCHAR[]   NOT NULL,
                    from_ts         TIMESTAMPTZ,
                    to_ts           TIMESTAMPTZ,
                    message_types   VARCHAR[],
                    output_format   VARCHAR     NOT NULL
                                      CHECK (output_format IN ('parquet', 'ndjson')),
                    status          VARCHAR     NOT NULL DEFAULT 'pending'
                                      CHECK (status IN ('pending', 'running', 'completed', 'failed')),
                    output_path     VARCHAR,
                    row_count       BIGINT,
                    error_message   VARCHAR,
                    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                    started_at      TIMESTAMPTZ,
                    completed_at    TIMESTAMPTZ
                )
                """);

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS broker_configs (
                    broker_id               VARCHAR PRIMARY KEY
                        CHECK (broker_id ~ '^[a-z][a-z0-9_-]*$'),
                    bootstrap_servers       VARCHAR NOT NULL,
                    security_protocol       VARCHAR NOT NULL DEFAULT 'PLAINTEXT'
                        CHECK (security_protocol IN ('PLAINTEXT','SASL_PLAINTEXT','SASL_SSL','SSL')),
                    sasl_mechanism          VARCHAR,
                    sasl_username           VARCHAR,
                    sasl_password           VARCHAR,
                    ssl_truststore_path     VARCHAR,
                    ssl_truststore_password VARCHAR,
                    ssl_keystore_path       VARCHAR,
                    ssl_keystore_password   VARCHAR,
                    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """);

            stmt.execute("ALTER TABLE topic_configs ADD COLUMN IF NOT EXISTS broker_id VARCHAR");

            // Instance registry: tracks running Joxette processes for cluster-wide observability.
            // In embedded DuckDB mode this table holds one row; in Quack/PostgreSQL mode it is
            // shared across all processes.  Rows are reaped on startup and deleted on clean shutdown.
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS joxette_instances (
                    instance_id      VARCHAR     NOT NULL PRIMARY KEY,
                    roles            VARCHAR[]   NOT NULL,
                    catalog_backend  VARCHAR     NOT NULL,
                    started_at       TIMESTAMPTZ NOT NULL,
                    last_heartbeat   TIMESTAMPTZ NOT NULL,
                    kafka_assignments JSON
                )
                """);

            log.debug("Config tables ready");
        }

        // Migration: add columns introduced after the initial schema version.
        // Must run AFTER the outer Statement is closed — DuckDB does not allow two
        // active statements on the same connection simultaneously; opening a second
        // Statement while the outer try-with-resources Statement is still open leaves
        // the connection in a "pending query" state and silently drops the ALTER TABLE.
        migrateCompactionHistory(conn);
        migrateKnownEntities(conn);
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
        int topicTableCount = 0;
        if (topics != null) {
            for (TopicEntry t : topics) {
                String mode = t.getMode();
                if ("general".equals(mode) || "both".equals(mode)) {
                    String tbl = "general_" + normalize(t.getTopic());
                    createGeneralCassetteTable(conn, catalog, tbl, flexType);
                    topicTableCount++;
                }
            }
        }

        List<EntityEntry> entities = properties.getBootstrap().getEntities();
        int entityTableCount = 0;
        if (entities != null) {
            for (EntityEntry e : entities) {
                String tbl = "entity_" + normalize(e.getType());
                createEntityCassetteTable(conn, catalog, tbl, flexType);
                entityTableCount++;
            }
        }
        log.info("Bootstrap DuckLake tables: {} general cassette table(s), {} entity cassette table(s)",
                topicTableCount, entityTableCount);
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
        ensureTableSorted(conn, catalog, tableName,
                "(kafka_timestamp ASC, kafka_partition ASC, kafka_offset ASC)");
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
        ensureTableSorted(conn, catalog, tableName,
                "(entity_id ASC, kafka_timestamp ASC, recorded_at ASC)");
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
        Connection conn    = duckLakeManager.getConnection();
        String    catalog  = duckLakeManager.getCatalogName();
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
            throw com.joxette.api.error.ValidationException.field("entityType",
                "must match [a-z][a-z0-9_]* (got '%s')".formatted(type));
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
            { "triggered_by",    "ALTER TABLE compaction_history ADD COLUMN IF NOT EXISTS triggered_by VARCHAR DEFAULT 'unknown'" },
            { "targets",         "ALTER TABLE compaction_history ADD COLUMN IF NOT EXISTS targets VARCHAR[]" },
            { "entity_buckets_compacted",     "ALTER TABLE compaction_history ADD COLUMN IF NOT EXISTS entity_buckets_compacted INTEGER DEFAULT 0" },
            { "general_partitions_compacted", "ALTER TABLE compaction_history ADD COLUMN IF NOT EXISTS general_partitions_compacted INTEGER DEFAULT 0" },
            { "files_processed",  "ALTER TABLE compaction_history ADD COLUMN IF NOT EXISTS files_processed BIGINT DEFAULT 0" },
            { "files_created",    "ALTER TABLE compaction_history ADD COLUMN IF NOT EXISTS files_created BIGINT DEFAULT 0" },
            { "error_message",   "ALTER TABLE compaction_history ADD COLUMN IF NOT EXISTS error_message VARCHAR" },
        };
        for (String[] m : migrations) {
            try (Statement st = conn.createStatement()) {
                st.execute(m[1]);
                log.debug("compaction_history migration applied: {}", m[0]);
            } catch (SQLException e) {
                // DDL failed for an unexpected reason — reset connection state so subsequent
                // migrations are not poisoned by DuckDB's "pending query" error.
                log.warn("compaction_history migration failed ({}): {}", m[0], e.getMessage());
                try { conn.rollback(); } catch (SQLException re) {
                    log.debug("rollback after failed migration: {}", re.getMessage());
                }
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
    /**
     * Idempotent migration: adds {@code message_count}, {@code source_topics},
     * and {@code last_message_type} to {@code known_entities}.
     */
    private void migrateKnownEntities(Connection conn) {
        // DuckDB does not support NOT NULL or DEFAULT [] on ADD COLUMN for existing tables.
        // Nulls are handled defensively in KnownEntitiesRepository and mapEntityInfo.
        String[][] migrations = {
            { "message_count",      "ALTER TABLE known_entities ADD COLUMN IF NOT EXISTS message_count BIGINT DEFAULT 0" },
            { "source_topics",      "ALTER TABLE known_entities ADD COLUMN IF NOT EXISTS source_topics VARCHAR[]" },
            { "last_message_type",  "ALTER TABLE known_entities ADD COLUMN IF NOT EXISTS last_message_type VARCHAR" },
        };
        for (String[] m : migrations) {
            try (Statement st = conn.createStatement()) {
                st.execute(m[1]);
                log.debug("known_entities migration applied: {}", m[0]);
            } catch (SQLException e) {
                log.warn("known_entities migration failed ({}): {}", m[0], e.getMessage());
                try { conn.rollback(); } catch (SQLException re) {
                    log.debug("rollback after failed known_entities migration: {}", re.getMessage());
                }
            }
        }
    }

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

    /**
     * Applies {@code ALTER TABLE … SET SORTED BY} so DuckLake 1.0+ automatically
     * enforces the declared sort order during compaction and inline-data flush.
     *
     * <p>This is issued as a separate statement after {@code CREATE TABLE IF NOT EXISTS}
     * because DuckLake does not support {@code SORTED BY} inline in the CREATE DDL.
     * It is safe to run on every startup against already-sorted tables — DuckLake
     * treats a repeated {@code SET SORTED BY} with the same columns as a no-op.
     *
     * <p><b>Important:</b> {@code SET SORTED BY} does <em>not</em> retroactively
     * reorder existing rows.  Pre-existing data will be sorted by DuckLake on the
     * next compaction or flush pass.  This is the expected behaviour; no special
     * handling is needed in the application.
     *
     * <p>Failures are logged as warnings and swallowed so that an older DuckLake
     * version that does not yet support the statement cannot prevent startup.
     */
    static void ensureTableSorted(Connection conn, String catalog,
                                   String tableName, String sortedBy) {
        String sql = "ALTER TABLE " + catalog + ".main." + tableName
                     + " SET SORTED BY " + sortedBy;
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            log.debug("SORTED BY applied to {}.main.{}: {}", catalog, tableName, sortedBy);
        } catch (SQLException e) {
            log.warn("Could not apply SORTED BY to {}.main.{} ({}); " +
                     "DuckLake will not enforce sort order automatically for this table",
                     catalog, tableName, e.getMessage());
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
