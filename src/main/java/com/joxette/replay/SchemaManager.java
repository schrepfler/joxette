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
 * <strong>Legacy schema manager – superseded by {@code com.joxette.db.SchemaManager}.</strong>
 *
 * <p>This class is kept as a plain class (not a Spring bean) so that
 * {@link com.joxette.management.EntityController} and
 * {@link com.joxette.compaction.CompactionService} can still call
 * {@link #createEntityTable} and {@link #dropEntityTable} via the
 * {@code com.joxette.db.SchemaManager} subtype.  The {@code @PostConstruct}
 * initialisation is intentionally disabled: all DDL is now handled by
 * {@code com.joxette.db.SchemaManager}.
 *
 * <p>The old {@code CREATE SCHEMA IF NOT EXISTS lake} call caused an
 * "Ambiguous reference" error once the DuckLake catalog named {@code lake}
 * was attached, because DuckDB cannot distinguish a schema named {@code lake}
 * inside the main database from the attached DuckLake catalog of the same name.
 *
 * @deprecated Use {@code com.joxette.db.SchemaManager} directly.
 */
@Deprecated(since = "DuckLake migration", forRemoval = true)
// NOT annotated with @Component – Spring must not instantiate this class.
// The bean named "schemaManager" is now com.joxette.db.SchemaManager (bean id "dbSchemaManager").
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
                createRetentionHistoryTable(st);
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
                    topic          VARCHAR NOT NULL PRIMARY KEY,
                    mode           VARCHAR NOT NULL DEFAULT 'general',
                    paused         BOOLEAN NOT NULL DEFAULT FALSE,
                    retention_days INTEGER
                )
                """);
        // Idempotent column addition for pre-existing databases
        st.execute("ALTER TABLE lake.config_topics ADD COLUMN IF NOT EXISTS retention_days INTEGER");
        st.execute("""
                CREATE TABLE IF NOT EXISTS lake.config_entities (
                    entity_type    VARCHAR NOT NULL PRIMARY KEY,
                    buckets        INTEGER NOT NULL DEFAULT 256,
                    retention_days INTEGER
                )
                """);
        st.execute("ALTER TABLE lake.config_entities ADD COLUMN IF NOT EXISTS retention_days INTEGER");
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

    private static void createRetentionHistoryTable(Statement st) throws SQLException {
        st.execute("CREATE SEQUENCE IF NOT EXISTS lake.retention_history_id_seq START 1");
        st.execute("""
                CREATE TABLE IF NOT EXISTS lake.retention_history (
                    id                     BIGINT      DEFAULT nextval('lake.retention_history_id_seq') PRIMARY KEY,
                    started_at             TIMESTAMPTZ NOT NULL,
                    completed_at           TIMESTAMPTZ,
                    status                 VARCHAR     NOT NULL,
                    triggered_by           VARCHAR     NOT NULL,
                    entity_rows_deleted    BIGINT      NOT NULL DEFAULT 0,
                    general_rows_deleted   BIGINT      NOT NULL DEFAULT 0,
                    known_entities_deleted BIGINT      NOT NULL DEFAULT 0,
                    error_message          VARCHAR
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
