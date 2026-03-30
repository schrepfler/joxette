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
 * lake.cassette          – general-mode messages (all topics that include general routing)
 * lake.entity_{type}     – per-entity-type cassette (one table per configured entity)
 * lake.known_entities    – registry of observed (entity_type, entity_id) pairs
 * </pre>
 *
 * <h2>Entity cassette ordering</h2>
 * <p>Entity cassette tables use {@code (entity_id, timestamp, recorded_at)} as
 * their primary key. {@code timestamp} is the primary sort dimension; {@code
 * recorded_at} acts as a tiebreaker for messages that share the same logical
 * timestamp (e.g. events replayed with the original timestamp).
 */
@Component
public class SchemaManager {

    /** Only lower-case letters, digits, and underscores are valid in entity type names. */
    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[a-z][a-z0-9_]*");

    private static final String HEADERS_TYPE = "LIST(STRUCT(key VARCHAR, value BLOB))";

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
        try (Statement st = duckDB.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS lake");
            createGeneralCassette(st);
            createKnownEntitiesRegistry(st);
            createEntityCassettes(st);
        }
    }

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
            String type = entity.getType();
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
    }

    /**
     * Guards against SQL injection in dynamically constructed table names.
     * Entity type names must match {@code [a-z][a-z0-9_]*}.
     */
    private static void validateEntityType(String type) {
        if (type == null || !SAFE_IDENTIFIER.matcher(type).matches()) {
            throw new IllegalArgumentException(
                    "Invalid entity type name '%s': must match [a-z][a-z0-9_]*".formatted(type));
        }
    }
}
