package com.joxette.config;

import com.joxette.db.CatalogBackend;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Provides the single shared DuckDB {@link Connection} bean.
 *
 * <p>DuckDB always runs embedded in-process as the query engine, regardless of
 * which DuckLake catalog backend is active.  The catalog backend (embedded file,
 * Quack server, or PostgreSQL) is determined solely from the URI scheme of
 * {@code joxette.catalog.path} and affects only the {@code ATTACH} statement
 * issued by {@link com.joxette.db.DuckLakeManager} — <strong>not</strong> this
 * connection.
 *
 * <h2>Connection strategy by backend</h2>
 * <dl>
 *   <dt>{@code :memory:}</dt>
 *   <dd>In-memory DuckDB — ephemeral; used only in unit and integration tests.</dd>
 *
 *   <dt>{@link CatalogBackend#EMBEDDED_DUCKDB} (local file)</dt>
 *   <dd>Opens a sibling {@code .db} file derived from the catalog path
 *       (e.g. {@code ./data/joxette.ducklake} → {@code ./data/joxette.db}).
 *       DuckDB does not allow a file already open as the main database to be
 *       re-ATTACHed as a named catalog, so the two files must be distinct.</dd>
 *
 *   <dt>{@link CatalogBackend#QUACK} / {@link CatalogBackend#POSTGRESQL}</dt>
 *   <dd>Opens {@value #REMOTE_BACKEND_CONFIG_DB_PATH} for the config-table schema
 *       ({@code topic_configs}, {@code entity_type_configs}, etc.).  These tables
 *       are local to each Joxette process and are never stored in the remote
 *       DuckLake catalog.  The default path matches the file that Stage 1
 *       (embedded DuckDB with default settings) would have created, so a Stage
 *       1 → Stage 2 migration requires no extra file-copy step when the default
 *       catalog path is used.</dd>
 * </dl>
 *
 * <p>DuckDB serialises writes internally; no external locking is required.
 * Multiple concurrent reads are safe via separate {@code Statement} objects.
 */
@Configuration
public class DuckDBConfig {

    private static final Logger log = LoggerFactory.getLogger(DuckDBConfig.class);

    /**
     * Local config-table database used when the DuckLake catalog backend is
     * remote (Quack or PostgreSQL).
     *
     * <p>Matches the file produced by a default Stage 1 deployment
     * ({@code ./data/joxette.ducklake} → {@code ./data/joxette.db}), so
     * Stage 1 → Stage 2 migration with default settings is seamless.
     */
    public static final String REMOTE_BACKEND_CONFIG_DB_PATH = "./data/joxette.db";

    /**
     * Opens (or creates) the main DuckDB database file used for config tables and jOOQ.
     *
     * <p>The connection is closed by Spring's {@code @Bean(destroyMethod)} on
     * application shutdown, giving DuckDB a chance to flush WAL data.
     */
    @Bean(destroyMethod = "close")
    public Connection duckDbConnection(JoxetteProperties properties) throws SQLException {
        String catalogPath = properties.getCatalog().getPath();

        // Special case: in-memory DuckDB (unit tests / ephemeral mode).
        if (":memory:".equals(catalogPath)) {
            log.debug("Opening in-memory DuckDB connection (catalog path is ':memory:')");
            return DriverManager.getConnection("jdbc:duckdb:");
        }

        CatalogBackend backend = CatalogBackend.detect(catalogPath);
        String mainDbPath = resolveMainDbPath(catalogPath, backend);

        log.info("DuckDB config-table database: '{}' (catalog backend: {})", mainDbPath, backend);

        // Ensure the parent directory exists before DuckDB tries to open the file.
        Path parent = Path.of(mainDbPath).getParent();
        if (parent != null) {
            parent.toFile().mkdirs();
        }

        return DriverManager.getConnection("jdbc:duckdb:" + mainDbPath);
    }

    /**
     * jOOQ {@link DSLContext} backed by the shared DuckDB connection.
     *
     * <p>{@link SQLDialect#DUCKDB} enables DuckDB-specific SQL rendering
     * (e.g. {@code TIMESTAMPTZ}, {@code BLOB}, array/struct literals) and correct
     * handling of DuckDB's sequence and type system in generated queries.
     */
    @Bean
    public DSLContext dslContext(Connection duckDbConnection) {
        return DSL.using(duckDbConnection, SQLDialect.DUCKDB);
    }

    /**
     * Resolves the local DuckDB file path for the config-table schema.
     *
     * <ul>
     *   <li>{@link CatalogBackend#EMBEDDED_DUCKDB}: strips {@code .ducklake} and appends
     *       {@code .db}, keeping both files in the same directory without extra config.</li>
     *   <li>{@link CatalogBackend#QUACK} / {@link CatalogBackend#POSTGRESQL}: returns
     *       {@value #REMOTE_BACKEND_CONFIG_DB_PATH}.</li>
     * </ul>
     */
    public static String resolveMainDbPath(String catalogPath, CatalogBackend backend) {
        return switch (backend) {
            case EMBEDDED_DUCKDB -> catalogPath.replaceAll("\\.ducklake$", "") + ".db";
            case QUACK, POSTGRESQL -> REMOTE_BACKEND_CONFIG_DB_PATH;
        };
    }
}
