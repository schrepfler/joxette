package com.joxette.config;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Provides the single shared DuckDB {@link Connection} bean.
 *
 * <p>DuckDB is embedded and single-process. All application threads (Kafka
 * consumers, REST handlers, compaction) share this one connection.  DuckDB
 * serialises writes internally, so no external locking is required.  Multiple
 * concurrent reads are safe via separate {@code Statement} objects created from
 * the shared connection.
 *
 * <p>If multi-process access is needed in the future, swap the DuckLake catalog
 * backend to PostgreSQL — the DuckLake schema is identical; only this bean
 * changes.
 */
@Configuration
public class DuckDBConfig {

    /**
     * Opens (or creates) the main DuckDB database file used for config tables and jOOQ.
     *
     * <p>This file is intentionally kept <em>separate</em> from the DuckLake catalog
     * file ({@code joxette.catalog.path}).  DuckDB does not allow a file that is already
     * open as the main database to be ATTACH-ed as a named catalog; using the same file
     * for both would cause a "Unique file handle conflict" error at startup.
     *
     * <p>The main DB path is derived from the catalog path by replacing the
     * {@code .ducklake} extension (if present) with {@code .db}.  This keeps both
     * files together in the same directory without any additional configuration.
     *
     * <p>The connection is closed by Spring's {@code @Bean(destroyMethod)} on
     * application shutdown, giving DuckDB a chance to flush WAL data.
     */
    @Bean(destroyMethod = "close")
    public Connection duckDbConnection(JoxetteProperties properties) throws SQLException {
        String catalogPath = properties.getCatalog().getPath();

        // For in-memory databases (e.g. integration tests), use the connection as-is.
        // DuckLakeManager will also use an in-memory ATTACH in this case.
        if (":memory:".equals(catalogPath)) {
            return DriverManager.getConnection("jdbc:duckdb:");
        }

        // Derive a sibling .db file so the DuckLake .ducklake file can be ATTACHed separately.
        // DuckDB does not allow a file that is already open as the main database to be
        // ATTACH-ed as a named catalog; both files must be distinct.
        String mainDbPath = catalogPath.replaceAll("\\.ducklake$", "") + ".db";

        // Ensure parent directories exist before DuckDB tries to open the file.
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
     * (e.g. {@code TIMESTAMPTZ}, {@code BLOB}, array/struct literals) and
     * correct handling of DuckDB's sequence and type system in generated queries.
     *
     * <p>The context is stateless and safe to share across threads; all
     * mutable state (transactions, cursors) lives in the {@link Connection}
     * which DuckDB serialises internally.
     */
    @Bean
    public DSLContext dslContext(Connection duckDbConnection) {
        return DSL.using(duckDbConnection, SQLDialect.DUCKDB);
    }
}
