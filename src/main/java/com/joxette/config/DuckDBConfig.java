package com.joxette.config;

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
     * Opens (or creates) the DuckDB database file at the path configured by
     * {@code joxette.catalog.path}.
     *
     * <p>The connection is closed by Spring's {@code @Bean(destroyMethod)} on
     * application shutdown, giving DuckDB a chance to flush WAL and inlined data.
     */
    @Bean(destroyMethod = "close")
    public Connection duckDbConnection(JoxetteProperties properties) throws SQLException {
        String catalogPath = properties.getCatalog().getPath();

        // Ensure parent directories exist before DuckDB tries to open the file.
        Path parent = Path.of(catalogPath).getParent();
        if (parent != null) {
            parent.toFile().mkdirs();
        }

        return DriverManager.getConnection("jdbc:duckdb:" + catalogPath);
    }
}
