package com.joxette.db;

import com.joxette.config.JoxetteProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Bootstraps and owns the DuckLake catalog attachment.
 *
 * <p>Runs once at startup to:
 * <ol>
 *   <li>Install and load the {@code ducklake} DuckDB extension.</li>
 *   <li>ATTACH the DuckLake catalog from the path specified by
 *       {@code joxette.catalog.path}, creating it if absent.</li>
 * </ol>
 *
 * <p>The DuckLake extension stores its own metadata tables in a {@code ducklake}
 * schema within the same DuckDB file as the main connection.  Plain config tables
 * live in the {@code main} schema of that same file.  DuckLake-backed tables
 * (cassettes, known_entities) are accessed via the named catalog
 * {@value #CATALOG_NAME}.
 *
 * <p>The single shared {@link Connection} is injected from {@code DuckDBConfig}.
 * All writes are serialised by DuckDB internally; no external locking is needed.
 */
@Component
public class DuckLakeManager {

    private static final Logger log = LoggerFactory.getLogger(DuckLakeManager.class);

    /** Named catalog used in all DuckLake table references: {@code lake.main.<table>}. */
    public static final String CATALOG_NAME = "lake";

    private final Connection connection;
    private final JoxetteProperties properties;

    public DuckLakeManager(Connection connection, JoxetteProperties properties) {
        this.connection = connection;
        this.properties = properties;
    }

    @PostConstruct
    public void initialize() throws SQLException {
        log.info("Initializing DuckLake catalog...");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSTALL ducklake");
            stmt.execute("LOAD ducklake");

            String catalogPath = properties.getCatalog().getPath();
            String dataPath    = resolveDataPath(catalogPath);

            log.debug("Attaching DuckLake catalog at '{}' with DATA_PATH '{}'", catalogPath, dataPath);
            try {
                stmt.execute(String.format(
                    "ATTACH IF NOT EXISTS 'ducklake:%s' AS %s (DATA_PATH '%s')",
                    catalogPath, CATALOG_NAME, dataPath));
                log.info("DuckLake catalog '{}' ready (data path: {})", CATALOG_NAME, dataPath);
            } catch (SQLException e) {
                if (isAlreadyAttached(e)) {
                    log.debug("DuckLake catalog '{}' already attached, skipping", CATALOG_NAME);
                } else {
                    throw e;
                }
            }
        }
    }

    /**
     * Returns the configured object-storage path, or derives a sibling local
     * directory when no object-storage path is configured (dev/test mode).
     */
    private String resolveDataPath(String catalogPath) {
        String configured = properties.getCatalog().getObjectStoragePath();
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        // Strip .ducklake extension (if present) and append _data suffix.
        String base = catalogPath.replaceAll("\\.ducklake$", "");
        return base + "_data";
    }

    private boolean isAlreadyAttached(SQLException e) {
        String msg = e.getMessage();
        return msg != null && (msg.contains("already attached") || msg.contains("already exists"));
    }

    /** Returns the single shared DuckDB JDBC connection. */
    public Connection getConnection() {
        return connection;
    }

    /** Returns the DuckLake catalog name used in fully-qualified table references. */
    public String getCatalogName() {
        return CATALOG_NAME;
    }
}
