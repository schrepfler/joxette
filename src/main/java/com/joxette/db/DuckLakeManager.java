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
        }
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("LOAD ducklake");
        }

        configureS3Secret();

        String catalogPath = properties.getCatalog().getPath();

        if (":memory:".equals(catalogPath)) {
            // In-memory catalog: metadata is ephemeral. Data path may be in-memory (unit
            // tests) or S3 (integration tests that wire a MinIO container via
            // @DynamicPropertySource + joxette.catalog.object-storage-path).
            String dataPath = resolveDataPath(catalogPath);
            boolean usesS3  = dataPath != null && dataPath.startsWith("s3://");
            String effectiveDataPath = usesS3 ? dataPath : ":memory:";
            log.info("Attaching in-memory DuckLake catalog '{}' (DATA_PATH='{}')", CATALOG_NAME, effectiveDataPath);
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(String.format(
                    "ATTACH 'ducklake::memory:' AS %s (DATA_PATH '%s')",
                    CATALOG_NAME, effectiveDataPath));
                log.info("In-memory DuckLake catalog '{}' ready", CATALOG_NAME);
            } catch (SQLException e) {
                if (isAlreadyAttached(e)) {
                    log.warn("In-memory DuckLake catalog '{}' already attached. Error: {}", CATALOG_NAME, e.getMessage());
                } else {
                    throw e;
                }
            }
            return;
        }

        String dataPath = resolveDataPath(catalogPath);

        log.info("Attaching DuckLake catalog at '{}' with DATA_PATH '{}'", catalogPath, dataPath);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s', OVERRIDE_DATA_PATH TRUE)",
                catalogPath, CATALOG_NAME, dataPath));
            log.info("DuckLake catalog '{}' ready (data path: {})", CATALOG_NAME, dataPath);
        } catch (SQLException e) {
            if (isAlreadyAttached(e)) {
                log.warn("DuckLake catalog '{}' reported as already attached – verify it is visible via duckdb_databases(). Error was: {}",
                    CATALOG_NAME, e.getMessage());
            } else {
                throw e;
            }
        }

        if (properties.getCatalog().isAutoMigrate()) {
            runCatalogMigration();
        } else {
            log.info("DuckLake catalog auto-migration disabled (joxette.catalog.auto-migrate=false)");
        }
    }

    /**
     * Calls {@code ducklake_migrate()} to apply any pending catalog schema changes
     * introduced by a DuckLake extension upgrade (DuckLake 1.0+).
     *
     * <p>Safe to call when the catalog is already current — it is a no-op in that case.
     *
     * @throws IllegalStateException if migration fails, to prevent startup with a
     *     potentially incompatible catalog schema.
     */
    private void runCatalogMigration() {
        log.info("Running DuckLake catalog migration check...");
        try (Statement stmt = connection.createStatement()) {
            boolean hasResultSet = stmt.execute("CALL ducklake_migrate('" + CATALOG_NAME + "')");
            if (hasResultSet) {
                try (var rs = stmt.getResultSet()) {
                    int count = 0;
                    while (rs.next()) {
                        count++;
                    }
                    if (count == 0) {
                        log.info("DuckLake catalog migration: no changes needed");
                    } else {
                        log.info("DuckLake catalog migration: applied {} change(s)", count);
                    }
                }
            } else {
                log.info("DuckLake catalog migration: completed");
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                "DuckLake catalog migration failed — check DuckLake version compatibility. " +
                "Set joxette.catalog.auto-migrate=false to skip and investigate manually.", e);
        }
    }

    /**
     * Configures a DuckDB S3 secret when an explicit endpoint override is set.
     *
     * <p>For local S3-compatible servers (RustFS, MinIO) we must disable TLS and
     * switch to path-style URLs.  For production AWS the secret is omitted so
     * DuckDB falls back to its built-in credential chain.
     */
    private void configureS3Secret() throws SQLException {
        JoxetteProperties.S3 s3 = properties.getS3();
        if (!s3.hasEndpoint()) {
            log.debug("No S3 endpoint override configured – using DuckDB default credential chain");
            return;
        }

        log.info("Configuring DuckDB S3 secret for endpoint '{}'", s3.getEndpoint());

        // Drop any pre-existing secret so re-starts don't fail on "already exists".
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP SECRET IF EXISTS joxette_s3");
        }

        String sql = String.format(
            "CREATE SECRET joxette_s3 (" +
            "  TYPE S3," +
            "  KEY_ID '%s'," +
            "  SECRET '%s'," +
            "  REGION '%s'," +
            "  ENDPOINT '%s'," +
            "  USE_SSL false," +
            "  URL_STYLE 'path'" +
            ")",
            s3.getAccessKey(),
            s3.getSecretKey(),
            s3.getRegion(),
            // DuckDB expects host:port without the http:// scheme
            s3.getEndpoint().replaceFirst("https?://", "")
        );

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            log.info("DuckDB S3 secret 'joxette_s3' configured (endpoint: {})", s3.getEndpoint());
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

    /**
     * Returns {@code true} only for the specific DuckDB message that means the
     * catalog alias is already registered on this connection.  Deliberately narrow
     * to avoid silently swallowing real ATTACH failures (e.g. missing extension,
     * S3 credentials, corrupt file) whose messages also contain "already exists".
     */
    private boolean isAlreadyAttached(SQLException e) {
        String msg = e.getMessage();
        if (msg == null) return false;
        // DuckDB emits "already attached" when the same alias is attached twice.
        return msg.contains("already attached");
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
