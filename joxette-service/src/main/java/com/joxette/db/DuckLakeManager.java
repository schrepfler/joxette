package com.joxette.db;

import com.joxette.config.JoxetteProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Bootstraps and owns the DuckLake catalog attachment.
 *
 * <p>Runs once at startup to:
 * <ol>
 *   <li>Install and load the {@code ducklake} DuckDB extension.</li>
 *   <li>Detect the catalog backend from the URI scheme of
 *       {@code joxette.catalog.path} via {@link CatalogBackend#detect(String)}.</li>
 *   <li>ATTACH the DuckLake catalog using the unified form
 *       {@code ATTACH 'ducklake:<path>' AS lake (DATA_PATH '<object-storage-path>', …)},
 *       creating it if absent.  The same statement form works for all three backends
 *       ({@link CatalogBackend#EMBEDDED_DUCKDB}, {@link CatalogBackend#QUACK},
 *       {@link CatalogBackend#POSTGRESQL}) — only the value after {@code ducklake:}
 *       differs.</li>
 * </ol>
 *
 * <p>If ATTACH fails, the {@link PostConstruct} method throws, preventing the Spring
 * context from completing startup.  This acts as a hard startup health check: the
 * application never marks itself ready if the catalog is inaccessible.  The companion
 * {@link CatalogHealthIndicator} provides ongoing readiness signalling via
 * {@code /actuator/health/readiness}.
 *
 * <p>The single shared {@link Connection} is injected from {@code DuckDBConfig}.
 * All writes are serialised by DuckDB internally; no external locking is required.
 *
 * <h2>Backend transition</h2>
 * <p>Changing {@code joxette.catalog.path} from a file path to {@code quack://…} or
 * {@code postgresql://…} is the <em>only</em> change needed to switch backends.
 * See {@code docs/catalog-scaling.md} for the full migration runbook.
 */
@Component
public class DuckLakeManager {

    private static final Logger log = LoggerFactory.getLogger(DuckLakeManager.class);
    /** Receives DuckLakeMetadata log entries when metadata-query-logging is enabled. */
    private static final Logger duckLakeLog = LoggerFactory.getLogger("com.joxette.catalog.ducklake");

    /** Named catalog used in all DuckLake table references: {@code lake.main.<table>}. */
    public static final String CATALOG_NAME = "lake";

    private final Connection connection;
    private final JoxetteProperties properties;

    /** Detected at startup; {@code null} until {@link #initialize()} completes. */
    private CatalogBackend backend;

    public DuckLakeManager(Connection connection, JoxetteProperties properties) {
        this.connection = connection;
        this.properties = properties;
    }

    /**
     * Attaches the DuckLake catalog and runs startup checks.
     *
     * <p>Throws on any failure — the application will not start if the catalog
     * cannot be attached, ensuring k8s readiness probes never pass in a broken state.
     */
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

        // -----------------------------------------------------------------------
        // Detect and log the catalog backend.
        // -----------------------------------------------------------------------
        backend = CatalogBackend.detect(catalogPath);
        if (":memory:".equals(catalogPath)) {
            log.info("Catalog backend: {} (in-memory — ephemeral, tests only)", backend);
        } else {
            log.info("Catalog backend: {} (path/URI: '{}')", backend, catalogPath);
        }

        // -----------------------------------------------------------------------
        // In-memory catalog (tests / ephemeral mode).
        // -----------------------------------------------------------------------
        if (":memory:".equals(catalogPath)) {
            String dataPath = resolveDataPath(catalogPath);
            boolean usesS3  = dataPath != null && dataPath.startsWith("s3://");
            String effectiveDataPath = usesS3 ? dataPath : ":memory:";
            log.info("Attaching in-memory DuckLake catalog '{}' (DATA_PATH='{}')",
                CATALOG_NAME, effectiveDataPath);
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(String.format(
                    "ATTACH 'ducklake::memory:' AS %s (DATA_PATH '%s')",
                    CATALOG_NAME, effectiveDataPath));
                log.info("In-memory DuckLake catalog '{}' ready", CATALOG_NAME);
            } catch (SQLException e) {
                if (isAlreadyAttached(e)) {
                    log.warn("In-memory DuckLake catalog '{}' already attached. Error: {}",
                        CATALOG_NAME, e.getMessage());
                } else {
                    throw e;
                }
            }
            applyInliningRowLimit();
            logDuckLakeVersion();
            if (properties.getCatalog().isMetadataQueryLogging()) {
                enableMetadataLogging();
                drainMetadataLogs("startup");
            }
            return;
        }

        // -----------------------------------------------------------------------
        // Persistent catalog — unified ATTACH for all three backends.
        //
        //   EMBEDDED_DUCKDB : ATTACH 'ducklake:./data/joxette.ducklake' AS lake (…)
        //   QUACK            : ATTACH 'ducklake:quack://host:9999/catalog' AS lake (…)
        //   POSTGRESQL       : ATTACH 'ducklake:postgresql://host/db?…'  AS lake (…)
        //
        // The ATTACH string is identical in structure; only the path/URI after
        // 'ducklake:' differs.  No code changes are needed when switching backends.
        // -----------------------------------------------------------------------
        String dataPath = resolveDataPath(catalogPath);
        String autoMigrate = properties.getCatalog().isAutoMigrate() ? "TRUE" : "FALSE";

        log.info("Attaching DuckLake catalog ({}) at '{}' with DATA_PATH '{}' (AUTOMATIC_MIGRATION {})",
            backend, catalogPath, dataPath, autoMigrate);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s', OVERRIDE_DATA_PATH TRUE, AUTOMATIC_MIGRATION %s)",
                catalogPath, CATALOG_NAME, dataPath, autoMigrate));
            log.info("DuckLake catalog '{}' ready (backend: {}, data path: {})",
                CATALOG_NAME, backend, dataPath);
        } catch (SQLException e) {
            if (isAlreadyAttached(e)) {
                log.warn("DuckLake catalog '{}' reported as already attached – " +
                    "verify it is visible via duckdb_databases(). Error was: {}",
                    CATALOG_NAME, e.getMessage());
            } else {
                throw e;
            }
        }

        applyInliningRowLimit();
        logDuckLakeVersion();

        if (properties.getCatalog().isMetadataQueryLogging()) {
            enableMetadataLogging();
        }
        if (properties.getCatalog().isMetadataQueryLogging()) {
            drainMetadataLogs("startup");
        }
    }

    /**
     * Queries {@code ducklake_settings()} (DuckLake 1.0+) and logs the extension
     * version, catalog back-end type, and object-storage data path at INFO level.
     *
     * <p>Must be called after the {@code lake} catalog is ATTACHed.  Falls back to
     * a WARN if the function is unavailable (older DuckLake build).
     */
    private void logDuckLakeVersion() {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM ducklake_settings('" + CATALOG_NAME + "')")) {
            if (rs.next()) {
                String version     = rs.getString("extension_version");
                String catalogType = rs.getString("catalog_type");
                String dataPath    = rs.getString("data_path");
                log.info("DuckLake version={} catalog={} data_path={}", version, catalogType, dataPath);
            }
        } catch (SQLException e) {
            log.warn("Could not query ducklake_settings() — requires DuckLake 1.0+: {}", e.getMessage());
        }
    }

    /**
     * Applies the {@code data_inlining_row_limit} DuckLake option when
     * {@code joxette.catalog.inlining-row-limit} is explicitly configured.
     *
     * <p>Must be called after ATTACH so that the {@code lake} catalog alias is
     * already registered.  A value of {@code 0} disables inlining entirely;
     * any positive integer overrides DuckLake's built-in default of 10 rows.
     */
    private void applyInliningRowLimit() {
        Integer limit = properties.getCatalog().getInliningRowLimit();
        if (limit == null) {
            log.info("DuckLake inlining row limit: default (10)");
            return;
        }
        log.info("DuckLake inlining row limit: {} ({})",
            limit, limit == 0 ? "inlining disabled, always write Parquet" : "overrides default of 10");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(
                "CALL %s.set_option('data_inlining_row_limit', %d)", CATALOG_NAME, limit));
        } catch (SQLException e) {
            log.warn("Could not set DuckLake data_inlining_row_limit={} — " +
                "requires DuckLake 1.0+ (PR #923). Option ignored: {}", limit, e.getMessage());
        }
    }

    /**
     * Activates DuckLakeMetadata catalog query tracing via DuckDB's built-in logging.
     *
     * <p>Must be called after the {@code ducklake} extension has been ATTACHed so that the
     * {@code DuckLakeMetadata} log type is registered.
     */
    private void enableMetadataLogging() {
        log.info("Enabling DuckLakeMetadata query logging (com.joxette.catalog.ducklake)");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("SET logging_type = 'DuckLakeMetadata'");
            stmt.execute("SET logging_level = 'DEBUG'");
        } catch (SQLException e) {
            log.warn("Could not enable DuckLakeMetadata logging — " +
                "requires DuckLake 1.0+ and ducklake extension already loaded: {}", e.getMessage());
        }
    }

    /**
     * Drains all entries currently in DuckDB's in-memory log buffer and emits them
     * through the {@code com.joxette.catalog.ducklake} SLF4J logger at DEBUG level.
     *
     * @param phase label prepended to each log line (e.g. {@code "startup"}, {@code "compaction"})
     */
    public void drainMetadataLogs(String phase) {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT level, message FROM duckdb_logs() ORDER BY timestamp")) {
            int count = 0;
            while (rs.next()) {
                duckLakeLog.debug("[{}] [{}] {}", phase, rs.getString("level"), rs.getString("message"));
                count++;
            }
            if (count > 0) {
                log.debug("Drained {} DuckLakeMetadata log entries for phase '{}'", count, phase);
            }
        } catch (SQLException e) {
            log.warn("Failed to drain DuckLakeMetadata logs for phase '{}': {}", phase, e.getMessage());
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
            String msg = e.getMessage();
            if (msg != null && msg.contains("ducklake_migrate") && msg.contains("does not exist")) {
                log.warn("ducklake_migrate() not available in this DuckLake build — " +
                    "catalog schema migration skipped. Upgrade ducklake extension if needed.");
                return;
            }
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
     * catalog alias is already registered on this connection.
     */
    private boolean isAlreadyAttached(SQLException e) {
        String msg = e.getMessage();
        if (msg == null) return false;
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

    /**
     * Returns the detected catalog backend.
     *
     * <p>Available after {@link #initialize()} completes; returns {@code null} if
     * called before the {@code @PostConstruct} phase.
     */
    public CatalogBackend getBackend() {
        return backend;
    }
}
