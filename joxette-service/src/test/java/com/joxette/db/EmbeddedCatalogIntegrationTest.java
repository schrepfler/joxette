package com.joxette.db;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.joxette.config.JoxetteProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for the <strong>embedded DuckDB</strong> catalog backend path
 * ({@link CatalogBackend#EMBEDDED_DUCKDB}) exercised through {@link DuckLakeManager}.
 *
 * <p>All tests use an in-memory DuckDB connection with {@code joxette.catalog.path = :memory:},
 * which {@link CatalogBackend#detect(String)} maps to {@link CatalogBackend#EMBEDDED_DUCKDB}.
 * No real file system or S3 access is required.
 *
 * <p>This class focuses on backend-detection and startup-log correctness.
 * Detailed {@code ducklake_settings()}, migration, and inlining tests remain in
 * {@link DuckLakeManagerTest}.
 */
class EmbeddedCatalogIntegrationTest {

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    // -------------------------------------------------------------------------
    // Shared helpers
    // -------------------------------------------------------------------------

    private static JoxetteProperties memoryCatalogProperties() {
        var props = new JoxetteProperties();
        props.getCatalog().setPath(":memory:");
        props.getCatalog().setObjectStoragePath(null);
        props.getCatalog().setAutoMigrate(false);
        props.getCatalog().setMetadataQueryLogging(false);
        props.getCatalog().setInliningRowLimit(null);
        return props;
    }

    private static ListAppender<ILoggingEvent> attachListAppender(Class<?> loggerClass) {
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        ((Logger) LoggerFactory.getLogger(loggerClass)).addAppender(appender);
        return appender;
    }

    private static void detachListAppender(Class<?> loggerClass, ListAppender<ILoggingEvent> appender) {
        ((Logger) LoggerFactory.getLogger(loggerClass)).detachAppender(appender);
        appender.stop();
    }

    // -------------------------------------------------------------------------
    // Backend detection
    // -------------------------------------------------------------------------

    @Nested
    class BackendDetectionTests {

        @Test
        void memoryPath_detectsEmbeddedDuckDb() {
            assertThat(CatalogBackend.detect(":memory:"))
                .isEqualTo(CatalogBackend.EMBEDDED_DUCKDB);
        }

        @Test
        void defaultCatalogPath_detectsEmbeddedDuckDb() {
            // Default value configured in JoxetteProperties.Catalog
            assertThat(CatalogBackend.detect("./data/joxette.ducklake"))
                .isEqualTo(CatalogBackend.EMBEDDED_DUCKDB);
        }

        @Test
        void initialize_setsBackendToEmbeddedDuckDb() throws SQLException {
            var manager = new DuckLakeManager(conn, memoryCatalogProperties());
            manager.initialize();

            assertThat(manager.getBackend()).isEqualTo(CatalogBackend.EMBEDDED_DUCKDB);
        }

        @Test
        void initialize_logsEmbeddedDuckDbBackend() throws Exception {
            var appender = attachListAppender(DuckLakeManager.class);
            try {
                new DuckLakeManager(conn, memoryCatalogProperties()).initialize();

                assertThat(appender.list)
                    .extracting(ILoggingEvent::getFormattedMessage)
                    .anyMatch(msg -> msg.contains("EMBEDDED_DUCKDB"));
            } finally {
                detachListAppender(DuckLakeManager.class, appender);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Catalog ATTACH succeeds (startup health check)
    // -------------------------------------------------------------------------

    @Nested
    class StartupHealthTests {

        @Test
        void initialize_doesNotThrow_forEmbeddedBackend() {
            var manager = new DuckLakeManager(conn, memoryCatalogProperties());
            assertThatCode(manager::initialize).doesNotThrowAnyException();
        }

        @Test
        void initialize_catalogIsQueryableAfterAttach() throws SQLException {
            new DuckLakeManager(conn, memoryCatalogProperties()).initialize();

            // ducklake_settings() returns a row → catalog is attached and queryable.
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                     "SELECT * FROM ducklake_settings('" + DuckLakeManager.CATALOG_NAME + "')")) {
                assertThat(rs.next()).as("ducklake_settings() must return at least one row").isTrue();
                assertThat(rs.getString("extension_version")).isNotBlank();
            }
        }

        @Test
        void initialize_catalogNameIsAccessibleAsDatabaseAlias() throws SQLException {
            new DuckLakeManager(conn, memoryCatalogProperties()).initialize();

            // duckdb_databases() lists all attached databases; 'lake' must appear.
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                     "SELECT database_name FROM duckdb_databases() " +
                     "WHERE database_name = '" + DuckLakeManager.CATALOG_NAME + "'")) {
                assertThat(rs.next())
                    .as("catalog alias '" + DuckLakeManager.CATALOG_NAME + "' must appear in duckdb_databases()")
                    .isTrue();
            }
        }

        @Test
        void getBackend_returnsNonNull_afterInitialize() throws SQLException {
            var manager = new DuckLakeManager(conn, memoryCatalogProperties());
            manager.initialize();
            assertThat(manager.getBackend()).isNotNull();
        }

        @Test
        void getBackend_returnsNull_beforeInitialize() {
            var manager = new DuckLakeManager(conn, memoryCatalogProperties());
            // @PostConstruct not called yet
            assertThat(manager.getBackend()).isNull();
        }
    }

    // -------------------------------------------------------------------------
    // DuckDBConfig path resolution
    // -------------------------------------------------------------------------

    @Nested
    class DuckDBConfigPathResolutionTests {

        @Test
        void embeddedDuckdb_derivesDbFileSiblingToCatalog() {
            String path = com.joxette.config.DuckDBConfig.resolveMainDbPath(
                "./data/joxette.ducklake", CatalogBackend.EMBEDDED_DUCKDB);
            assertThat(path).isEqualTo("./data/joxette.db");
        }

        @Test
        void embeddedDuckdb_withoutExtension_appendsDb() {
            String path = com.joxette.config.DuckDBConfig.resolveMainDbPath(
                "./data/catalog", CatalogBackend.EMBEDDED_DUCKDB);
            assertThat(path).isEqualTo("./data/catalog.db");
        }

        @Test
        void quack_returnsRemoteBackendConfigDbPath() {
            String path = com.joxette.config.DuckDBConfig.resolveMainDbPath(
                "quack://quack-host:9999/joxette-catalog", CatalogBackend.QUACK);
            assertThat(path).isEqualTo(com.joxette.config.DuckDBConfig.REMOTE_BACKEND_CONFIG_DB_PATH);
        }

        @Test
        void postgresql_returnsRemoteBackendConfigDbPath() {
            String path = com.joxette.config.DuckDBConfig.resolveMainDbPath(
                "postgresql://pg-host:5432/joxette_catalog", CatalogBackend.POSTGRESQL);
            assertThat(path).isEqualTo(com.joxette.config.DuckDBConfig.REMOTE_BACKEND_CONFIG_DB_PATH);
        }

        @Test
        void remoteBackendConfigDbPath_matchesDefaultEmbeddedPath() {
            // Verify that the Stage-1→Stage-2 migration with default settings is seamless:
            // the embedded backend config DB and the remote backend fallback must be the same file.
            String embeddedDefault = com.joxette.config.DuckDBConfig.resolveMainDbPath(
                "./data/joxette.ducklake", CatalogBackend.EMBEDDED_DUCKDB);
            String remoteDefault   = com.joxette.config.DuckDBConfig.REMOTE_BACKEND_CONFIG_DB_PATH;
            assertThat(remoteDefault)
                .as("remote backend config DB must match default embedded DB path for seamless Stage 1→2 migration")
                .isEqualTo(embeddedDefault);
        }
    }
}
