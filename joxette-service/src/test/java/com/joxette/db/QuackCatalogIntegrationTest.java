package com.joxette.db;

import com.joxette.config.JoxetteProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration test stub for the {@link CatalogBackend#QUACK} catalog backend.
 *
 * <p><strong>Skipped by default.</strong> All tests in this class are gated by the
 * {@code QUACK_URL} environment variable.  Set it to a live Quack server URI before
 * running to enable the suite:
 *
 * <pre>
 *   export QUACK_URL="quack://quack-host:9999/joxette-catalog"
 *   mvn -pl joxette-service test -Dtest=QuackCatalogIntegrationTest
 * </pre>
 *
 * <h2>Starting a local Quack server for manual testing</h2>
 * <ol>
 *   <li>Install DuckDB 1.5.3+ CLI.</li>
 *   <li>Start the Quack server:
 *     <pre>
 *       duckdb ./data/quack-test-catalog.duckdb \
 *         -c "INSTALL quack; LOAD quack; CALL quack_serve(port := 9999);"
 *     </pre>
 *   </li>
 *   <li>Export the variable and run the tests.</li>
 * </ol>
 *
 * <h2>Quack status</h2>
 * <p>Quack is <strong>beta</strong> in DuckDB 1.5.3. Re-evaluate production readiness
 * at DuckDB 2.0 GA. See {@code docs/catalog-scaling.md} for the migration runbook.
 */
@EnabledIfEnvironmentVariable(named = "QUACK_URL", matches = ".+",
    disabledReason = "Set QUACK_URL=quack://host:9999/catalog to run Quack integration tests")
class QuackCatalogIntegrationTest {

    private Connection conn;
    private String quackUrl;

    @BeforeEach
    void setUp() throws SQLException {
        quackUrl = System.getenv("QUACK_URL");
        conn = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private JoxetteProperties quackProperties() {
        var props = new JoxetteProperties();
        props.getCatalog().setPath(quackUrl);
        props.getCatalog().setObjectStoragePath(null);
        props.getCatalog().setAutoMigrate(false);
        props.getCatalog().setMetadataQueryLogging(false);
        props.getCatalog().setInliningRowLimit(null);
        return props;
    }

    // -------------------------------------------------------------------------
    // Backend detection
    // -------------------------------------------------------------------------

    @Test
    void quackUrl_detectsQuackBackend() {
        assertThat(CatalogBackend.detect(quackUrl)).isEqualTo(CatalogBackend.QUACK);
    }

    // -------------------------------------------------------------------------
    // Startup health check — ATTACH succeeds
    // -------------------------------------------------------------------------

    @Test
    void initialize_attachesQuackCatalogWithoutError() {
        var manager = new DuckLakeManager(conn, quackProperties());
        assertThatCode(manager::initialize).doesNotThrowAnyException();
    }

    @Test
    void initialize_setsBackendToQuack() throws SQLException {
        var manager = new DuckLakeManager(conn, quackProperties());
        manager.initialize();
        assertThat(manager.getBackend()).isEqualTo(CatalogBackend.QUACK);
    }

    @Test
    void initialize_catalogIsQueryableAfterAttach() throws SQLException {
        var manager = new DuckLakeManager(conn, quackProperties());
        manager.initialize();

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT * FROM ducklake_settings('" + DuckLakeManager.CATALOG_NAME + "')")) {
            assertThat(rs.next())
                .as("ducklake_settings() must return at least one row for Quack backend")
                .isTrue();
            assertThat(rs.getString("extension_version")).isNotBlank();
            // catalog_type should reflect a Quack-based catalog, not a plain file
            assertThat(rs.getString("catalog_type")).isNotBlank();
        }
    }

    @Test
    void initialize_catalogAliasAppearsInDuckDbDatabases() throws SQLException {
        var manager = new DuckLakeManager(conn, quackProperties());
        manager.initialize();

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT database_name FROM duckdb_databases() " +
                 "WHERE database_name = '" + DuckLakeManager.CATALOG_NAME + "'")) {
            assertThat(rs.next())
                .as("catalog alias '" + DuckLakeManager.CATALOG_NAME + "' must appear in duckdb_databases()")
                .isTrue();
        }
    }
}
