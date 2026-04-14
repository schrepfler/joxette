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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link DuckLakeManager} using an in-memory DuckDB connection
 * with the {@code ducklake} extension loaded — no real S3 or file-system catalog needed.
 *
 * <p>Each test obtains a fresh {@link Connection} in {@code @BeforeEach} and tears it
 * down in {@code @AfterEach}.  Private methods that need isolated invocation are reached
 * via reflection with {@link InvocationTargetException} unwrapping so AssertJ assertions
 * see the root cause.
 *
 * <p>The {@code ducklake_options('lake')} table function is used to verify that
 * {@code CALL lake.set_option()} was actually executed: after {@link DuckLakeManager#initialize()}
 * the option value is readable there without any additional mocking.
 */
class DuckLakeManagerTest {

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

    /**
     * Base properties pointing at the in-memory DuckLake catalog.
     * auto-migrate, metadata logging, and inlining row limit are all off/null
     * so individual tests opt-in to exactly the feature under test.
     */
    private static JoxetteProperties memoryCatalogProperties() {
        var props = new JoxetteProperties();
        props.getCatalog().setPath(":memory:");
        props.getCatalog().setObjectStoragePath(null); // resolves to ":memory:" data path
        props.getCatalog().setAutoMigrate(false);
        props.getCatalog().setMetadataQueryLogging(false);
        props.getCatalog().setInliningRowLimit(null);
        return props;
    }

    /**
     * Calls the private {@code runCatalogMigration()} via reflection, unwrapping
     * {@link InvocationTargetException} so the caller sees the root cause directly.
     */
    private static void invokeMigration(DuckLakeManager manager) throws Exception {
        Method m = DuckLakeManager.class.getDeclaredMethod("runCatalogMigration");
        m.setAccessible(true);
        try {
            m.invoke(manager);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception ex) throw ex;
            throw e;
        }
    }

    /** Installs and loads the ducklake extension without ATTACHing any catalog alias. */
    private void loadDuckLakeExtension() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("INSTALL ducklake");
        }
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("LOAD ducklake");
        }
    }

    /**
     * Reads {@code ducklake_options('lake')} and returns the current value of
     * {@code data_inlining_row_limit}, or {@code null} if the option row is absent.
     */
    private String readInliningRowLimitOption() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT value FROM ducklake_options('lake') " +
                     "WHERE option_name = 'data_inlining_row_limit'")) {
            return rs.next() ? rs.getString("value") : null;
        }
    }

    // -------------------------------------------------------------------------
    // Log-capture helpers (Logback ListAppender)
    // -------------------------------------------------------------------------

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
    // ducklake_settings()
    // -------------------------------------------------------------------------

    @Nested
    class DuckLakeSettingsTests {

        @Test
        void ducklake_settings_returnsRowWithExpectedColumns() throws Exception {
            new DuckLakeManager(conn, memoryCatalogProperties()).initialize();

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT * FROM ducklake_settings('" + DuckLakeManager.CATALOG_NAME + "')")) {

                assertThat(rs.next())
                        .as("ducklake_settings() must return at least one row after ATTACH")
                        .isTrue();

                // All three columns read by logDuckLakeVersion() must be present and non-null.
                assertThat(rs.getString("extension_version")).as("extension_version").isNotBlank();
                assertThat(rs.getString("catalog_type")).as("catalog_type").isNotBlank();
                assertThat(rs.getString("data_path")).as("data_path").isNotNull();
            }
        }

        @Test
        void logDuckLakeVersion_doesNotThrow() {
            var manager = new DuckLakeManager(conn, memoryCatalogProperties());

            assertThatCode(manager::initialize).doesNotThrowAnyException();
        }

        @Test
        void logDuckLakeVersion_logsVersionCatalogAndDataPath() throws Exception {
            var appender = attachListAppender(DuckLakeManager.class);
            try {
                new DuckLakeManager(conn, memoryCatalogProperties()).initialize();

                // The INFO log emitted by logDuckLakeVersion() contains all three fields.
                assertThat(appender.list)
                        .extracting(ILoggingEvent::getFormattedMessage)
                        .anyMatch(msg -> msg.startsWith("DuckLake version=")
                                && msg.contains("catalog=")
                                && msg.contains("data_path="));
            } finally {
                detachListAppender(DuckLakeManager.class, appender);
            }
        }
    }

    // -------------------------------------------------------------------------
    // ducklake_migrate()
    // -------------------------------------------------------------------------

    @Nested
    class MigrationTests {

        @Test
        void runCatalogMigration_onFreshCatalog_doesNotThrow() throws Exception {
            var manager = new DuckLakeManager(conn, memoryCatalogProperties());
            manager.initialize(); // attaches in-memory 'lake' catalog

            // Either completes normally (ducklake_migrate available) or logs a warning
            // and skips (ducklake_migrate not yet in this build) — both are non-throwing.
            assertThatCode(() -> invokeMigration(manager)).doesNotThrowAnyException();
        }

        @Test
        void runCatalogMigration_whenCatalogAlreadyCurrent_doesNotThrow() throws Exception {
            var manager = new DuckLakeManager(conn, memoryCatalogProperties());
            manager.initialize();

            invokeMigration(manager); // first call

            // Second call — catalog still current (or function still absent) — must not throw.
            assertThatCode(() -> invokeMigration(manager)).doesNotThrowAnyException();
        }

        @Test
        void runCatalogMigration_whenConnectionFails_throwsWithDescriptiveMessage() throws Exception {
            // Load the extension and attach the catalog, then close the connection to simulate
            // a DB failure mid-migration.  createStatement() on a closed connection throws a
            // SQLException that does not match the "does not exist" guard, so the method must
            // rethrow as IllegalStateException with the expected message.
            loadDuckLakeExtension();
            var manager = new DuckLakeManager(conn, memoryCatalogProperties());
            conn.close(); // simulate loss of DB connection

            assertThatThrownBy(() -> invokeMigration(manager))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("DuckLake catalog migration failed");
        }

        @Test
        void runCatalogMigration_logsOutcomeMessage() throws Exception {
            var manager = new DuckLakeManager(conn, memoryCatalogProperties());
            manager.initialize();

            var appender = attachListAppender(DuckLakeManager.class);
            try {
                invokeMigration(manager);

                // One of the three outcome messages must appear.
                assertThat(appender.list)
                        .extracting(ILoggingEvent::getFormattedMessage)
                        .anyMatch(msg -> msg.contains("no changes needed")
                                || msg.contains("completed")
                                || msg.contains("not available in this DuckLake build"));
            } finally {
                detachListAppender(DuckLakeManager.class, appender);
            }
        }
    }

    // -------------------------------------------------------------------------
    // DATA_INLINING_ROW_LIMIT set_option()
    // -------------------------------------------------------------------------

    @Nested
    class InliningRowLimitTests {

        @Test
        void inliningRowLimit_zero_setOptionExecutesWithoutError() {
            var props = memoryCatalogProperties();
            props.getCatalog().setInliningRowLimit(0);

            assertThatCode(() -> new DuckLakeManager(conn, props).initialize())
                    .doesNotThrowAnyException();
        }

        @Test
        void inliningRowLimit_zero_catalogOptionReflectsValue() throws Exception {
            var props = memoryCatalogProperties();
            props.getCatalog().setInliningRowLimit(0);
            new DuckLakeManager(conn, props).initialize();

            assertThat(readInliningRowLimitOption())
                    .as("data_inlining_row_limit in ducklake_options after set_option(0)")
                    .isEqualTo("0");
        }

        @Test
        void inliningRowLimit_positive_setOptionExecutesWithoutError() {
            var props = memoryCatalogProperties();
            props.getCatalog().setInliningRowLimit(100);

            assertThatCode(() -> new DuckLakeManager(conn, props).initialize())
                    .doesNotThrowAnyException();
        }

        @Test
        void inliningRowLimit_positive_catalogOptionReflectsValue() throws Exception {
            var props = memoryCatalogProperties();
            props.getCatalog().setInliningRowLimit(100);
            new DuckLakeManager(conn, props).initialize();

            assertThat(readInliningRowLimitOption())
                    .as("data_inlining_row_limit in ducklake_options after set_option(100)")
                    .isEqualTo("100");
        }

        @Test
        void inliningRowLimit_null_setOptionNotCalled() throws Exception {
            var props = memoryCatalogProperties();
            props.getCatalog().setInliningRowLimit(null); // explicit for clarity

            var appender = attachListAppender(DuckLakeManager.class);
            try {
                assertThatCode(() -> new DuckLakeManager(conn, props).initialize())
                        .doesNotThrowAnyException();

                List<String> messages = appender.list.stream()
                        .map(ILoggingEvent::getFormattedMessage)
                        .toList();

                // The null guard logs the "default (10)" sentinel and returns before any SQL.
                assertThat(messages)
                        .as("null limit must log the default-value sentinel")
                        .anyMatch(msg -> msg.contains("inlining row limit") && msg.contains("default"));

                // Neither the "inlining disabled" nor "overrides default" message should appear.
                assertThat(messages)
                        .as("no set_option variant message when limit is null")
                        .noneMatch(msg -> msg.contains("inlining disabled") || msg.contains("overrides default"));
            } finally {
                detachListAppender(DuckLakeManager.class, appender);
            }
        }

        @Test
        void inliningRowLimit_zero_logsDisabledMessage() throws Exception {
            var props = memoryCatalogProperties();
            props.getCatalog().setInliningRowLimit(0);

            var appender = attachListAppender(DuckLakeManager.class);
            try {
                new DuckLakeManager(conn, props).initialize();

                assertThat(appender.list)
                        .extracting(ILoggingEvent::getFormattedMessage)
                        .anyMatch(msg -> msg.contains("inlining disabled, always write Parquet"));
            } finally {
                detachListAppender(DuckLakeManager.class, appender);
            }
        }

        @Test
        void inliningRowLimit_positive_logsOverrideMessage() throws Exception {
            var props = memoryCatalogProperties();
            props.getCatalog().setInliningRowLimit(100);

            var appender = attachListAppender(DuckLakeManager.class);
            try {
                new DuckLakeManager(conn, props).initialize();

                assertThat(appender.list)
                        .extracting(ILoggingEvent::getFormattedMessage)
                        .anyMatch(msg -> msg.contains("100") && msg.contains("overrides default of 10"));
            } finally {
                detachListAppender(DuckLakeManager.class, appender);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Metadata query logging (SET logging_type / SET logging_level)
    // -------------------------------------------------------------------------

    @Nested
    class MetadataQueryLoggingTests {

        @Test
        void metadataLogging_true_setStatementsExecuteWithoutError() {
            // enableMetadataLogging() catches any SQLException from SET logging_type
            // (the statement is not supported in all DuckLake builds), so initialize()
            // must complete without throwing regardless.
            var props = memoryCatalogProperties();
            props.getCatalog().setMetadataQueryLogging(true);

            assertThatCode(() -> new DuckLakeManager(conn, props).initialize())
                    .doesNotThrowAnyException();
        }

        @Test
        void metadataLogging_true_logsEnablingMessage() throws Exception {
            var props = memoryCatalogProperties();
            props.getCatalog().setMetadataQueryLogging(true);

            var appender = attachListAppender(DuckLakeManager.class);
            try {
                new DuckLakeManager(conn, props).initialize();

                assertThat(appender.list)
                        .extracting(ILoggingEvent::getFormattedMessage)
                        .anyMatch(msg -> msg.contains("Enabling DuckLakeMetadata query logging"));
            } finally {
                detachListAppender(DuckLakeManager.class, appender);
            }
        }

        @Test
        void metadataLogging_false_setStatementsNotExecuted() throws Exception {
            var props = memoryCatalogProperties();
            props.getCatalog().setMetadataQueryLogging(false); // default

            var appender = attachListAppender(DuckLakeManager.class);
            try {
                new DuckLakeManager(conn, props).initialize();

                // enableMetadataLogging() logs this sentinel only when called.
                // Its absence proves SET logging_type / SET logging_level were not executed.
                assertThat(appender.list)
                        .extracting(ILoggingEvent::getFormattedMessage)
                        .noneMatch(msg -> msg.contains("DuckLakeMetadata query logging"));
            } finally {
                detachListAppender(DuckLakeManager.class, appender);
            }
        }

        @Test
        void metadataLogging_false_doesNotThrow() {
            var props = memoryCatalogProperties();
            props.getCatalog().setMetadataQueryLogging(false);

            assertThatCode(() -> new DuckLakeManager(conn, props).initialize())
                    .doesNotThrowAnyException();
        }
    }
}
