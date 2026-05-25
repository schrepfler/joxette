package com.joxette.db;

import com.joxette.config.JoxetteProperties;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Verifies that the VARIANT type survives a full DuckLake round-trip: CREATE TABLE with
 * a VARIANT column → INSERT a JSON value → force Parquet path (inlining disabled) → SELECT
 * it back and assert the payload is intact.
 *
 * <p>All tests use a real in-memory DuckLake catalog (DuckDB + ducklake extension) rather
 * than mocks so the probe exercises the actual Parquet encoding/decoding path.
 *
 * <p>Each test group documents the expected outcome for the current library versions:
 * <ul>
 *   <li>duckdb_jdbc {@code 1.5.3.0} + current {@code ducklake} extension:
 *       VARIANT is <b>supported</b> — {@code probeVariant} returns {@code true} and
 *       {@code SchemaManager.isVariantSupported()} returns {@code true}.</li>
 *   <li>If a future version regresses the Parquet serialisation of VARIANT, the probe
 *       will return {@code false} and the {@code metadata} column will fall back to
 *       {@code JSON} automatically — no manual intervention needed.</li>
 * </ul>
 */
class VariantProbeTest {

    private Connection conn;
    private DuckLakeManager duckLakeManager;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        duckLakeManager = new DuckLakeManager(conn, memoryCatalogProperties());
        duckLakeManager.initialize();
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

    private static JoxetteProperties memoryCatalogPropertiesWithInlining(Integer limit) {
        var props = memoryCatalogProperties();
        props.getCatalog().setInliningRowLimit(limit);
        return props;
    }

    /**
     * Invokes the private {@code probeVariant(Connection, String)} method via reflection.
     * Unwraps {@link InvocationTargetException} so the caller sees the root cause directly.
     */
    private static boolean invokeProbeVariant(SchemaManager manager,
                                               Connection c, String catalog) throws Exception {
        Method m = SchemaManager.class.getDeclaredMethod("probeVariant", Connection.class, String.class);
        m.setAccessible(true);
        try {
            return (Boolean) m.invoke(manager, c, catalog);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception ex) throw ex;
            throw e;
        }
    }

    private SchemaManager schemaManagerFor(JoxetteProperties props) {
        var dm = new DuckLakeManager(conn, props);
        return new SchemaManager(dm, props);
    }

    // -------------------------------------------------------------------------
    // Full round-trip: VARIANT through DuckLake Parquet
    // -------------------------------------------------------------------------

    @Nested
    class RoundTripTests {

        /**
         * The core contract: VARIANT survives the full DuckLake cycle.
         * The probe disables inlining ({@code data_inlining_row_limit=0}) so the row is
         * sent through DuckLake's Parquet serialisation path rather than buffered in the
         * catalog DB.  The SELECT asserts that the JSON payload is intact after the cycle.
         *
         * <p>Expected result with duckdb_jdbc 1.5.3.0: {@code true} — VARIANT works.
         */
        @Test
        void probeVariant_withRealDuckLakeCatalog_returnsTrue() throws Exception {
            var props = memoryCatalogProperties();
            var manager = schemaManagerFor(props);

            boolean result = invokeProbeVariant(manager, conn, DuckLakeManager.CATALOG_NAME);

            assertThat(result)
                    .as("VARIANT must survive the DuckLake Parquet round-trip " +
                        "(duckdb_jdbc 1.5.3.0 + current ducklake extension)")
                    .isTrue();
        }

        static Stream<Arguments> payloads() {
            return Stream.of(
                Arguments.of("'{\"probe\":true}'",              "probe"),
                Arguments.of("'{\"a\":1,\"b\":\"hello\"}'",     "hello"),
                Arguments.of("'{\"nested\":{\"x\":42}}'",       "nested"),
                Arguments.of("'{\"arr\":[1,2,3]}'",             "arr")
            );
        }

        /**
         * Exercises the VARIANT round-trip with several JSON payloads, including
         * nested objects, arrays, and numeric values.  Each payload must survive the
         * DuckLake cycle and be readable as a VARCHAR without data loss.
         */
        @ParameterizedTest(name = "[{index}] payload={0}")
        @MethodSource("payloads")
        void probeVariant_variousPayloads_roundTripCorrectly(
                String insertExpr, String expectedSubstring) throws Exception {
            // Build a minimal probe inline — we verify the round-trip at the SQL level
            // rather than through the private method so we can control the payload.
            String probeTable = DuckLakeManager.CATALOG_NAME + ".main.__variant_test_payload";
            try {
                exec("DROP TABLE IF EXISTS " + probeTable);
                try (Statement st = conn.createStatement()) {
                    st.execute("CALL " + DuckLakeManager.CATALOG_NAME +
                               ".set_option('data_inlining_row_limit', 0)");
                }
                exec("CREATE TABLE " + probeTable + " (v VARIANT)");
                exec("INSERT INTO " + probeTable + " VALUES (" + insertExpr + "::VARIANT)");

                String roundTripped;
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("SELECT v::VARCHAR FROM " + probeTable)) {
                    assertThat(rs.next()).as("row must be present after INSERT").isTrue();
                    roundTripped = rs.getString(1);
                }

                assertThat(roundTripped)
                        .as("round-tripped value must contain '%s' (payload: %s)",
                            expectedSubstring, insertExpr)
                        .isNotNull()
                        .contains(expectedSubstring);
            } finally {
                try { exec("DROP TABLE IF EXISTS " + probeTable); } catch (SQLException ignored) {}
            }
        }

        private void exec(String sql) throws SQLException {
            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Probe cleanup: no leftover __variant_probe table
    // -------------------------------------------------------------------------

    @Nested
    class CleanupTests {

        @Test
        void probeVariant_onSuccess_dropsProbeTable() throws Exception {
            var manager = schemaManagerFor(memoryCatalogProperties());
            invokeProbeVariant(manager, conn, DuckLakeManager.CATALOG_NAME);

            // After a successful probe the __variant_probe table must be gone.
            assertThat(probeTableExists())
                    .as("__variant_probe must be dropped after a successful probe")
                    .isFalse();
        }

        @Test
        void probeVariant_whenCatalogNotAttached_returnsFalse() throws Exception {
            // A fresh connection with no DuckLake catalog attached.
            try (Connection freshConn = DriverManager.getConnection("jdbc:duckdb:")) {
                var freshDm = new DuckLakeManager(freshConn, memoryCatalogProperties());
                var manager = new SchemaManager(freshDm, memoryCatalogProperties());

                // Do NOT call duckLakeManager.initialize() — catalog "lake" is absent.
                boolean result = invokeProbeVariant(manager, freshConn, "lake");

                assertThat(result)
                        .as("probe must return false when the catalog is not attached")
                        .isFalse();
            }
        }

        private boolean probeTableExists() throws SQLException {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT table_name FROM duckdb_tables()" +
                         " WHERE database_name = '" + DuckLakeManager.CATALOG_NAME + "'" +
                         " AND table_name = '__variant_probe'")) {
                return rs.next();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Inlining limit restore after probe
    // -------------------------------------------------------------------------

    @Nested
    class InliningRestoreTests {

        @Test
        void probeVariant_whenInliningLimitIsNull_restoresToDefault() throws Exception {
            var props = memoryCatalogPropertiesWithInlining(null);
            var manager = schemaManagerFor(props);

            invokeProbeVariant(manager, conn, DuckLakeManager.CATALOG_NAME);

            // null configured → restore to DuckLake default of 10
            assertThat(readInliningRowLimitOption())
                    .as("inlining row limit must be restored to DuckLake default (10) after probe")
                    .isEqualTo("10");
        }

        @Test
        void probeVariant_whenInliningLimitIsZero_restoresToZero() throws Exception {
            var props = memoryCatalogPropertiesWithInlining(0);
            var manager = schemaManagerFor(props);

            invokeProbeVariant(manager, conn, DuckLakeManager.CATALOG_NAME);

            assertThat(readInliningRowLimitOption())
                    .as("inlining row limit must be restored to configured value (0) after probe")
                    .isEqualTo("0");
        }

        @Test
        void probeVariant_whenInliningLimitIsPositive_restoresToConfiguredValue() throws Exception {
            var props = memoryCatalogPropertiesWithInlining(50);
            var manager = schemaManagerFor(props);

            invokeProbeVariant(manager, conn, DuckLakeManager.CATALOG_NAME);

            assertThat(readInliningRowLimitOption())
                    .as("inlining row limit must be restored to configured value (50) after probe")
                    .isEqualTo("50");
        }

        private String readInliningRowLimitOption() throws SQLException {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT value FROM ducklake_options('" + DuckLakeManager.CATALOG_NAME + "')" +
                         " WHERE option_name = 'data_inlining_row_limit'")) {
                return rs.next() ? rs.getString("value") : null;
            }
        }
    }

    // -------------------------------------------------------------------------
    // 1.5.3 bug-fix coverage: small decimal Parquet writing
    // -------------------------------------------------------------------------

    /**
     * Regression coverage for duckdb_jdbc 1.5.3.0 fix:
     * <em>"Corrected VARIANT small decimal writing to Parquet"</em>.
     *
     * <p>Prior to 1.5.3 a DECIMAL value embedded inside a VARIANT JSON object could
     * be silently mis-encoded when written to a Parquet file, causing the value to
     * change (or become NULL) on read-back.  This test forces the Parquet path
     * (inlining disabled) and asserts that typical order-amount decimals survive the
     * round-trip exactly.
     *
     * <p>Amounts chosen to cover distinct IEEE-754 edge cases:
     * <ul>
     *   <li>{@code 0.01} — smallest typical amount; triggers subnormal-precision paths</li>
     *   <li>{@code 99.50} — two-decimal "money" amount representative of order totals</li>
     *   <li>{@code 1234567.89} — large amount exercising the full mantissa width</li>
     *   <li>{@code 0.001} — sub-cent precision (cryptocurrency / micro-billing)</li>
     * </ul>
     */
    @Nested
    class DecimalParquetTests {

        static Stream<Arguments> decimalPayloads() {
            return Stream.of(
                Arguments.of("'{\"amount\":0.01}'",          "0.01"),
                Arguments.of("'{\"amount\":99.50}'",         "99.5"),      // JSON normalises trailing zero
                Arguments.of("'{\"amount\":1234567.89}'",    "1234567.89"),
                Arguments.of("'{\"amount\":0.001}'",         "0.001")
            );
        }

        @ParameterizedTest(name = "[{index}] payload={0} must contain {1}")
        @MethodSource("decimalPayloads")
        void smallDecimal_roundTripsThroughParquet(
                String insertExpr, String expectedSubstring) throws Exception {
            String probeTable = DuckLakeManager.CATALOG_NAME + ".main.__variant_decimal_test";
            try {
                exec("DROP TABLE IF EXISTS " + probeTable);
                try (Statement st = conn.createStatement()) {
                    st.execute("CALL " + DuckLakeManager.CATALOG_NAME +
                               ".set_option('data_inlining_row_limit', 0)");
                }
                exec("CREATE TABLE " + probeTable + " (v VARIANT)");
                exec("INSERT INTO " + probeTable + " VALUES (" + insertExpr + "::VARIANT)");

                String roundTripped;
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("SELECT v::VARCHAR FROM " + probeTable)) {
                    assertThat(rs.next()).as("row must be present after INSERT").isTrue();
                    roundTripped = rs.getString(1);
                }

                assertThat(roundTripped)
                        .as("decimal value must survive DuckLake Parquet round-trip " +
                            "(1.5.3 fix: 'corrected VARIANT small decimal writing to Parquet'); " +
                            "payload=%s, expected substring='%s'", insertExpr, expectedSubstring)
                        .isNotNull()
                        .contains(expectedSubstring);
            } finally {
                try { exec("DROP TABLE IF EXISTS " + probeTable); } catch (SQLException ignored) {}
            }
        }

        private void exec(String sql) throws SQLException {
            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        }
    }

    // -------------------------------------------------------------------------
    // 1.5.3 bug-fix coverage: VARIANT selection vector indexing
    // -------------------------------------------------------------------------

    /**
     * Regression coverage for duckdb_jdbc 1.5.3.0 fix:
     * <em>"Fixed VARIANT selection vector indexing issues"</em>.
     *
     * <p>Prior to 1.5.3, when multiple VARIANT rows were read back from Parquet,
     * incorrect selection-vector indexing could cause a row at position N to return
     * the value from position M.  This test inserts several rows with distinct,
     * easily-distinguishable JSON values and verifies that each row contains its own
     * payload — not a neighbour's — after the Parquet round-trip.
     */
    @Nested
    class SelectionVectorTests {

        @Test
        void multipleRows_eachReturnCorrectVariantValue() throws Exception {
            String probeTable = DuckLakeManager.CATALOG_NAME + ".main.__variant_selvec_test";
            try {
                exec("DROP TABLE IF EXISTS " + probeTable);
                // Disable inlining to force the Parquet code path.
                try (Statement st = conn.createStatement()) {
                    st.execute("CALL " + DuckLakeManager.CATALOG_NAME +
                               ".set_option('data_inlining_row_limit', 0)");
                }
                exec("CREATE TABLE " + probeTable + " (id INTEGER, v VARIANT)");

                // Insert 5 rows each with a unique, non-overlapping sentinel value.
                for (int i = 1; i <= 5; i++) {
                    exec("INSERT INTO " + probeTable +
                         " VALUES (" + i + ", '{\"row\":" + i + ",\"tag\":\"row" + i + "\"}'::VARIANT)");
                }

                // Read back ordered by id and assert each row has its own sentinel.
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery(
                             "SELECT id, v::VARCHAR FROM " + probeTable +
                             " ORDER BY id ASC")) {

                    for (int expected = 1; expected <= 5; expected++) {
                        assertThat(rs.next())
                                .as("row %d must be present", expected)
                                .isTrue();
                        int gotId         = rs.getInt("id");
                        String gotVariant = rs.getString(2);

                        assertThat(gotId)
                                .as("row id must match insertion order (selection vector indexing)")
                                .isEqualTo(expected);
                        assertThat(gotVariant)
                                .as("VARIANT at row %d must contain sentinel 'row%d' " +
                                    "(1.5.3 fix: 'fixed VARIANT selection vector indexing issues')",
                                    expected, expected)
                                .isNotNull()
                                .contains("row" + expected);
                    }
                    assertThat(rs.next())
                            .as("no extra rows beyond the 5 inserted")
                            .isFalse();
                }
            } finally {
                try { exec("DROP TABLE IF EXISTS " + probeTable); } catch (SQLException ignored) {}
            }
        }

        /**
         * A filtered SELECT (WHERE clause) relies on the selection vector to map
         * result rows back to storage positions.  Verify that VARIANT values are
         * still correct after the engine applies a predicate filter.
         */
        @Test
        void filteredSelect_returnsCorrectVariantRow() throws Exception {
            String probeTable = DuckLakeManager.CATALOG_NAME + ".main.__variant_selvec_filter_test";
            try {
                exec("DROP TABLE IF EXISTS " + probeTable);
                try (Statement st = conn.createStatement()) {
                    st.execute("CALL " + DuckLakeManager.CATALOG_NAME +
                               ".set_option('data_inlining_row_limit', 0)");
                }
                exec("CREATE TABLE " + probeTable + " (id INTEGER, v VARIANT)");

                for (int i = 1; i <= 10; i++) {
                    exec("INSERT INTO " + probeTable +
                         " VALUES (" + i + ", '{\"id\":" + i + ",\"label\":\"item" + i + "\"}'::VARIANT)");
                }

                // Select only rows 4 and 7 — tests that the selection vector correctly
                // maps filtered positions back to the right VARIANT storage cells.
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery(
                             "SELECT id, v::VARCHAR FROM " + probeTable +
                             " WHERE id IN (4, 7) ORDER BY id")) {

                    assertThat(rs.next()).as("first filtered row (id=4)").isTrue();
                    assertThat(rs.getInt("id")).isEqualTo(4);
                    assertThat(rs.getString(2))
                            .as("VARIANT at row 4 must contain 'item4'")
                            .contains("item4");

                    assertThat(rs.next()).as("second filtered row (id=7)").isTrue();
                    assertThat(rs.getInt("id")).isEqualTo(7);
                    assertThat(rs.getString(2))
                            .as("VARIANT at row 7 must contain 'item7'")
                            .contains("item7");

                    assertThat(rs.next())
                            .as("no rows beyond the two selected")
                            .isFalse();
                }
            } finally {
                try { exec("DROP TABLE IF EXISTS " + probeTable); } catch (SQLException ignored) {}
            }
        }

        private void exec(String sql) throws SQLException {
            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        }
    }

    // -------------------------------------------------------------------------
    // SchemaManager.initialize() integration: isVariantSupported()
    // -------------------------------------------------------------------------

    @Nested
    class SchemaManagerInitializeTests {

        /**
         * Full initialize() smoke test.  With a real DuckLake catalog the probe should
         * succeed, so isVariantSupported() must return true and cassette tables will
         * use VARIANT for the {@code metadata} column.
         *
         * <p>No bootstrap topics or entities are configured, so no DuckLake tables are
         * created beyond the probe table itself (which is cleaned up by the probe).
         */
        @Test
        void initialize_withInMemoryDuckLake_setsVariantSupportedTrue() throws SQLException {
            var props = memoryCatalogProperties();
            var sm = new SchemaManager(duckLakeManager, props);
            sm.initialize();

            assertThat(sm.isVariantSupported())
                    .as("isVariantSupported() must be true after successful probe " +
                        "with duckdb_jdbc 1.5.3.0 + current ducklake extension")
                    .isTrue();
        }

        @Test
        void initialize_doesNotLeaveProbeTableBehind() throws SQLException {
            var props = memoryCatalogProperties();
            var sm = new SchemaManager(duckLakeManager, props);
            sm.initialize();

            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT table_name FROM duckdb_tables()" +
                         " WHERE database_name = '" + DuckLakeManager.CATALOG_NAME + "'" +
                         " AND table_name = '__variant_probe'")) {
                assertThat(rs.next())
                        .as("__variant_probe must be cleaned up by initialize()")
                        .isFalse();
            }
        }
    }
}
