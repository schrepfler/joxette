package com.joxette.db;

import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SchemaManager#ensureTableSorted}.
 *
 * <h2>Test environment</h2>
 * <p>Idempotency and non-existence tests run against an in-memory DuckDB
 * connection produced by {@link DuckDBTestSupport#newConnection()}.  Plain
 * DuckDB does not support the {@code SET SORTED BY} DuckLake extension, so
 * every call through the real connection exercises the warn-and-swallow path.
 * This is intentional: the purpose of those tests is to verify that
 * {@code ensureTableSorted} never propagates an exception regardless of
 * whether the underlying DuckLake/DuckDB version supports the statement.
 *
 * <h2>Sort-key correctness tests</h2>
 * <p>A Mockito {@link Connection} mock is used to capture the exact SQL
 * string that {@code ensureTableSorted} constructs and executes.  This
 * decouples the sort-key assertions from whether the current DuckDB build
 * supports the DDL.  In a full DuckLake environment one could additionally
 * verify the declared order via {@code ducklake_table_info()} metadata or by
 * inserting out-of-order rows and confirming that a subsequent compaction /
 * inline-flush produces sorted output.
 */
class SchemaManagerSortedTablesTest {

    /** Expected sort expression for a general cassette table. */
    private static final String GENERAL_SORT =
            "(kafka_timestamp ASC, kafka_partition ASC, kafka_offset ASC)";

    /** Expected sort expression for an entity cassette table. */
    private static final String ENTITY_SORT =
            "(entity_id ASC, kafka_timestamp ASC, recorded_at ASC)";

    private Connection conn;

    @BeforeEach
    void setUp() throws Exception {
        conn = DuckDBTestSupport.newConnection();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    // -------------------------------------------------------------------------
    // Idempotency — general cassette
    // -------------------------------------------------------------------------

    @Test
    void generalCassette_firstCall_doesNotThrow() throws Exception {
        DuckDBTestSupport.createGeneralCassetteTable(conn, "orders.events");

        assertThatCode(() ->
            SchemaManager.ensureTableSorted(conn, "lake", "general_orders_events", GENERAL_SORT))
                .doesNotThrowAnyException();
    }

    @Test
    void generalCassette_repeatedCall_isIdempotent() throws Exception {
        DuckDBTestSupport.createGeneralCassetteTable(conn, "orders.events");
        // First call
        SchemaManager.ensureTableSorted(conn, "lake", "general_orders_events", GENERAL_SORT);

        // Second call on the same table must also succeed without throwing
        assertThatCode(() ->
            SchemaManager.ensureTableSorted(conn, "lake", "general_orders_events", GENERAL_SORT))
                .doesNotThrowAnyException();
    }

    // -------------------------------------------------------------------------
    // Idempotency — entity cassette
    // -------------------------------------------------------------------------

    @Test
    void entityCassette_firstCall_doesNotThrow() throws Exception {
        DuckDBTestSupport.createEntityTable(conn, "order");

        assertThatCode(() ->
            SchemaManager.ensureTableSorted(conn, "lake", "entity_order", ENTITY_SORT))
                .doesNotThrowAnyException();
    }

    @Test
    void entityCassette_repeatedCall_isIdempotent() throws Exception {
        DuckDBTestSupport.createEntityTable(conn, "order");
        // First call
        SchemaManager.ensureTableSorted(conn, "lake", "entity_order", ENTITY_SORT);

        // Second call must also be safe to run on every startup
        assertThatCode(() ->
            SchemaManager.ensureTableSorted(conn, "lake", "entity_order", ENTITY_SORT))
                .doesNotThrowAnyException();
    }

    // -------------------------------------------------------------------------
    // Non-existent table: warn + swallow, never throw
    // -------------------------------------------------------------------------

    @Test
    void nonExistentTable_doesNotThrow() {
        // The table has not been created; the ALTER TABLE will fail at the
        // DuckDB/DuckLake layer.  ensureTableSorted must log a warning and
        // return normally so that a startup against an older DuckLake build
        // (or plain DuckDB) does not prevent the application from starting.
        assertThatCode(() ->
            SchemaManager.ensureTableSorted(
                conn, "lake", "general_no_such_table", GENERAL_SORT))
                .doesNotThrowAnyException();
    }

    // -------------------------------------------------------------------------
    // Correct sort keys — verified via the SQL string sent to the driver
    // -------------------------------------------------------------------------

    @Test
    void generalCassette_sqlContainsCorrectSortColumns() throws Exception {
        Statement mockStmt = mock(Statement.class);
        Connection mockConn = mock(Connection.class);
        when(mockConn.createStatement()).thenReturn(mockStmt);

        SchemaManager.ensureTableSorted(
                mockConn, "lake", "general_orders_events", GENERAL_SORT);

        verify(mockStmt).execute(
                "ALTER TABLE lake.main.general_orders_events SET SORTED BY "
                + GENERAL_SORT);
    }

    @Test
    void entityCassette_sqlContainsCorrectSortColumns() throws Exception {
        Statement mockStmt = mock(Statement.class);
        Connection mockConn = mock(Connection.class);
        when(mockConn.createStatement()).thenReturn(mockStmt);

        SchemaManager.ensureTableSorted(mockConn, "lake", "entity_order", ENTITY_SORT);

        verify(mockStmt).execute(
                "ALTER TABLE lake.main.entity_order SET SORTED BY " + ENTITY_SORT);
    }

    // -------------------------------------------------------------------------
    // SQLException from driver is always swallowed (never re-thrown)
    // -------------------------------------------------------------------------

    @Test
    void sqlExceptionFromDriver_isSwallowedNotPropagated() throws Exception {
        Statement mockStmt = mock(Statement.class);
        Connection mockConn = mock(Connection.class);
        when(mockConn.createStatement()).thenReturn(mockStmt);
        when(mockStmt.execute(org.mockito.ArgumentMatchers.anyString()))
                .thenThrow(new SQLException("Parser Error: syntax error at or near 'SORTED'"));

        // Even a hard driver error must not propagate out of the method
        assertThatCode(() ->
            SchemaManager.ensureTableSorted(
                mockConn, "lake", "general_orders_events", GENERAL_SORT))
                .doesNotThrowAnyException();
    }
}
