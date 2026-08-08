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
 * Unit tests for {@link SchemaManager#ensureTablePartitioned}.
 *
 * <p>Plain DuckDB (the {@link DuckDBTestSupport} harness) does not support
 * the {@code SET PARTITIONED BY} DuckLake extension, so every call through
 * the real connection exercises the warn-and-swallow path — this mirrors
 * {@link SchemaManagerSortedTablesTest}'s approach for {@code ensureTableSorted}.
 */
class SchemaManagerPartitionedTablesTest {

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

    @Test
    void entityCassette_firstCall_doesNotThrow() throws Exception {
        DuckDBTestSupport.createEntityTable(conn, "order");

        assertThatCode(() ->
            SchemaManager.ensureTablePartitioned(conn, "lake", "entity_order"))
                .doesNotThrowAnyException();
    }

    @Test
    void entityCassette_repeatedCall_isIdempotent() throws Exception {
        DuckDBTestSupport.createEntityTable(conn, "order");
        SchemaManager.ensureTablePartitioned(conn, "lake", "entity_order");

        assertThatCode(() ->
            SchemaManager.ensureTablePartitioned(conn, "lake", "entity_order"))
                .doesNotThrowAnyException();
    }

    @Test
    void nonExistentTable_doesNotThrow() {
        assertThatCode(() ->
            SchemaManager.ensureTablePartitioned(conn, "lake", "entity_no_such_table"))
                .doesNotThrowAnyException();
    }

    @Test
    void entityCassette_sqlContainsBucketColumn() throws Exception {
        Statement mockStmt = mock(Statement.class);
        Connection mockConn = mock(Connection.class);
        when(mockConn.createStatement()).thenReturn(mockStmt);

        SchemaManager.ensureTablePartitioned(mockConn, "lake", "entity_order");

        verify(mockStmt).execute(
                "ALTER TABLE lake.main.entity_order SET PARTITIONED BY (bucket)");
    }

    @Test
    void sqlExceptionFromDriver_isSwallowedNotPropagated() throws Exception {
        Statement mockStmt = mock(Statement.class);
        Connection mockConn = mock(Connection.class);
        when(mockConn.createStatement()).thenReturn(mockStmt);
        when(mockStmt.execute(org.mockito.ArgumentMatchers.anyString()))
                .thenThrow(new SQLException("Parser Error: syntax error at or near 'PARTITIONED'"));

        assertThatCode(() ->
            SchemaManager.ensureTablePartitioned(mockConn, "lake", "entity_order"))
                .doesNotThrowAnyException();
    }
}
