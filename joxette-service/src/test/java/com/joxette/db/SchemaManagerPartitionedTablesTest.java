package com.joxette.db;

import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link SchemaManager#hasUnpartitionedFiles}. */
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
    void hasUnpartitionedFiles_againstPlainDuckDB_throwsSQLException() {
        // The fake harness has no __ducklake_metadata_lake tables — this proves
        // the method surfaces the failure as a checked SQLException rather than
        // silently returning a wrong boolean, so callers can choose their own
        // fallback behavior (CompactionService and EntityReplayService both do).
        assertThatThrownBy(() ->
                SchemaManager.hasUnpartitionedFiles(conn, "entity_order"))
                .isInstanceOf(SQLException.class);
    }
}
