package com.joxette.compaction;

import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/** {@link RetentionService#rewriteSql} as text, and run against a real DuckLake catalog. */
class RetentionRewriteSqlTest {

    @TempDir
    Path dir;

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DuckDBTestSupport.newDuckLakeConnection(dir);
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE lake.main.t (id INTEGER)");
        }
        for (int file = 0; file < 3; file++) {
            try (Statement st = conn.createStatement()) {
                st.execute("INSERT INTO lake.main.t SELECT i FROM range(" + file * 10 + ", " + (file * 10 + 10) + ") r(i)");
            }
            DuckDBTestSupport.flushInlinedData(conn);
        }
        try (Statement st = conn.createStatement()) {
            st.execute("DELETE FROM lake.main.t WHERE id % 10 < 6"); // 60% of every file
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null) conn.close();
    }

    @Test
    void rewriteSql_capsFilesPerCall() {
        assertThat(RetentionService.rewriteSql("t", 0.5, 8))
                .isEqualTo("CALL ducklake_rewrite_data_files('lake', 't', delete_threshold => 0.5, "
                        + "max_compacted_files => 8)");
    }

    @Test
    void rewriteSql_rewritesDeleteHeavyFiles_thenFindsNothingLeft() throws SQLException {
        assertThat(DuckDBTestSupport.filesProcessed(conn, RetentionService.rewriteSql("t", 0.5, 8))).isEqualTo(3);
        assertThat(DuckDBTestSupport.filesProcessed(conn, RetentionService.rewriteSql("t", 0.5, 8))).isZero();

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(delete_file) FROM ducklake_list_files('lake', 't')")) {
            rs.next();
            assertThat(rs.getLong(1)).as("no delete files left after the rewrite").isZero();
        }
    }
}
