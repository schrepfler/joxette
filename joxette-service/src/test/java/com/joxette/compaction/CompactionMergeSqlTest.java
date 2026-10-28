package com.joxette.compaction;

import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/** {@link CompactionService#mergeSql} as text, and run against a real DuckLake catalog. */
class CompactionMergeSqlTest {

    private static final long TARGET_FILE_SIZE = 256L * 1024 * 1024;

    @TempDir
    Path dir;

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DuckDBTestSupport.newDuckLakeConnection(dir);
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE lake.main.t (id INTEGER)");
            st.execute("INSERT INTO lake.main.t VALUES (1)");
        }
        DuckDBTestSupport.flushInlinedData(conn);
        try (Statement st = conn.createStatement()) {
            st.execute("INSERT INTO lake.main.t VALUES (2)");
        }
        DuckDBTestSupport.flushInlinedData(conn); // two small files, both just written
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null) conn.close();
    }

    @Test
    void mergeSql_withoutLookback_hasNoOlderThan() {
        assertThat(CompactionService.mergeSql("t", TARGET_FILE_SIZE, 8, 0))
                .isEqualTo("CALL ducklake_merge_adjacent_files('lake', 't', max_compacted_files => 8, "
                        + "max_file_size => 268435456)");
    }

    @Test
    void mergeSql_withLookback_passesOlderThan() {
        assertThat(CompactionService.mergeSql("t", TARGET_FILE_SIZE, 8, 7))
                .isEqualTo("CALL ducklake_merge_adjacent_files('lake', 't', max_compacted_files => 8, "
                        + "max_file_size => 268435456, older_than => now() - INTERVAL '7 days')");
    }

    @Test
    void lookback_leavesFilesNewerThanTheCutoffAlone() throws SQLException {
        assertThat(DuckDBTestSupport.filesProcessed(conn, CompactionService.mergeSql("t", TARGET_FILE_SIZE, 8, 1)))
                .isZero();
        assertThat(DuckDBTestSupport.dataFileCount(conn, "t")).isEqualTo(2);
    }

    @Test
    void noLookback_mergesTheSameFiles() throws SQLException {
        assertThat(DuckDBTestSupport.filesProcessed(conn, CompactionService.mergeSql("t", TARGET_FILE_SIZE, 8, 0)))
                .isEqualTo(2);
        assertThat(DuckDBTestSupport.dataFileCount(conn, "t")).isEqualTo(1);
    }
}
