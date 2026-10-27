package com.joxette.db;

import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs SchemaManager's cassette DDL against a real DuckLake catalog in a temp directory
 * (local files, no object store) and checks what DuckLake recorded and wrote.
 */
class DuckLakeTableLayoutTest {

    @TempDir
    Path dir;

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DuckDBTestSupport.newDuckLakeConnection(dir);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null) conn.close();
    }

    @Test
    void entityTable_isPartitionedAndSortedFromCreation() throws SQLException {
        exec(SchemaManager.entityCassetteDdl("lake", "entity_order", "VARIANT"));

        assertThat(partitionKeys("entity_order")).containsExactly("bucket");
        assertThat(sortKeys("entity_order")).containsExactly("entity_id", "kafka_timestamp", "recorded_at");
    }

    @Test
    void generalTable_isSortedFromCreation() throws SQLException {
        exec(SchemaManager.generalCassetteDdl("lake", "general_orders_events", "VARIANT"));

        assertThat(sortKeys("general_orders_events"))
                .containsExactly("kafka_timestamp", "kafka_partition", "kafka_offset");
    }

    @Test
    void ddl_isIdempotent() throws SQLException {
        exec(SchemaManager.entityCassetteDdl("lake", "entity_order", "VARIANT"));
        exec(SchemaManager.entityCassetteDdl("lake", "entity_order", "VARIANT"));

        assertThat(partitionKeys("entity_order")).containsExactly("bucket");
        assertThat(sortKeys("entity_order")).containsExactly("entity_id", "kafka_timestamp", "recorded_at");
    }

    // -------------------------------------------------------------------------

    private void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    /** Partition keys in order, as {@code column} or {@code transform(column)}. */
    private List<String> partitionKeys(String table) throws SQLException {
        return strings("""
                SELECT CASE WHEN pc.transform = 'identity' THEN c.column_name
                            ELSE pc.transform || '(' || c.column_name || ')' END
                FROM __ducklake_metadata_lake.ducklake_partition_column pc
                JOIN __ducklake_metadata_lake.ducklake_table t
                  ON t.table_id = pc.table_id AND t.end_snapshot IS NULL
                JOIN __ducklake_metadata_lake.ducklake_column c
                  ON c.table_id = pc.table_id AND c.column_id = pc.column_id AND c.end_snapshot IS NULL
                WHERE t.table_name = ?
                ORDER BY pc.partition_key_index
                """, table);
    }

    private List<String> sortKeys(String table) throws SQLException {
        return strings("""
                SELECT se.expression
                FROM __ducklake_metadata_lake.ducklake_sort_expression se
                JOIN __ducklake_metadata_lake.ducklake_table t
                  ON t.table_id = se.table_id AND t.end_snapshot IS NULL
                WHERE t.table_name = ?
                ORDER BY se.sort_id, se.sort_key_index
                """, table);
    }

    private List<String> strings(String sql, String param) throws SQLException {
        List<String> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, param);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(rs.getString(1));
            }
        }
        return out;
    }
}
