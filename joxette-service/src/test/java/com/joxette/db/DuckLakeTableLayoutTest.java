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

        assertThat(partitionKeys("entity_order")).containsExactly("bucket", "epoch_month(recorded_at)");
        assertThat(sortKeys("entity_order")).containsExactly("entity_id", "kafka_timestamp", "recorded_at");
    }

    @Test
    void generalTable_isPartitionedAndSortedFromCreation() throws SQLException {
        exec(SchemaManager.generalCassetteDdl("lake", "general_orders_events", "VARIANT"));

        assertThat(sortKeys("general_orders_events"))
                .containsExactly("kafka_timestamp", "kafka_partition", "kafka_offset");
        assertThat(partitionKeys("general_orders_events")).containsExactly("epoch_day(recorded_at)");
    }

    @Test
    void ddl_isIdempotent() throws SQLException {
        exec(SchemaManager.entityCassetteDdl("lake", "entity_order", "VARIANT"));
        exec(SchemaManager.entityCassetteDdl("lake", "entity_order", "VARIANT"));

        assertThat(partitionKeys("entity_order")).containsExactly("bucket", "epoch_month(recorded_at)");
        assertThat(sortKeys("entity_order")).containsExactly("entity_id", "kafka_timestamp", "recorded_at");
    }

    @Test
    void entityRows_landInBucketThenMonthDirectories() throws SQLException {
        exec(SchemaManager.entityCassetteDdl("lake", "entity_order", "VARIANT"));
        insertEntityRow(3, "2026-09-15 10:00:00+00");
        insertEntityRow(3, "2026-10-15 10:00:00+00");
        DuckDBTestSupport.flushInlinedData(conn);

        // EntityReplayService globs bucket=N/**/*.parquet — the month level must sit below bucket.
        assertThat(dataFiles("entity_order"))
                .hasSize(2)
                .allSatisfy(f -> assertThat(f)
                        .matches(".*/main/entity_order/bucket=3/epoch_month=\\d+/[^/]+\\.parquet"));
    }

    @Test
    void generalRows_landInDayDirectories() throws SQLException {
        exec(SchemaManager.generalCassetteDdl("lake", "general_orders_events", "VARIANT"));
        insertGeneralRow("2026-10-01 10:00:00+00");
        insertGeneralRow("2026-10-02 10:00:00+00");
        DuckDBTestSupport.flushInlinedData(conn);

        assertThat(dataFiles("general_orders_events"))
                .hasSize(2)
                .allSatisfy(f -> assertThat(f)
                        .matches(".*/main/general_orders_events/epoch_day=\\d+/[^/]+\\.parquet"));
    }

    @Test
    void retentionDeleteOnADayBoundary_retiresWholeFilesWithoutDeleteFiles() throws SQLException {
        exec(SchemaManager.generalCassetteDdl("lake", "general_orders_events", "VARIANT"));
        insertGeneralRow("2026-10-01 10:00:00+00");
        insertGeneralRow("2026-10-02 10:00:00+00");
        DuckDBTestSupport.flushInlinedData(conn);

        // Same predicate shape RetentionService issues: recorded_at < cutoff.
        exec("DELETE FROM lake.main.general_orders_events "
                + "WHERE recorded_at < TIMESTAMPTZ '2026-10-02 00:00:00+00'");

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*), count(delete_file) "
                     + "FROM ducklake_list_files('lake', 'general_orders_events')")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).as("the 2026-10-01 file is retired, the 2026-10-02 one stays").isEqualTo(1);
            assertThat(rs.getLong(2)).as("nothing left for ducklake_rewrite_data_files to do").isZero();
        }
    }

    // -------------------------------------------------------------------------

    private void insertEntityRow(int bucket, String recordedAt) throws SQLException {
        exec("INSERT INTO lake.main.entity_order "
                + "(recorded_at, entity_id, bucket, topic, kafka_offset, kafka_partition, kafka_timestamp) "
                + "VALUES (TIMESTAMPTZ '" + recordedAt + "', 'order-1', " + bucket + ", 'orders.events', 0, 0, "
                + "TIMESTAMPTZ '" + recordedAt + "')");
    }

    private void insertGeneralRow(String recordedAt) throws SQLException {
        exec("INSERT INTO lake.main.general_orders_events "
                + "(recorded_at, kafka_offset, kafka_partition, kafka_timestamp) "
                + "VALUES (TIMESTAMPTZ '" + recordedAt + "', 0, 0, TIMESTAMPTZ '" + recordedAt + "')");
    }

    private List<String> dataFiles(String table) throws SQLException {
        return strings("SELECT data_file FROM ducklake_list_files('lake', ?)", table);
    }

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
