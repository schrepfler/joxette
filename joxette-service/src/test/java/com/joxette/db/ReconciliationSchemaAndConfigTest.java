package com.joxette.db;

import com.joxette.config.JoxetteProperties;
import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

class ReconciliationSchemaAndConfigTest {

    private Connection duckDB;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
    }

    @Test
    void reconciliationHistoryTable_existsWithExpectedColumns() throws Exception {
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT column_name FROM duckdb_columns() " +
                     "WHERE table_name = 'reconciliation_history' ORDER BY column_name")) {
            java.util.List<String> columns = new java.util.ArrayList<>();
            while (rs.next()) columns.add(rs.getString(1));
            assertThat(columns).containsExactlyInAnyOrder(
                    "id", "started_at", "completed_at", "status", "triggered_by",
                    "targets", "tables_scanned", "orphaned_files", "orphaned_bytes",
                    "missing_files", "missing_bytes", "recovered_files",
                    "recovery_requested", "details", "error_message");
        }
    }

    @Test
    void reconciliationHistoryTable_acceptsAnInsertWithGeneratedId() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("""
                    INSERT INTO reconciliation_history
                        (started_at, status, triggered_by, targets, recovery_requested)
                    VALUES (now(), 'running', 'manual', ['orders.events'], false)
                    """);
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM reconciliation_history")) {
                rs.next();
                assertThat(rs.getLong(1)).isEqualTo(1L);
            }
        }
    }

    @Test
    void reconciliationConfig_hasExpectedDefaults() {
        JoxetteProperties props = new JoxetteProperties();
        assertThat(props.getReconciliation().isEnabled()).isTrue();
        assertThat(props.getReconciliation().getSchedule()).isEqualTo("0 0 4 * * *");
    }
}
