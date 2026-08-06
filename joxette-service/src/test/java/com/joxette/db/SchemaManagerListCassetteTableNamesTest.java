package com.joxette.db;

import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaManagerListCassetteTableNamesTest {

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
    void listCassetteTableNames_returnsOnlyGeneralAndEntityTablesSorted() throws Exception {
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, "orders.events");
        DuckDBTestSupport.createEntityTable(duckDB, "order");
        DuckDBTestSupport.createEntityTable(duckDB, "customer");

        List<String> tables = SchemaManager.listCassetteTableNames(duckDB);

        assertThat(tables).containsExactly("entity_customer", "entity_order", "general_orders_events");
    }

    @Test
    void listCassetteTableNames_returnsEmptyWhenNoCassetteTablesExist() throws Exception {
        List<String> tables = SchemaManager.listCassetteTableNames(duckDB);

        assertThat(tables).isEmpty();
    }
}
