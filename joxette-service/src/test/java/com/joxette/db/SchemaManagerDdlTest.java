package com.joxette.db;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** String checks on the cassette DDL; {@link DuckLakeTableLayoutTest} runs it against real DuckLake. */
class SchemaManagerDdlTest {

    @Test
    void generalDdl_declaresSortOrderInline() {
        String ddl = SchemaManager.generalCassetteDdl("lake", "general_orders_events", "VARIANT");

        assertThat(ddl)
                .contains("CREATE TABLE IF NOT EXISTS lake.main.general_orders_events (")
                .contains("SORTED BY (kafka_timestamp, kafka_partition, kafka_offset)")
                .doesNotContain("ASC"); // DuckLake rejects ASC inside an inline SORTED BY
    }

    @Test
    void entityDdl_declaresBucketPartitioningAndSortOrderInline() {
        String ddl = SchemaManager.entityCassetteDdl("lake", "entity_order", "VARIANT");

        assertThat(ddl)
                .contains("CREATE TABLE IF NOT EXISTS lake.main.entity_order (")
                .contains("PARTITIONED BY (bucket)")
                .contains("SORTED BY (entity_id, kafka_timestamp, recorded_at)")
                .doesNotContain("ASC");
    }

    @Test
    void ddl_usesJsonWhenVariantIsUnsupported() {
        assertThat(SchemaManager.entityCassetteDdl("lake", "entity_order", "JSON"))
                .contains("metadata        JSON");
    }
}
