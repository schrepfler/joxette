package com.joxette.db;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** String checks on the cassette DDL; {@link DuckLakeTableLayoutTest} runs it against real DuckLake. */
class SchemaManagerDdlTest {

    @Test
    void generalDdl_partitionsByDayAndDeclaresSortOrderInline() {
        String ddl = SchemaManager.generalCassetteDdl("lake", "general_orders_events", "VARIANT");

        assertThat(ddl)
                .contains("CREATE TABLE IF NOT EXISTS lake.main.general_orders_events (")
                .contains("PARTITIONED BY (epoch_day(recorded_at))")
                .contains("SORTED BY (kafka_timestamp, kafka_partition, kafka_offset)")
                .doesNotContain("ASC"); // DuckLake rejects ASC inside an inline SORTED BY
    }

    @Test
    void entityDdl_partitionsByBucketAndMonthAndDeclaresSortOrderInline() {
        String ddl = SchemaManager.entityCassetteDdl("lake", "entity_order", "VARIANT");

        assertThat(ddl)
                .contains("CREATE TABLE IF NOT EXISTS lake.main.entity_order (")
                .contains("PARTITIONED BY (bucket, epoch_month(recorded_at))")
                .contains("SORTED BY (entity_id, kafka_timestamp, recorded_at)")
                .doesNotContain("ASC");
    }

    @Test
    void ddl_usesJsonWhenVariantIsUnsupported() {
        assertThat(SchemaManager.entityCassetteDdl("lake", "entity_order", "JSON"))
                .contains("metadata        JSON");
    }

    @Test
    void ddl_skipsStatsOnlyOnUnfilteredNonVariantPayloadColumns() {
        for (String ddl : new String[] {
                SchemaManager.generalCassetteDdl("lake", "general_orders_events", "VARIANT"),
                SchemaManager.entityCassetteDdl("lake", "entity_order", "VARIANT")}) {
            assertThat(ddl)
                    .contains("WITH (skip_stats_columns = ['kafka_value', 'headers'])")
                    // VARIANT columns can't skip stats; kafka_key is filtered by the $.key pushdown
                    .doesNotContain("'metadata'")
                    .doesNotContain("'kafka_key'");
        }
    }
}
