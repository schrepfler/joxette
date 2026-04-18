package com.joxette.replay;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Row-count and size statistics for a topic's slice of the general cassette.
 *
 * <p>{@code estimatedSizeBytes} is the DuckDB-reported estimated size of the
 * whole {@code lake.cassette} table (not per-topic) and is intended as an
 * order-of-magnitude guide only.
 */
@Schema(description = "Row-count and estimated size statistics for a topic's slice of the general cassette. " +
                       "Note: `estimatedSizeBytes` reflects the whole `lake.cassette` table, not a per-topic slice.",
        example = """
            {
              "topic": "orders",
              "tableName": "lake.cassette",
              "rowCount": 1048576,
              "estimatedSizeBytes": 536870912
            }""")
public record CassetteStats(
        @Schema(description = "Kafka topic name", example = "orders")
        String topic,

        @Schema(description = "DuckDB table containing this topic's data", example = "lake.cassette")
        String tableName,

        @Schema(description = "Number of rows recorded for this topic", example = "1048576")
        long rowCount,

        @Schema(description = "DuckDB-estimated size of the whole cassette table in bytes (order-of-magnitude guide only)",
                example = "536870912")
        long estimatedSizeBytes
) {}
