package com.joxette.replay;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

/**
 * Per-bucket storage statistics for an entity cassette table.
 *
 * <p>{@code totalEstimatedSizeBytes} is the DuckDB-reported estimated size of
 * the whole entity table; per-bucket sizes are distributed proportionally by
 * row count and are approximate.
 */
@Schema(description = "Per-bucket storage statistics for an entity cassette.",
        example = """
            {
              "entityType": "order",
              "tableName": "lake.main.entity_order",
              "totalRows": 500000,
              "totalEstimatedSizeBytes": 134217728,
              "buckets": [
                {"bucket": 0, "rowCount": 2012, "estimatedSizeBytes": 539648},
                {"bucket": 1, "rowCount": 1987, "estimatedSizeBytes": 532941}
              ]
            }""")
public record EntityStorageStats(
        @Schema(description = "Entity type name", example = "order")
        String entityType,

        @Schema(description = "Fully-qualified DuckLake table name", example = "lake.main.entity_order")
        String tableName,

        @Schema(description = "Total row count across all buckets", example = "500000")
        long totalRows,

        @Schema(description = "DuckDB-estimated total size of the entity table in bytes (order-of-magnitude guide only)",
                example = "134217728")
        long totalEstimatedSizeBytes,

        @Schema(description = "Per-bucket breakdown, ordered by bucket number")
        List<BucketStats> buckets
) {
    @Schema(description = "Row count and proportional size estimate for one entity bucket.")
    public record BucketStats(
            @Schema(description = "Entity bucket number", example = "0")
            int bucket,

            @Schema(description = "Number of rows in this bucket", example = "2012")
            long rowCount,

            @Schema(description = "Proportional estimated size in bytes (totalEstimatedSizeBytes × rowCount / totalRows)",
                    example = "539648")
            long estimatedSizeBytes
    ) {}
}
