package com.joxette.replay;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Dimension-cardinality breakdown for a cassette, scoped to an optional
 * time window. Counts are computed after the same at-least-once
 * deduplication normal replay reads apply.
 */
@Schema(description = "Dimension-cardinality breakdown for a cassette, scoped to an optional time window. "
                     + "Each dimension's value list is capped at the top 20 by count; any remainder is folded "
                     + "into a single {\"value\": \"__other__\"} entry.",
        example = """
            {
              "totalRecords": 128456,
              "from": "2026-08-24T00:00:00Z",
              "to": "2026-08-24T12:00:00Z",
              "dimensions": {
                "partition": [
                  {"value": "3", "count": 40213},
                  {"value": "1", "count": 38010}
                ],
                "messageType": [
                  {"value": "OrderCreated", "count": 88012},
                  {"value": null, "count": 1200}
                ]
              }
            }""")
public record CassetteSummary(
    @Schema(description = "Total deduplicated records in the scoped window", example = "128456")
    long totalRecords,
    @Schema(description = "Start of the scoped time window, or null if unbounded", example = "2026-08-24T00:00:00Z")
    Instant from,
    @Schema(description = "End of the scoped time window, or null if unbounded", example = "2026-08-24T12:00:00Z")
    Instant to,
    @Schema(description = "Per-dimension top-20 value counts, keyed by dimension name "
                         + "(\"partition\"+\"messageType\" for topics, \"sourceTopic\"+\"messageType\" for entities)")
    Map<String, List<ValueCount>> dimensions
) {}
