package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.Map;

/**
 * Request body for replay-to-topic operations.
 *
 * <p>All fields are optional.  When neither {@code targetTopic} nor
 * {@code topicMappings} is supplied, each message is produced back to its
 * original source topic (identity routing).  All filter fields
 * ({@code from}, {@code to}, {@code partition}, {@code offsetFrom},
 * {@code offsetTo}) are optional and mirror the filter parameters available on
 * the corresponding cassette replay endpoints.  The {@code partition},
 * {@code offsetFrom}, and {@code offsetTo} fields apply only to general
 * (topic) cassette replays; they are ignored for entity cassette replays.
 *
 * <p>The optional {@code transforms} block enables message mutations before
 * producing: timestamp re-anchoring ({@code restamp}) and JSONPath field
 * substitutions ({@code fieldSubstitutions}).  Omit or set to {@code null} to
 * replay messages verbatim.
 */
@Schema(description = "Request body for replay-to-topic operations",
        example = """
            {
              "targetTopic": "orders-replay",
              "from": "2024-01-01T00:00:00Z",
              "to":   "2024-12-31T23:59:59Z",
              "transforms": {
                "restamp": true,
                "fieldSubstitutions": [
                  {"path": "$.order_id",    "generate": "uuid"},
                  {"path": "$.environment", "value":    "staging"}
                ]
              }
            }""")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ReplayToTopicRequest(

        @Schema(description = "Target Kafka topic to produce messages into. " +
                              "Optional when topicMappings covers all source topics; " +
                              "omit entirely for identity routing (replay back to original topics).",
                example = "orders-replay")
        String targetTopic,

        @Schema(description = "Replay only records with kafka_timestamp >= this value (ISO-8601 instant)",
                example = "2024-01-01T00:00:00Z")
        Instant from,

        @Schema(description = "Replay only records with kafka_timestamp <= this value (ISO-8601 instant)",
                example = "2024-12-31T23:59:59Z")
        Instant to,

        @Schema(description = "Filter to a single Kafka partition (topic replay only)")
        Integer partition,

        @Schema(description = "Replay only records with Kafka offset >= this value (topic replay only)")
        Long offsetFrom,

        @Schema(description = "Replay only records with Kafka offset <= this value (topic replay only)")
        Long offsetTo,

        @Schema(description = "Optional transforms applied to each message before producing. " +
                              "Omit or set to null to replay messages verbatim.")
        ReplayTransformConfig transforms,

        @Schema(description = """
                Per-source-topic routing overrides. Keys are source topic names; values are
                target topic names. Topics absent from this map fall back to `targetTopic`.
                For entity replays, `targetTopic` may be omitted if every source topic has
                an explicit entry here. For topic replays, at most one mapping key is relevant
                (the source topic itself).
                """,
                name = "topic_mappings",
                example = "{\"orders.events\":\"orders.events.staging\",\"payments.events\":\"payments.events.staging\"}")
        Map<String, String> topicMappings,

        @Schema(description = """
                Controls how source partition numbers are mapped to the target topic.
                DEFAULT (default) — Kafka default partitioner (key-hash or round-robin).
                PRESERVE — carry the exact source partition; requires equal partition counts.
                MODULO — source_partition % target_partition_count.
                """,
                defaultValue = "DEFAULT",
                name = "partition_strategy")
        PartitionStrategy partitionStrategy

) {
    public ReplayToTopicRequest {
        if (partitionStrategy == null) {
            partitionStrategy = PartitionStrategy.DEFAULT;
        }
    }
}
