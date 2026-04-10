package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

/**
 * Request body for replay-to-topic operations.
 *
 * <p>The {@code targetTopic} is the only required field.  All filter fields
 * ({@code from}, {@code to}, {@code partition}, {@code offsetFrom},
 * {@code offsetTo}) are optional and mirror the filter parameters available on
 * the corresponding cassette replay endpoints.  The {@code partition},
 * {@code offsetFrom}, and {@code offsetTo} fields apply only to general
 * (topic) cassette replays; they are ignored for entity cassette replays.
 */
@Schema(description = "Request body for replay-to-topic operations",
        example = """
            {
              "targetTopic": "orders-replay",
              "from": "2024-01-01T00:00:00Z",
              "to":   "2024-12-31T23:59:59Z"
            }""")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ReplayToTopicRequest(

        @Schema(description = "Target Kafka topic to produce messages into", required = true,
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
        Long offsetTo

) {
    public ReplayToTopicRequest {
        if (targetTopic == null || targetTopic.isBlank()) {
            throw new IllegalArgumentException("targetTopic is required and must not be blank");
        }
    }
}
