package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.List;

/**
 * A single deduplicated row from an entity cassette ({@code lake.entity_{type}}).
 */
@Schema(description = "A single deduplicated event from an entity cassette. " +
                       "`key`, `value`, and header values are base64url-encoded (no padding). " +
                       "Null fields are omitted from the JSON response.",
        example = """
            {
              "entityId": "cust-042",
              "entityBucket": 5,
              "topic": "customer-events",
              "partition": 1,
              "offset": 8800,
              "timestamp": "2024-06-01T10:00:00Z",
              "recordedAt": "2024-06-01T10:00:00.456Z",
              "key": "Y3VzdC0wNDI",
              "value": "eyJldmVudCI6InVwZGF0ZWQiLCJlbWFpbCI6InRlc3RAZXhhbXBsZS5jb20ifQ",
              "headers": []
            }""")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EntityRecord(
        @Schema(description = "Entity identifier", example = "cust-042")
        String entityId,

        @Schema(description = "Consistent hash bucket for the entity ID", example = "5")
        int entityBucket,

        @Schema(description = "Source Kafka topic", example = "customer-events")
        String topic,

        @Schema(description = "Source Kafka partition number", example = "1")
        int partition,

        @Schema(description = "Source Kafka offset within the partition", example = "8800")
        long offset,

        @Schema(description = "Kafka message timestamp (event time)", example = "2024-06-01T10:00:00Z")
        Instant timestamp,

        @Schema(description = "Wall-clock time when the event was recorded into the cassette",
                example = "2024-06-01T10:00:00.456Z")
        Instant recordedAt,

        @Schema(description = "Base64url-encoded Kafka message key (no padding). Null if absent.",
                example = "Y3VzdC0wNDI")
        String key,

        @Schema(description = "Base64url-encoded Kafka message value (no padding). Null if null.",
                example = "eyJldmVudCI6InVwZGF0ZWQifQ")
        String value,

        @Schema(description = "Kafka message headers. Null if the message had no headers.")
        List<CassetteRecord.Header> headers
) {}
