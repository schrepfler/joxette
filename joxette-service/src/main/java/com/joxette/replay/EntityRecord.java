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
              "entityId": "fixture-42",
              "messageType": "marketSet",
              "topic": "events",
              "partition": 1,
              "offset": 8800,
              "timestamp": "2024-06-01T10:00:00Z",
              "recordedAt": "2024-06-01T10:00:00.456Z",
              "key": "Zml4dHVyZS00Mg",
              "value": "eyJtYXJrZXRTZXQiOnsiZml4dHVyZUlkIjoiZml4dHVyZS00MiJ9fQ",
              "headers": []
            }""")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EntityRecord(
        @Schema(description = "Entity identifier", example = "fixture-42")
        String entityId,

        @Schema(description = "Message variant label — identifies which envelope type produced this row",
                example = "marketSet")
        String messageType,

        @Schema(description = "Source Kafka topic", example = "events")
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
                example = "Zml4dHVyZS00Mg")
        String key,

        @Schema(description = "Base64url-encoded Kafka message value (no padding). Null if null.",
                example = "eyJtYXJrZXRTZXQiOnsiZml4dHVyZUlkIjoiZml4dHVyZS00MiJ9fQ")
        String value,

        @Schema(description = "Kafka message headers. Null if the message had no headers.")
        List<CassetteRecord.Header> headers
) {}
