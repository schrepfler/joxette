package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.List;

/**
 * A single row from the general cassette ({@code lake.cassette}).
 *
 * <p>{@code value} and each header {@code value} are base64url-encoded (no
 * padding) because they are raw bytes from the Kafka wire format. A {@code null}
 * field means the column was NULL in DuckDB.
 */
@Schema(description = "A single message recorded from a Kafka topic. " +
                       "`key`, `value`, and header values are base64url-encoded (no padding) " +
                       "because they are raw bytes from the Kafka wire format. " +
                       "Null fields are omitted from the JSON response.",
        example = """
            {
              "topic": "orders",
              "partition": 0,
              "offset": 1024,
              "timestamp": "2024-06-01T12:00:00Z",
              "recordedAt": "2024-06-01T12:00:00.123Z",
              "key": "b3JkZXItNDI",
              "value": "eyJvcmRlcklkIjoiNDIiLCJzdGF0dXMiOiJwZW5kaW5nIn0",
              "headers": [
                {"key": "content-type", "value": "YXBwbGljYXRpb24vanNvbg"}
              ]
            }""")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CassetteRecord(
        @Schema(description = "Kafka topic the message was consumed from", example = "orders")
        String topic,

        @Schema(description = "Kafka partition number", example = "0")
        int partition,

        @Schema(description = "Kafka offset within the partition", example = "1024")
        long offset,

        @Schema(description = "Kafka message timestamp (event time)", example = "2024-06-01T12:00:00Z")
        Instant timestamp,

        @Schema(description = "Wall-clock time when the message was recorded into the cassette",
                example = "2024-06-01T12:00:00.123Z")
        Instant recordedAt,

        @Schema(description = "Base64url-encoded Kafka message key (no padding). Null if the key was absent.",
                example = "b3JkZXItNDI")
        String key,

        @Schema(description = "Base64url-encoded Kafka message value (no padding). Null if the value was null.",
                example = "eyJvcmRlcklkIjoiNDIiLCJzdGF0dXMiOiJwZW5kaW5nIn0")
        String value,

        @Schema(description = "Kafka message headers. Null if the message had no headers.")
        List<Header> headers
) {

    @Schema(description = "A single Kafka message header key/value pair. " +
                           "The value is base64url-encoded (no padding).")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Header(
            @Schema(description = "Header key", example = "content-type")
            String key,

            @Schema(description = "Base64url-encoded header value (no padding)",
                    example = "YXBwbGljYXRpb24vanNvbg")
            String value
    ) {}
}
