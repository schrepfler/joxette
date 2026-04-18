package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

/**
 * Progress snapshot emitted during a replay-to-topic operation.
 *
 * <p>During a streaming (SSE) replay each event carries one of these objects
 * as its JSON data payload.  The final event in a successful replay has
 * {@code status = "completed"}.  If the replay fails mid-stream the last
 * event has {@code status = "failed"} and a non-null {@code errorMessage}.
 *
 * <p>For a synchronous (JSON) replay the endpoint blocks until completion and
 * returns the final {@code ReplayProgress} directly in the response body.
 *
 * <h2>Status values</h2>
 * <dl>
 *   <dt>in_progress</dt><dd>Emitted periodically while records are being produced.</dd>
 *   <dt>completed</dt><dd>All matching records have been produced successfully.</dd>
 *   <dt>failed</dt><dd>The replay was aborted due to a Kafka producer error.</dd>
 * </dl>
 */
@Schema(description = "Progress snapshot for an in-flight or completed replay-to-topic operation",
        example = """
            {
              "status": "completed",
              "targetTopic": "orders-replay",
              "sentCount": 42000,
              "errorCount": 0,
              "currentTimestamp": "2024-06-01T12:00:00Z"
            }""")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ReplayProgress(

        @Schema(description = "One of: in_progress, completed, failed", example = "completed")
        String status,

        @Schema(description = "Target Kafka topic messages are being produced into",
                example = "orders-replay")
        String targetTopic,

        @Schema(description = "Number of records successfully sent so far", example = "42000")
        long sentCount,

        @Schema(description = "Number of records that failed to produce", example = "0")
        long errorCount,

        @Schema(description = "Kafka timestamp of the last record that was attempted",
                example = "2024-06-01T12:00:00Z")
        Instant currentTimestamp,

        @Schema(description = "Error detail when status=failed; null otherwise")
        String errorMessage

) {}
