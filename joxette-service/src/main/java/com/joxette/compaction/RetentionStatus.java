package com.joxette.compaction;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

/**
 * Live status summary returned by {@code GET /compaction/retention-status}.
 *
 * @param lastRun           most-recent entry from {@code retention_history};
 *                          {@code null} if no run has ever occurred
 * @param nextScheduledRun  next cron fire time, or {@code null} if it cannot be
 *                          determined (e.g. the cron expression is invalid)
 * @param running           {@code true} while a retention run is actively executing
 */
@Schema(description = "Live retention enforcement status summary", example = """
        {
          "lastRun": {
            "id": 3,
            "startedAt": "2024-06-01T01:00:00Z",
            "completedAt": "2024-06-01T01:00:04Z",
            "status": "completed",
            "triggeredBy": "scheduled",
            "entityRowsDeleted": 12500,
            "generalRowsDeleted": 8300,
            "knownEntitiesDeleted": 47,
            "errorMessage": null
          },
          "nextScheduledRun": "2024-06-02T01:00:00Z",
          "running": false
        }""")
public record RetentionStatus(
        @Schema(description = "Most-recent retention run; null if no run has ever occurred")
        RetentionRun lastRun,
        @Schema(description = "Next scheduled cron fire time; null if the cron expression is invalid",
                example = "2024-06-02T01:00:00Z")
        Instant nextScheduledRun,
        @Schema(description = "True while a retention run is actively executing", example = "false")
        boolean running
) {}
