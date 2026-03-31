package com.joxette.compaction;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

/**
 * Live status summary returned by {@code GET /compaction/status}.
 *
 * @param lastRun           most-recent entry from {@code lake.compaction_history};
 *                          {@code null} if no run has ever occurred
 * @param nextScheduledRun  next cron fire time, or {@code null} if it cannot be
 *                          determined (e.g. the cron expression is invalid)
 * @param running           {@code true} while a compaction is actively executing
 */
@Schema(description = "Live compaction status summary", example = """
        {
          "lastRun": {
            "id": 7,
            "startedAt": "2024-06-01T03:00:00Z",
            "completedAt": "2024-06-01T03:00:12Z",
            "status": "completed",
            "triggeredBy": "scheduled",
            "targets": null,
            "entityBucketsCompacted": 42,
            "generalPartitionsCompacted": 0,
            "errorMessage": null
          },
          "nextScheduledRun": "2024-06-02T03:00:00Z",
          "running": false
        }""")
public record CompactionStatus(
        @Schema(description = "Most-recent compaction run; null if no run has ever occurred")
        CompactionRun lastRun,
        @Schema(description = "Next scheduled cron fire time; null if the cron expression is invalid or no schedule is configured",
                example = "2024-06-02T03:00:00Z")
        Instant nextScheduledRun,
        @Schema(description = "True while a compaction is actively executing", example = "false")
        boolean running
) {}
