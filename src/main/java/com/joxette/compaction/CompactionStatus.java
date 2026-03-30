package com.joxette.compaction;

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
public record CompactionStatus(
        CompactionRun lastRun,
        Instant nextScheduledRun,
        boolean running
) {}
