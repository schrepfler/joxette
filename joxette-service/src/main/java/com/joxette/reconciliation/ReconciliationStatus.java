package com.joxette.reconciliation;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

/**
 * Live status summary returned by {@code GET /compaction/reconciliation-status}.
 *
 * @param lastRun          most-recent entry from {@code reconciliation_history};
 *                         {@code null} if no run has ever occurred
 * @param nextScheduledRun next cron fire time, or {@code null} if it cannot be determined
 * @param running          {@code true} while a reconciliation run is actively executing
 */
@Schema(description = "Live catalog/object-storage reconciliation status summary")
public record ReconciliationStatus(
        ReconciliationRun lastRun,
        Instant nextScheduledRun,
        boolean running
) {}
