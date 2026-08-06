package com.joxette.reconciliation;

import com.joxette.compaction.RunStatus;
import com.joxette.compaction.TriggerSource;

import java.time.Instant;
import java.util.List;

/** Snapshot of a single reconciliation audit run recorded in {@code reconciliation_history}. */
public record ReconciliationRun(
        long id,
        Instant startedAt,
        Instant completedAt,
        RunStatus status,
        TriggerSource triggeredBy,
        List<String> targets,
        int tablesScanned,
        int orphanedFiles,
        long orphanedBytes,
        int missingFiles,
        long missingBytes,
        int recoveredFiles,
        boolean recoveryRequested,
        String errorMessage
) {}
