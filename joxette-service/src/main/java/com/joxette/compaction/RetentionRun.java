package com.joxette.compaction;

import java.time.Instant;

/** Snapshot of a single retention enforcement run recorded in {@code lake.retention_history}. */
public record RetentionRun(
        long id,
        Instant startedAt,
        Instant completedAt,
        RunStatus status,
        TriggerSource triggeredBy,
        long entityRowsDeleted,
        long generalRowsDeleted,
        long knownEntitiesDeleted,
        String errorMessage
) {}
