package com.joxette.compaction;

import java.time.Instant;
import java.util.List;

/**
 * Immutable snapshot of a single compaction run, as recorded in
 * {@code lake.compaction_history}.
 */
public record CompactionRun(
        long id,
        Instant startedAt,
        Instant completedAt,
        /** One of {@code "running"}, {@code "completed"}, {@code "failed"}. */
        String status,
        /** {@code "scheduled"} or {@code "manual"}. */
        String triggeredBy,
        /** {@code null} means all entity types (+ general if enabled). */
        List<String> targets,
        int entityBucketsCompacted,
        int generalPartitionsCompacted,
        String errorMessage
) {}
