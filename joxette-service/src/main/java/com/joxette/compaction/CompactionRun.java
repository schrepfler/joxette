package com.joxette.compaction;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.List;

/**
 * Immutable snapshot of a single compaction run, as recorded in
 * {@code lake.compaction_history}.
 */
@Schema(description = "Snapshot of a single compaction run as recorded in lake.compaction_history",
        example = """
        {
          "id": 7,
          "startedAt": "2024-06-01T03:00:00Z",
          "completedAt": "2024-06-01T03:00:12Z",
          "status": "completed",
          "triggeredBy": "scheduled",
          "targets": null,
          "entityBucketsCompacted": 42,
          "generalPartitionsCompacted": 0,
          "filesProcessed": 120,
          "filesCreated": 15,
          "errorMessage": null
        }""")
public record CompactionRun(
        @Schema(description = "Auto-incremented run identifier", example = "7")
        long id,
        @Schema(description = "Timestamp when the run started", example = "2024-06-01T03:00:00Z")
        Instant startedAt,
        @Schema(description = "Timestamp when the run finished; null while still running",
                example = "2024-06-01T03:00:12Z")
        Instant completedAt,
        @Schema(description = "Run status", example = "completed")
        RunStatus status,
        @Schema(description = "What triggered the run", example = "scheduled")
        TriggerSource triggeredBy,
        @Schema(description = "Entity types targeted by this run; null means all entity types (plus general if enabled)",
                example = "[\"order\", \"payment\"]")
        List<String> targets,
        @Schema(description = "Number of entity partition buckets compacted", example = "42")
        int entityBucketsCompacted,
        @Schema(description = "Number of general-cassette topics compacted", example = "0")
        int generalPartitionsCompacted,
        @Schema(description = "Total input files merged across all compacted tables "
                + "(sum of files_processed from ducklake_merge_adjacent_files)", example = "120")
        long filesProcessed,
        @Schema(description = "Total output files written across all compacted tables "
                + "(sum of files_created from ducklake_merge_adjacent_files)", example = "15")
        long filesCreated,
        @Schema(description = "Error message if the run failed; null on success")
        String errorMessage
) {}
