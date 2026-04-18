package com.joxette.replay;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

/** Metadata record for a DuckDB EXPORT DATABASE snapshot. */
@Schema(description = "Metadata for a DuckDB EXPORT DATABASE snapshot.",
        example = """
            {
              "name": "snapshot-before-migration",
              "createdAt": "2024-06-01T00:00:00Z",
              "sizeBytes": 1073741824
            }""")
public record SnapshotInfo(
        @Schema(description = "Unique snapshot name", example = "snapshot-before-migration")
        String name,

        @Schema(description = "Wall-clock time when the snapshot was created", example = "2024-06-01T00:00:00Z")
        Instant createdAt,

        @Schema(description = "Total size of the snapshot directory in bytes", example = "1073741824")
        long sizeBytes
) {}
