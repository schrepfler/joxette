package com.joxette.management;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Response body for {@code GET /config/runtime}.
 *
 * <p>Combines two orthogonal views of the system:
 * <ul>
 *   <li>{@link DeploymentConfig} — immutable settings read from YAML at startup; a restart
 *       is required before any changes take effect.</li>
 *   <li>{@link DomainSummary} — live counts drawn from the config tables; reflects the
 *       current state of topic/entity registration.</li>
 * </ul>
 */
@Schema(description = "Effective runtime configuration and live domain summary")
public record RuntimeConfigResponse(
        @Schema(description = "Immutable deployment settings read from YAML at startup; requires restart to change")
        DeploymentConfig deployment,
        @Schema(description = "Live domain configuration summary from config tables")
        DomainSummary domain) {

    @Schema(description = "Deployment configuration — all fields are immutable at runtime")
    public record DeploymentConfig(
            @Schema(description = "Kafka bootstrap servers", example = "localhost:9092")
            String kafkaBootstrapServers,
            @Schema(description = "DuckLake catalog file path", example = "./data/joxette.ducklake")
            String catalogPath,
            @Schema(description = "Object storage root path for Parquet files; null if not configured",
                    example = "s3://joxette-data/", nullable = true)
            String objectStoragePath,
            @Schema(description = "S3-compatible endpoint override; empty string means use the AWS default credential chain",
                    example = "http://localhost:9000")
            String s3Endpoint,
            @Schema(description = "AWS S3 region passed to DuckDB", example = "us-east-1")
            String s3Region,
            @Schema(description = "Flush inlined data to Parquet once this many MB are buffered", example = "4")
            int inlineThresholdMb,
            @Schema(description = "Flush inlined data to Parquet once this many records are buffered", example = "50000")
            int inlineThresholdRecords,
            @Schema(description = "Cron expression for the scheduled compaction run (Spring 6-field format)",
                    example = "0 0 3 * * *")
            String compactionSchedule,
            @Schema(description = "Cron expression for the scheduled retention enforcement run (Spring 6-field format)",
                    example = "0 0 1 * * *")
            String retentionSchedule,
            @Schema(description = "Maximum number of messages accumulated before writing a batch", example = "10000")
            int recordingBatchSize,
            @Schema(description = "Maximum time in milliseconds to wait before flushing an incomplete batch",
                    example = "1000")
            long recordingBatchTimeoutMs,
            @Schema(description = "Entity compaction: minimum files per bucket before compaction is triggered",
                    example = "10")
            int compactionEntityMinFilesPerBucket,
            @Schema(description = "Entity compaction: target Parquet file size in MB", example = "256")
            int compactionEntityTargetFileSizeMb,
            @Schema(description = "Entity compaction: only compact data older than this many days", example = "30")
            int compactionEntityLookbackDays,
            @Schema(description = "Whether general cassette compaction is enabled", example = "false")
            boolean compactionGeneralEnabled,
            @Schema(description = "General compaction: minimum files per partition before compaction is triggered",
                    example = "20")
            int compactionGeneralMinFilesPerPartition,
            @Schema(description = "General compaction: target Parquet file size in MB", example = "256")
            int compactionGeneralTargetFileSizeMb) {}

    @Schema(description = "Live domain configuration drawn from the config tables")
    public record DomainSummary(
            @Schema(description = "Number of topics registered for recording", example = "5")
            int topicCount,
            @Schema(description = "Number of entity types registered", example = "2")
            int entityTypeCount,
            @Schema(description = "Total entity source mappings across all entity types", example = "7")
            int sourceMappingCount) {}
}
