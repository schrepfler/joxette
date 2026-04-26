package com.joxette.compaction;

import com.joxette.config.JoxetteProperties;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

/**
 * REST API for compaction management.
 *
 * <pre>
 * GET  /compaction/status           current status and last run summary
 * POST /compaction/trigger          kick off a manual compaction run (async, 202)
 * GET  /compaction/history          paginated run history (newest first)
 * </pre>
 *
 * <h2>POST /compaction/trigger</h2>
 * <p>Optional JSON body:
 * <pre>
 * {
 *   "targets": ["order", "payment", "general"]
 * }
 * </pre>
 * <p>Omitting the body (or passing {@code "targets": null}) compacts all entity
 * types plus the general cassette if it is enabled.  Including {@code "general"}
 * in the targets list enables general-cassette compaction for that run regardless
 * of the {@code joxette.compaction.general.enabled} flag.
 *
 * <p>The endpoint returns {@code 202 Accepted} with the initial
 * {@link CompactionRun} record (status {@code "running"}).  Use
 * {@code GET /compaction/history} or {@code GET /compaction/status} to follow
 * progress.  Returns {@code 409 Conflict} if a compaction is already running.
 */
@Tag(name = "Compaction",
     description = "Manage DuckLake compaction runs that merge small Parquet files to reduce read amplification.")
@RestController
@RequestMapping("/compaction")
public class CompactionController {

    private static final int DEFAULT_HISTORY_LIMIT = 20;

    private final CompactionService compactionService;
    private final RetentionService retentionService;
    private final TaskScheduler compactionTaskScheduler;
    private final JoxetteProperties props;

    public CompactionController(
            CompactionService compactionService,
            RetentionService retentionService,
            @Qualifier("compactionTaskScheduler") TaskScheduler compactionTaskScheduler,
            JoxetteProperties props) {
        this.compactionService       = compactionService;
        this.retentionService        = retentionService;
        this.compactionTaskScheduler = compactionTaskScheduler;
        this.props                   = props;
    }

    // =========================================================================
    // Endpoints
    // =========================================================================

    @Operation(
        operationId = "getCompactionStatus",
        summary = "Get compaction status",
        description = "Returns the current compaction status including the most-recent run summary, " +
                      "the next scheduled cron fire time, and whether a run is currently active."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Current compaction status",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = CompactionStatus.class))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/status", produces = MediaType.APPLICATION_JSON_VALUE)
    public CompactionStatus getStatus() throws SQLException {
        return compactionService.getStatus();
    }

    /**
     * Triggers a manual compaction run asynchronously.
     *
     * <p>The run record is inserted synchronously (so the response contains a
     * valid {@code id}) and the actual work is submitted to the
     * {@code compactionTaskScheduler}.
     *
     * @param body optional; if absent all entity types are targeted
     * @return 202 with the initial run record, or 409 if already running
     */
    @Operation(
        operationId = "triggerCompaction",
        summary = "Trigger a manual compaction run",
        description = "Asynchronously starts a compaction run. The run record is inserted synchronously so the " +
                      "response contains a valid id with status `running`. " +
                      "Omitting the request body (or passing `targets: null`) compacts all entity types plus the " +
                      "general cassette if it is enabled. Including `general` in targets enables general-cassette " +
                      "compaction for that run regardless of the `joxette.compaction.general.enabled` flag."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "202", description = "Compaction run accepted; body contains the initial run record",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = CompactionRun.class),
                examples = @ExampleObject(name = "accepted", value = """
                    {
                      "id": 8,
                      "startedAt": "2024-06-01T04:00:00Z",
                      "completedAt": null,
                      "status": "running",
                      "triggeredBy": "manual",
                      "targets": ["order", "payment"],
                      "entityBucketsCompacted": 0,
                      "generalPartitionsCompacted": 0,
                      "errorMessage": null
                    }"""))),
        @ApiResponse(responseCode = "409", description = "A compaction run is already in progress",
            content = @Content(mediaType = "application/problem+json",
                schema = @Schema(type = "object"),
                examples = @ExampleObject(value = """
                    {
                      "type": "https://joxette.dev/problems/conflict",
                      "title": "Conflict",
                      "status": 409,
                      "detail": "Compaction already in progress",
                      "errorCode": "ERR_CONFLICT"
                    }"""))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @PostMapping(value = "/trigger", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<CompactionRun> trigger(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                description = "Optional list of entity-type targets. Omit or set targets to null to compact all.",
                required = false,
                content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = TriggerRequest.class),
                    examples = @ExampleObject(value = "{\"targets\": [\"order\", \"payment\"]}")))
            @Valid @RequestBody(required = false) TriggerRequest body) throws SQLException {
        List<String> targets = (body != null) ? body.targets() : null;
        CompactionRun run = compactionService.beginRun("manual", targets);
        compactionTaskScheduler.schedule(
                () -> compactionService.executeRun(run.id(), targets),
                Instant.now());
        return ResponseEntity.accepted().body(run);
    }

    @Operation(
        operationId = "getCompactionHistory",
        summary = "Get compaction run history",
        description = "Returns the most-recent compaction runs, newest first. " +
                      "The default page size is " + DEFAULT_HISTORY_LIMIT + "."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "List of compaction runs (newest first)",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(type = "array", implementation = CompactionRun.class))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/history", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<CompactionRun> getHistory(
            @Parameter(description = "Maximum number of runs to return (default " + DEFAULT_HISTORY_LIMIT + ")",
                       example = "20")
            @RequestParam(defaultValue = "" + DEFAULT_HISTORY_LIMIT) int limit) throws SQLException {
        return compactionService.getHistory(limit);
    }

    @Operation(
        operationId = "getCompactionConfig",
        summary = "Get effective compaction configuration",
        description = "Returns the runtime compaction settings derived from application properties: " +
                      "cron schedule, entity and general compaction thresholds."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Effective compaction configuration",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = CompactionConfig.class)))
    })
    @GetMapping(value = "/config", produces = MediaType.APPLICATION_JSON_VALUE)
    public CompactionConfig getConfig() {
        JoxetteProperties.Compaction c = props.getCompaction();
        return new CompactionConfig(
                c.getSchedule(),
                new CompactionConfig.EntityConfig(
                        c.getEntity().getLookbackDays(),
                        c.getEntity().getMinFilesPerBucket(),
                        c.getEntity().getTargetFileSizeMb()),
                new CompactionConfig.GeneralConfig(
                        c.getGeneral().isEnabled(),
                        c.getGeneral().getLookbackDays(),
                        c.getGeneral().getMinFilesPerPartition(),
                        c.getGeneral().getTargetFileSizeMb()));
    }

    @Operation(
        operationId = "getRetentionStatus",
        summary = "Get retention enforcement status",
        description = "Returns the current retention status including the most-recent run summary " +
                      "(with per-table row counts), the next scheduled cron fire time, and whether " +
                      "a run is currently active."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Current retention status",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = RetentionStatus.class))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/retention-status", produces = MediaType.APPLICATION_JSON_VALUE)
    public RetentionStatus getRetentionStatus() throws SQLException {
        return retentionService.getStatus();
    }

    // =========================================================================
    // Response / request types
    // =========================================================================

    @Schema(description = "Effective runtime compaction configuration derived from application properties")
    record CompactionConfig(
            @Schema(description = "Spring 6-field cron expression for the scheduled compaction run",
                    example = "0 0 3 * * *")
            String schedule,
            @Schema(description = "Entity cassette compaction settings")
            EntityConfig entity,
            @Schema(description = "General cassette compaction settings")
            GeneralConfig general) {

        @Schema(description = "Entity cassette compaction thresholds")
        record EntityConfig(
                @Schema(description = "Only compact rows older than this many days", example = "30")
                int lookbackDays,
                @Schema(description = "Compact a bucket when its estimated file count exceeds this threshold", example = "10")
                int minFilesPerBucket,
                @Schema(description = "Target output Parquet file size in MB", example = "256")
                int targetFileSizeMb) {}

        @Schema(description = "General cassette compaction thresholds")
        record GeneralConfig(
                @Schema(description = "Whether general cassette compaction is enabled", example = "false")
                boolean enabled,
                @Schema(description = "Only compact rows older than this many days", example = "30")
                int lookbackDays,
                @Schema(description = "Compact a partition when its estimated file count exceeds this threshold", example = "20")
                int minFilesPerPartition,
                @Schema(description = "Target output Parquet file size in MB", example = "256")
                int targetFileSizeMb) {}
    }

    @Schema(description = "Optional request body for POST /compaction/trigger",
            example = "{\"targets\": [\"order\", \"payment\"]}")
    record TriggerRequest(
            @Schema(description = "Entity-type names to compact; null or absent means all types (plus general if enabled)",
                    example = "[\"order\", \"payment\"]")
            List<String> targets) {}

}
