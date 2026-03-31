package com.joxette.compaction;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

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
    private final TaskScheduler compactionTaskScheduler;

    public CompactionController(
            CompactionService compactionService,
            @Qualifier("compactionTaskScheduler") TaskScheduler compactionTaskScheduler) {
        this.compactionService      = compactionService;
        this.compactionTaskScheduler = compactionTaskScheduler;
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
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(type = "object"),
                examples = @ExampleObject(value = "{\"error\": \"Compaction already running\"}"))),
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
            @RequestBody(required = false) TriggerRequest body) throws SQLException {
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

    // =========================================================================
    // Request types
    // =========================================================================

    @Schema(description = "Optional request body for POST /compaction/trigger",
            example = "{\"targets\": [\"order\", \"payment\"]}")
    record TriggerRequest(
            @Schema(description = "Entity-type names to compact; null or absent means all types (plus general if enabled)",
                    example = "[\"order\", \"payment\"]")
            List<String> targets) {}

    // =========================================================================
    // Error handlers
    // =========================================================================

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, String>> handleConflict(IllegalStateException ex) {
        return ResponseEntity.status(409).body(Map.of("error", ex.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> handleBadRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(ex.getMessage());
    }

    @ExceptionHandler(NoSuchElementException.class)
    public ResponseEntity<Void> handleNotFound(NoSuchElementException ex) {
        return ResponseEntity.notFound().build();
    }

    @ExceptionHandler(SQLException.class)
    public ResponseEntity<String> handleSqlError(SQLException ex) {
        return ResponseEntity.internalServerError()
                .body("Database error: " + ex.getMessage());
    }
}
