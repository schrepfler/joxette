package com.joxette.compaction;

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
    @PostMapping(value = "/trigger", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<CompactionRun> trigger(
            @RequestBody(required = false) TriggerRequest body) throws SQLException {
        List<String> targets = (body != null) ? body.targets() : null;
        CompactionRun run = compactionService.beginRun("manual", targets);
        compactionTaskScheduler.schedule(
                () -> compactionService.executeRun(run.id(), targets),
                Instant.now());
        return ResponseEntity.accepted().body(run);
    }

    @GetMapping(value = "/history", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<CompactionRun> getHistory(
            @RequestParam(defaultValue = "" + DEFAULT_HISTORY_LIMIT) int limit) throws SQLException {
        return compactionService.getHistory(limit);
    }

    // =========================================================================
    // Request types
    // =========================================================================

    record TriggerRequest(List<String> targets) {}

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
