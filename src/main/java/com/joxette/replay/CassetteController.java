package com.joxette.replay;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * REST endpoints for cassette replay.
 *
 * <h2>Content negotiation</h2>
 * <p>Replay endpoints ({@code /topics/{topic}} and
 * {@code /entities/{type}/{id}}) support three response formats selected via
 * the {@code Accept} header:
 * <dl>
 *   <dt>{@code application/json} (default)</dt>
 *   <dd>Cursor-paginated JSON: {@code {"data":[…], "nextCursor":"…", "hasMore":true}}.</dd>
 *   <dt>{@code text/event-stream}</dt>
 *   <dd>Server-Sent Events. Each event carries one JSON record as its data.</dd>
 *   <dt>{@code application/x-ndjson}</dt>
 *   <dd>Newline-delimited JSON. One JSON object per line, flushed incrementally.</dd>
 * </dl>
 *
 * <h2>Error handling</h2>
 * <p>Invalid entity-type names (must match {@code [a-z][a-z0-9_]*}) and
 * malformed cursors return HTTP 400.
 */
@RestController
@RequestMapping("/cassettes")
public class CassetteController {

    private static final int DEFAULT_LIMIT = 100;

    private final TopicReplayService topicService;
    private final EntityReplayService entityService;
    private final SseReplayHandler sseHandler;
    private final CassetteLifecycleService lifecycle;

    public CassetteController(
            TopicReplayService topicService,
            EntityReplayService entityService,
            SseReplayHandler sseHandler,
            CassetteLifecycleService lifecycle) {
        this.topicService  = topicService;
        this.entityService = entityService;
        this.sseHandler    = sseHandler;
        this.lifecycle     = lifecycle;
    }

    // =========================================================================
    // General cassette – topic replay
    // =========================================================================

    @GetMapping(value = "/topics/{topic}", produces = MediaType.APPLICATION_JSON_VALUE)
    public PagedResponse<CassetteRecord> getTopicJson(
            @PathVariable String topic,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to,
            @RequestParam(required = false) Integer partition,
            @RequestParam(name = "offset_from", required = false) Long offsetFrom,
            @RequestParam(name = "offset_to",   required = false) Long offsetTo,
            @RequestParam(defaultValue = "" + DEFAULT_LIMIT) int limit,
            @RequestParam(required = false) String cursor
    ) throws SQLException {
        return topicService.query(topic, from, to, partition, offsetFrom, offsetTo, limit, cursor);
    }

    @GetMapping(value = "/topics/{topic}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter getTopicSse(
            @PathVariable String topic,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to,
            @RequestParam(required = false) Integer partition,
            @RequestParam(name = "offset_from", required = false) Long offsetFrom,
            @RequestParam(name = "offset_to",   required = false) Long offsetTo
    ) {
        return sseHandler.<CassetteRecord>streamSse(
                sink -> topicService.streamAll(topic, from, to, partition, offsetFrom, offsetTo, sink));
    }

    @GetMapping(value = "/topics/{topic}", produces = "application/x-ndjson")
    public ResponseEntity<StreamingResponseBody> getTopicNdjson(
            @PathVariable String topic,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to,
            @RequestParam(required = false) Integer partition,
            @RequestParam(name = "offset_from", required = false) Long offsetFrom,
            @RequestParam(name = "offset_to",   required = false) Long offsetTo
    ) {
        StreamingResponseBody body = sseHandler.<CassetteRecord>streamNdjson(
                sink -> topicService.streamAll(topic, from, to, partition, offsetFrom, offsetTo, sink));
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("application/x-ndjson"))
                .body(body);
    }

    // =========================================================================
    // Known-entity list & search
    // =========================================================================

    @GetMapping(value = "/entities/{entityType}", produces = MediaType.APPLICATION_JSON_VALUE)
    public PagedResponse<EntityInfo> listEntities(
            @PathVariable String entityType,
            @RequestParam(defaultValue = "" + DEFAULT_LIMIT) int limit,
            @RequestParam(required = false) String cursor
    ) throws SQLException {
        return entityService.listEntities(entityType, limit, cursor);
    }

    @GetMapping(value = "/entities/{entityType}/search", produces = MediaType.APPLICATION_JSON_VALUE)
    public PagedResponse<EntityInfo> searchEntities(
            @PathVariable String entityType,
            @RequestParam(defaultValue = "") String q,
            @RequestParam(defaultValue = "" + DEFAULT_LIMIT) int limit,
            @RequestParam(required = false) String cursor
    ) throws SQLException {
        return entityService.searchEntities(entityType, q, limit, cursor);
    }

    // =========================================================================
    // Entity event replay
    // =========================================================================

    @GetMapping(value = "/entities/{entityType}/{entityId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public PagedResponse<EntityRecord> getEntityJson(
            @PathVariable String entityType,
            @PathVariable String entityId,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to,
            @RequestParam(defaultValue = "" + DEFAULT_LIMIT) int limit,
            @RequestParam(required = false) String cursor
    ) throws SQLException {
        return entityService.queryEntityEvents(entityType, entityId, from, to, limit, cursor);
    }

    @GetMapping(value = "/entities/{entityType}/{entityId}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter getEntitySse(
            @PathVariable String entityType,
            @PathVariable String entityId,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to
    ) {
        return sseHandler.<EntityRecord>streamSse(
                sink -> entityService.streamEntityEvents(entityType, entityId, from, to, sink));
    }

    @GetMapping(value = "/entities/{entityType}/{entityId}", produces = "application/x-ndjson")
    public ResponseEntity<StreamingResponseBody> getEntityNdjson(
            @PathVariable String entityType,
            @PathVariable String entityId,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to
    ) {
        StreamingResponseBody body = sseHandler.<EntityRecord>streamNdjson(
                sink -> entityService.streamEntityEvents(entityType, entityId, from, to, sink));
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("application/x-ndjson"))
                .body(body);
    }

    // =========================================================================
    // Entity stats
    // =========================================================================

    @GetMapping(value = "/entities/{entityType}/{entityId}/stats",
                produces = MediaType.APPLICATION_JSON_VALUE)
    public EntityStats getEntityStats(
            @PathVariable String entityType,
            @PathVariable String entityId
    ) throws SQLException {
        return entityService.getEntityStats(entityType, entityId);
    }

    // =========================================================================
    // Cassette lifecycle – stats, compaction, truncation
    // =========================================================================

    @GetMapping(value = "/topics/{topic}/stats", produces = MediaType.APPLICATION_JSON_VALUE)
    public CassetteStats getTopicStats(@PathVariable String topic) throws SQLException {
        return lifecycle.getTopicCassetteStats(topic);
    }

    /** Triggers a DuckDB CHECKPOINT to flush the WAL for the given topic's cassette. */
    @PostMapping("/topics/{topic}/compact")
    public ResponseEntity<Void> compactTopic(@PathVariable String topic) throws SQLException {
        lifecycle.compactTopicCassette(topic);
        return ResponseEntity.accepted().build();
    }

    /** Deletes all rows for {@code topic} from {@code lake.cassette}. */
    @PostMapping(value = "/topics/{topic}/truncate", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Long>> truncateTopic(@PathVariable String topic)
            throws SQLException {
        long deleted = lifecycle.truncateTopicCassette(topic);
        return ResponseEntity.ok(Map.of("deleted", deleted));
    }

    // =========================================================================
    // GDPR entity deletion
    // =========================================================================

    /**
     * Permanently deletes all data for the given entity (right to erasure).
     * Removes rows from {@code lake.entity_{entityType}} and
     * {@code lake.known_entities}.
     */
    @DeleteMapping(value = "/entities/{entityType}/{entityId}",
                   produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Long>> deleteEntity(
            @PathVariable String entityType,
            @PathVariable String entityId) throws SQLException {
        long deleted = lifecycle.deleteEntityFromCassette(entityType, entityId);
        return ResponseEntity.ok(Map.of("deleted", deleted));
    }

    // =========================================================================
    // Snapshot management
    // =========================================================================

    @GetMapping(value = "/snapshots", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<SnapshotInfo> listSnapshots() throws SQLException {
        return lifecycle.listSnapshots();
    }

    @PostMapping(value = "/snapshots",
                 consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<SnapshotInfo> createSnapshot(@RequestBody Map<String, String> body)
            throws SQLException {
        String name = body.getOrDefault("name",
                "snapshot-" + Instant.now().toEpochMilli());
        SnapshotInfo info = lifecycle.createSnapshot(name);
        return ResponseEntity.status(201).body(info);
    }

    @PostMapping(value = "/snapshots/{name}/restore")
    public ResponseEntity<Void> restoreSnapshot(@PathVariable String name) throws SQLException {
        lifecycle.restoreSnapshot(name);
        return ResponseEntity.ok().build();
    }

    // =========================================================================
    // Error handling
    // =========================================================================

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> handleBadRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(ex.getMessage());
    }

    @ExceptionHandler(NoSuchElementException.class)
    public ResponseEntity<String> handleNotFound(NoSuchElementException ex) {
        return ResponseEntity.notFound().build();
    }

    @ExceptionHandler(SQLException.class)
    public ResponseEntity<String> handleSqlError(SQLException ex) {
        return ResponseEntity.internalServerError()
                .body("Database error: " + ex.getMessage());
    }
}
