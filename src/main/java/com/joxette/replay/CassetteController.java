package com.joxette.replay;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.sql.SQLException;
import java.time.Instant;

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

    public CassetteController(
            TopicReplayService topicService,
            EntityReplayService entityService,
            SseReplayHandler sseHandler) {
        this.topicService  = topicService;
        this.entityService = entityService;
        this.sseHandler    = sseHandler;
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
    // Error handling
    // =========================================================================

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> handleBadRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(ex.getMessage());
    }

    @ExceptionHandler(SQLException.class)
    public ResponseEntity<String> handleSqlError(SQLException ex) {
        return ResponseEntity.internalServerError()
                .body("Database error: " + ex.getMessage());
    }
}
