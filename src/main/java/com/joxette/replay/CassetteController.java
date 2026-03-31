package com.joxette.replay;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
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
@Tag(name = "Cassette Replay",
     description = "Replay recorded Kafka messages stored in DuckLake. " +
                   "Replay endpoints support three response formats via the Accept header: " +
                   "application/json (cursor-paginated), text/event-stream (SSE), and application/x-ndjson (streaming).")
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

    @Operation(
        operationId = "getTopicRecordsJson",
        summary = "Replay topic records (JSON)",
        description = "Returns a cursor-paginated page of messages recorded from the given Kafka topic. " +
                      "Supports filtering by timestamp range, partition, and Kafka offset range. " +
                      "Pass `nextCursor` from the previous response to advance to the next page. " +
                      "Use `Accept: text/event-stream` for SSE streaming or `Accept: application/x-ndjson` for NDJSON streaming."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Paginated list of cassette records",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = PagedResponse.class),
                examples = @ExampleObject(name = "page", value = """
                    {
                      "data": [
                        {
                          "topic": "orders",
                          "partition": 0,
                          "offset": 1024,
                          "timestamp": "2024-06-01T12:00:00Z",
                          "recordedAt": "2024-06-01T12:00:00.123Z",
                          "key": "b3JkZXItNDI",
                          "value": "eyJvcmRlcklkIjoiNDIiLCJzdGF0dXMiOiJwZW5kaW5nIn0",
                          "headers": [
                            {"key": "content-type", "value": "YXBwbGljYXRpb24vanNvbg"}
                          ]
                        }
                      ],
                      "nextCursor": "eyJ0cyI6IjIwMjQtMDYtMDFUMTI6MDA6MDAuMTIzWiIsIm8iOjEwMjR9",
                      "hasMore": true
                    }"""))),
        @ApiResponse(responseCode = "400", description = "Invalid topic, filter value, or malformed cursor",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/topics/{topic}", produces = MediaType.APPLICATION_JSON_VALUE)
    public PagedResponse<CassetteRecord> getTopicJson(
            @Parameter(description = "Kafka topic name", required = true, example = "orders")
            @PathVariable String topic,
            @Parameter(description = "Include only records with timestamp >= this value (ISO-8601 instant)")
            @RequestParam(required = false) Instant from,
            @Parameter(description = "Include only records with timestamp <= this value (ISO-8601 instant)")
            @RequestParam(required = false) Instant to,
            @Parameter(description = "Filter to a single Kafka partition number")
            @RequestParam(required = false) Integer partition,
            @Parameter(description = "Include only records with Kafka offset >= this value", name = "offset_from")
            @RequestParam(name = "offset_from", required = false) Long offsetFrom,
            @Parameter(description = "Include only records with Kafka offset <= this value", name = "offset_to")
            @RequestParam(name = "offset_to",   required = false) Long offsetTo,
            @Parameter(description = "Maximum number of records to return per page (default 100)", example = "100")
            @RequestParam(defaultValue = "" + DEFAULT_LIMIT) int limit,
            @Parameter(description = "Opaque cursor from a previous response's `nextCursor` field")
            @RequestParam(required = false) String cursor
    ) throws SQLException {
        return topicService.query(topic, from, to, partition, offsetFrom, offsetTo, limit, cursor);
    }

    @Operation(
        operationId = "getTopicRecordsSse",
        summary = "Replay topic records (SSE)",
        description = "Streams all matching messages as Server-Sent Events (Accept: text/event-stream). " +
                      "Each `data:` field contains a single JSON-serialised `CassetteRecord`. " +
                      "The stream ends when all matching records have been sent. " +
                      "Supports the same time, partition, and offset filters as the JSON variant."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Server-Sent Event stream of cassette records",
            content = @Content(mediaType = MediaType.TEXT_EVENT_STREAM_VALUE,
                schema = @Schema(type = "string",
                    description = "Each SSE event: `data: {CassetteRecord JSON}\\n\\n`"),
                examples = @ExampleObject(name = "event", value =
                    "data: {\"topic\":\"orders\",\"partition\":0,\"offset\":1024," +
                          "\"timestamp\":\"2024-06-01T12:00:00Z\"," +
                          "\"recordedAt\":\"2024-06-01T12:00:00.123Z\"," +
                          "\"key\":\"b3JkZXItNDI\",\"value\":\"eyJvcmRlcklkIjoiNDIifQ\"}\n\n"))),
        @ApiResponse(responseCode = "400", description = "Invalid topic or filter value",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/topics/{topic}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter getTopicSse(
            @Parameter(description = "Kafka topic name", required = true, example = "orders")
            @PathVariable String topic,
            @Parameter(description = "Include only records with timestamp >= this value (ISO-8601 instant)")
            @RequestParam(required = false) Instant from,
            @Parameter(description = "Include only records with timestamp <= this value (ISO-8601 instant)")
            @RequestParam(required = false) Instant to,
            @Parameter(description = "Filter to a single Kafka partition number")
            @RequestParam(required = false) Integer partition,
            @Parameter(description = "Include only records with Kafka offset >= this value", name = "offset_from")
            @RequestParam(name = "offset_from", required = false) Long offsetFrom,
            @Parameter(description = "Include only records with Kafka offset <= this value", name = "offset_to")
            @RequestParam(name = "offset_to",   required = false) Long offsetTo
    ) {
        return sseHandler.<CassetteRecord>streamSse(
                sink -> topicService.streamAll(topic, from, to, partition, offsetFrom, offsetTo, sink));
    }

    @Operation(
        operationId = "getTopicRecordsNdjson",
        summary = "Replay topic records (NDJSON)",
        description = "Streams all matching messages as newline-delimited JSON (Accept: application/x-ndjson). " +
                      "Each line is a complete JSON-serialised `CassetteRecord`, flushed incrementally. " +
                      "Supports the same time, partition, and offset filters as the JSON variant."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "NDJSON stream of cassette records",
            content = @Content(mediaType = "application/x-ndjson",
                schema = @Schema(type = "string",
                    description = "One JSON object per line: `{CassetteRecord}\\n`"),
                examples = @ExampleObject(name = "line", value =
                    "{\"topic\":\"orders\",\"partition\":0,\"offset\":1024," +
                     "\"timestamp\":\"2024-06-01T12:00:00Z\"," +
                     "\"recordedAt\":\"2024-06-01T12:00:00.123Z\"," +
                     "\"key\":\"b3JkZXItNDI\",\"value\":\"eyJvcmRlcklkIjoiNDIifQ\"}"))),
        @ApiResponse(responseCode = "400", description = "Invalid topic or filter value",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/topics/{topic}", produces = "application/x-ndjson")
    public ResponseEntity<StreamingResponseBody> getTopicNdjson(
            @Parameter(description = "Kafka topic name", required = true, example = "orders")
            @PathVariable String topic,
            @Parameter(description = "Include only records with timestamp >= this value (ISO-8601 instant)")
            @RequestParam(required = false) Instant from,
            @Parameter(description = "Include only records with timestamp <= this value (ISO-8601 instant)")
            @RequestParam(required = false) Instant to,
            @Parameter(description = "Filter to a single Kafka partition number")
            @RequestParam(required = false) Integer partition,
            @Parameter(description = "Include only records with Kafka offset >= this value", name = "offset_from")
            @RequestParam(name = "offset_from", required = false) Long offsetFrom,
            @Parameter(description = "Include only records with Kafka offset <= this value", name = "offset_to")
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

    @Operation(
        operationId = "listEntities",
        summary = "List known entities",
        description = "Returns a cursor-paginated list of entity IDs registered under the given entity type, " +
                      "ordered by entity ID ascending."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Paginated list of known entities",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = PagedResponse.class),
                examples = @ExampleObject(name = "page", value = """
                    {
                      "data": [
                        {
                          "entityType": "customer",
                          "entityId": "cust-001",
                          "entityBucket": 3,
                          "firstSeen": "2024-01-10T08:00:00Z",
                          "lastSeen": "2024-06-01T12:00:00Z"
                        }
                      ],
                      "nextCursor": "Y3VzdC0wMDE",
                      "hasMore": true
                    }"""))),
        @ApiResponse(responseCode = "400", description = "Invalid entity type name",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/entities/{entityType}", produces = MediaType.APPLICATION_JSON_VALUE)
    public PagedResponse<EntityInfo> listEntities(
            @Parameter(description = "Entity type name (must match `[a-z][a-z0-9_]*`)", required = true, example = "customer")
            @PathVariable String entityType,
            @Parameter(description = "Maximum number of entities to return per page (default 100)", example = "100")
            @RequestParam(defaultValue = "" + DEFAULT_LIMIT) int limit,
            @Parameter(description = "Opaque cursor from a previous response's `nextCursor` field")
            @RequestParam(required = false) String cursor
    ) throws SQLException {
        return entityService.listEntities(entityType, limit, cursor);
    }

    @Operation(
        operationId = "searchEntities",
        summary = "Search entities by ID",
        description = "Returns entities whose ID contains the query string `q` (case-insensitive, substring match). " +
                      "Results are cursor-paginated and ordered by entity ID ascending."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Paginated search results",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = PagedResponse.class),
                examples = @ExampleObject(name = "results", value = """
                    {
                      "data": [
                        {
                          "entityType": "customer",
                          "entityId": "cust-042",
                          "entityBucket": 5,
                          "firstSeen": "2024-03-01T09:15:00Z",
                          "lastSeen": "2024-06-01T11:45:00Z"
                        }
                      ],
                      "hasMore": false
                    }"""))),
        @ApiResponse(responseCode = "400", description = "Invalid entity type name",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/entities/{entityType}/search", produces = MediaType.APPLICATION_JSON_VALUE)
    public PagedResponse<EntityInfo> searchEntities(
            @Parameter(description = "Entity type name (must match `[a-z][a-z0-9_]*`)", required = true, example = "customer")
            @PathVariable String entityType,
            @Parameter(description = "Substring to match against entity IDs (case-insensitive)", example = "cust-04")
            @RequestParam(defaultValue = "") String q,
            @Parameter(description = "Maximum number of results to return per page (default 100)", example = "100")
            @RequestParam(defaultValue = "" + DEFAULT_LIMIT) int limit,
            @Parameter(description = "Opaque cursor from a previous response's `nextCursor` field")
            @RequestParam(required = false) String cursor
    ) throws SQLException {
        return entityService.searchEntities(entityType, q, limit, cursor);
    }

    // =========================================================================
    // Entity event replay
    // =========================================================================

    @Operation(
        operationId = "getEntityRecordsJson",
        summary = "Replay entity events (JSON)",
        description = "Returns a cursor-paginated page of deduplicated events for the given entity from its entity cassette. " +
                      "Supports filtering by timestamp range. " +
                      "Use `Accept: text/event-stream` for SSE streaming or `Accept: application/x-ndjson` for NDJSON streaming."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Paginated list of entity event records",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = PagedResponse.class),
                examples = @ExampleObject(name = "page", value = """
                    {
                      "data": [
                        {
                          "entityId": "cust-042",
                          "entityBucket": 5,
                          "topic": "customer-events",
                          "partition": 1,
                          "offset": 8800,
                          "timestamp": "2024-06-01T10:00:00Z",
                          "recordedAt": "2024-06-01T10:00:00.456Z",
                          "key": "Y3VzdC0wNDI",
                          "value": "eyJldmVudCI6InVwZGF0ZWQiLCJlbWFpbCI6InRlc3RAZXhhbXBsZS5jb20ifQ",
                          "headers": []
                        }
                      ],
                      "nextCursor": "eyJ0cyI6IjIwMjQtMDYtMDFUMTA6MDA6MDAuNDU2WiIsIm8iOjg4MDB9",
                      "hasMore": false
                    }"""))),
        @ApiResponse(responseCode = "400", description = "Invalid entity type or malformed cursor",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/entities/{entityType}/{entityId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public PagedResponse<EntityRecord> getEntityJson(
            @Parameter(description = "Entity type name (must match `[a-z][a-z0-9_]*`)", required = true, example = "customer")
            @PathVariable String entityType,
            @Parameter(description = "Entity identifier", required = true, example = "cust-042")
            @PathVariable String entityId,
            @Parameter(description = "Include only events with timestamp >= this value (ISO-8601 instant)")
            @RequestParam(required = false) Instant from,
            @Parameter(description = "Include only events with timestamp <= this value (ISO-8601 instant)")
            @RequestParam(required = false) Instant to,
            @Parameter(description = "Maximum number of events to return per page (default 100)", example = "100")
            @RequestParam(defaultValue = "" + DEFAULT_LIMIT) int limit,
            @Parameter(description = "Opaque cursor from a previous response's `nextCursor` field")
            @RequestParam(required = false) String cursor
    ) throws SQLException {
        return entityService.queryEntityEvents(entityType, entityId, from, to, limit, cursor);
    }

    @Operation(
        operationId = "getEntityRecordsSse",
        summary = "Replay entity events (SSE)",
        description = "Streams all deduplicated events for the given entity as Server-Sent Events " +
                      "(Accept: text/event-stream). Each `data:` field contains a single JSON-serialised `EntityRecord`. " +
                      "Supports the same timestamp filters as the JSON variant."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Server-Sent Event stream of entity records",
            content = @Content(mediaType = MediaType.TEXT_EVENT_STREAM_VALUE,
                schema = @Schema(type = "string",
                    description = "Each SSE event: `data: {EntityRecord JSON}\\n\\n`"),
                examples = @ExampleObject(name = "event", value =
                    "data: {\"entityId\":\"cust-042\",\"entityBucket\":5,\"topic\":\"customer-events\"," +
                          "\"partition\":1,\"offset\":8800," +
                          "\"timestamp\":\"2024-06-01T10:00:00Z\"," +
                          "\"recordedAt\":\"2024-06-01T10:00:00.456Z\"," +
                          "\"key\":\"Y3VzdC0wNDI\",\"value\":\"eyJldmVudCI6InVwZGF0ZWQifQ\"}\n\n"))),
        @ApiResponse(responseCode = "400", description = "Invalid entity type",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/entities/{entityType}/{entityId}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter getEntitySse(
            @Parameter(description = "Entity type name (must match `[a-z][a-z0-9_]*`)", required = true, example = "customer")
            @PathVariable String entityType,
            @Parameter(description = "Entity identifier", required = true, example = "cust-042")
            @PathVariable String entityId,
            @Parameter(description = "Include only events with timestamp >= this value (ISO-8601 instant)")
            @RequestParam(required = false) Instant from,
            @Parameter(description = "Include only events with timestamp <= this value (ISO-8601 instant)")
            @RequestParam(required = false) Instant to
    ) {
        return sseHandler.<EntityRecord>streamSse(
                sink -> entityService.streamEntityEvents(entityType, entityId, from, to, sink));
    }

    @Operation(
        operationId = "getEntityRecordsNdjson",
        summary = "Replay entity events (NDJSON)",
        description = "Streams all deduplicated events for the given entity as newline-delimited JSON " +
                      "(Accept: application/x-ndjson). Each line is a complete JSON-serialised `EntityRecord`, " +
                      "flushed incrementally. Supports the same timestamp filters as the JSON variant."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "NDJSON stream of entity records",
            content = @Content(mediaType = "application/x-ndjson",
                schema = @Schema(type = "string",
                    description = "One JSON object per line: `{EntityRecord}\\n`"),
                examples = @ExampleObject(name = "line", value =
                    "{\"entityId\":\"cust-042\",\"entityBucket\":5,\"topic\":\"customer-events\"," +
                     "\"partition\":1,\"offset\":8800," +
                     "\"timestamp\":\"2024-06-01T10:00:00Z\"," +
                     "\"recordedAt\":\"2024-06-01T10:00:00.456Z\"," +
                     "\"key\":\"Y3VzdC0wNDI\",\"value\":\"eyJldmVudCI6InVwZGF0ZWQifQ\"}"))),
        @ApiResponse(responseCode = "400", description = "Invalid entity type",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/entities/{entityType}/{entityId}", produces = "application/x-ndjson")
    public ResponseEntity<StreamingResponseBody> getEntityNdjson(
            @Parameter(description = "Entity type name (must match `[a-z][a-z0-9_]*`)", required = true, example = "customer")
            @PathVariable String entityType,
            @Parameter(description = "Entity identifier", required = true, example = "cust-042")
            @PathVariable String entityId,
            @Parameter(description = "Include only events with timestamp >= this value (ISO-8601 instant)")
            @RequestParam(required = false) Instant from,
            @Parameter(description = "Include only events with timestamp <= this value (ISO-8601 instant)")
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

    @Operation(
        operationId = "getEntityStats",
        summary = "Entity event statistics",
        description = "Returns aggregate statistics for the given entity: total deduplicated message count, " +
                      "first/last message timestamps, first/last seen timestamps from the entity registry, " +
                      "and a per-source-topic message count breakdown."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Entity statistics",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = EntityStats.class),
                examples = @ExampleObject(name = "stats", value = """
                    {
                      "entityType": "customer",
                      "entityId": "cust-042",
                      "messageCount": 17,
                      "firstMessage": "2024-01-15T09:00:00Z",
                      "lastMessage": "2024-06-01T10:00:00Z",
                      "firstSeen": "2024-01-15T09:00:01Z",
                      "lastSeen": "2024-06-01T10:00:01Z",
                      "countByTopic": {
                        "customer-events": 12,
                        "customer-orders": 5
                      }
                    }"""))),
        @ApiResponse(responseCode = "400", description = "Invalid entity type name",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/entities/{entityType}/{entityId}/stats",
                produces = MediaType.APPLICATION_JSON_VALUE)
    public EntityStats getEntityStats(
            @Parameter(description = "Entity type name (must match `[a-z][a-z0-9_]*`)", required = true, example = "customer")
            @PathVariable String entityType,
            @Parameter(description = "Entity identifier", required = true, example = "cust-042")
            @PathVariable String entityId
    ) throws SQLException {
        return entityService.getEntityStats(entityType, entityId);
    }

    // =========================================================================
    // Cassette lifecycle – stats, compaction, truncation
    // =========================================================================

    @Operation(
        operationId = "getTopicStats",
        summary = "Topic cassette statistics",
        description = "Returns row count and estimated size statistics for the given topic's slice of the general cassette table."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Topic cassette statistics",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = CassetteStats.class),
                examples = @ExampleObject(name = "stats", value = """
                    {
                      "topic": "orders",
                      "tableName": "lake.cassette",
                      "rowCount": 1048576,
                      "estimatedSizeBytes": 536870912
                    }"""))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/topics/{topic}/stats", produces = MediaType.APPLICATION_JSON_VALUE)
    public CassetteStats getTopicStats(
            @Parameter(description = "Kafka topic name", required = true, example = "orders")
            @PathVariable String topic
    ) throws SQLException {
        return lifecycle.getTopicCassetteStats(topic);
    }

    /** Triggers a DuckDB CHECKPOINT to flush the WAL for the given topic's cassette. */
    @Operation(
        operationId = "compactTopicCassette",
        summary = "Compact topic cassette (CHECKPOINT)",
        description = "Triggers a DuckDB CHECKPOINT to flush the write-ahead log for the given topic's cassette, " +
                      "reducing WAL size and improving read performance. Returns 202 Accepted immediately."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "202", description = "Compaction triggered"),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @PostMapping("/topics/{topic}/compact")
    public ResponseEntity<Void> compactTopic(
            @Parameter(description = "Kafka topic name", required = true, example = "orders")
            @PathVariable String topic
    ) throws SQLException {
        lifecycle.compactTopicCassette(topic);
        return ResponseEntity.accepted().build();
    }

    /** Deletes all rows for {@code topic} from {@code lake.cassette}. */
    @Operation(
        operationId = "truncateTopicCassette",
        summary = "Truncate topic cassette",
        description = "Permanently deletes all recorded messages for the given Kafka topic from `lake.cassette`. " +
                      "Returns the number of deleted rows. This operation cannot be undone."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Number of deleted rows",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(type = "object"),
                examples = @ExampleObject(name = "result", value = "{\"deleted\": 42000}"))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @PostMapping(value = "/topics/{topic}/truncate", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Long>> truncateTopic(
            @Parameter(description = "Kafka topic name", required = true, example = "orders")
            @PathVariable String topic
    ) throws SQLException {
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
    @Operation(
        operationId = "deleteEntity",
        summary = "Delete entity data (right to erasure)",
        description = "Permanently deletes all data for the given entity from `lake.entity_{entityType}` and " +
                      "`lake.known_entities`. Intended to satisfy GDPR right-to-erasure requests. " +
                      "Returns the total number of deleted rows across both tables."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Number of deleted rows",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(type = "object"),
                examples = @ExampleObject(name = "result", value = "{\"deleted\": 17}"))),
        @ApiResponse(responseCode = "400", description = "Invalid entity type name",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @DeleteMapping(value = "/entities/{entityType}/{entityId}",
                   produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Long>> deleteEntity(
            @Parameter(description = "Entity type name (must match `[a-z][a-z0-9_]*`)", required = true, example = "customer")
            @PathVariable String entityType,
            @Parameter(description = "Entity identifier to erase", required = true, example = "cust-042")
            @PathVariable String entityId
    ) throws SQLException {
        long deleted = lifecycle.deleteEntityFromCassette(entityType, entityId);
        return ResponseEntity.ok(Map.of("deleted", deleted));
    }

    // =========================================================================
    // Snapshot management
    // =========================================================================

    @Operation(
        operationId = "listSnapshots",
        summary = "List database snapshots",
        description = "Returns all available DuckDB EXPORT DATABASE snapshots, ordered by creation time."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "List of snapshots",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(type = "array", implementation = SnapshotInfo.class),
                examples = @ExampleObject(name = "list", value = """
                    [
                      {
                        "name": "snapshot-before-migration",
                        "createdAt": "2024-06-01T00:00:00Z",
                        "sizeBytes": 1073741824
                      }
                    ]"""))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/snapshots", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<SnapshotInfo> listSnapshots() throws SQLException {
        return lifecycle.listSnapshots();
    }

    @Operation(
        operationId = "createSnapshot",
        summary = "Create database snapshot",
        description = "Exports the full DuckLake database as a named snapshot using `EXPORT DATABASE`. " +
                      "If `name` is omitted from the request body a timestamp-based name is generated. " +
                      "Returns the created snapshot metadata with HTTP 201."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "201", description = "Snapshot created",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = SnapshotInfo.class),
                examples = @ExampleObject(name = "created", value = """
                    {
                      "name": "snapshot-before-migration",
                      "createdAt": "2024-06-01T00:00:00Z",
                      "sizeBytes": 1073741824
                    }"""))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @PostMapping(value = "/snapshots",
                 consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<SnapshotInfo> createSnapshot(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                description = "Optional snapshot name. If omitted a timestamp-based name is used.",
                content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(type = "object"),
                    examples = @ExampleObject(value = "{\"name\": \"snapshot-before-migration\"}")))
            @RequestBody Map<String, String> body
    ) throws SQLException {
        String name = body.getOrDefault("name",
                "snapshot-" + Instant.now().toEpochMilli());
        SnapshotInfo info = lifecycle.createSnapshot(name);
        return ResponseEntity.status(201).body(info);
    }

    @Operation(
        operationId = "restoreSnapshot",
        summary = "Restore database snapshot",
        description = "Restores the DuckLake database from a previously created named snapshot using `IMPORT DATABASE`. " +
                      "All current data is replaced by the snapshot contents."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Snapshot restored successfully"),
        @ApiResponse(responseCode = "404", description = "Snapshot not found"),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @PostMapping(value = "/snapshots/{name}/restore")
    public ResponseEntity<Void> restoreSnapshot(
            @Parameter(description = "Snapshot name to restore", required = true, example = "snapshot-before-migration")
            @PathVariable String name
    ) throws SQLException {
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
