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

import com.joxette.replay.transform.ReplayMetadataInjector;
import com.joxette.replay.transform.TransformPipeline;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

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
 * <h2>Scheduled replay</h2>
 * <p>All replay endpoints accept two optional scheduling parameters:
 * <ul>
 *   <li>{@code start_at} — ISO-8601 absolute timestamp at which streaming begins.</li>
 *   <li>{@code start_delay_ms} — relative delay in milliseconds before streaming begins.</li>
 * </ul>
 * At most one of the two may be specified per request.
 * <ul>
 *   <li>For {@code text/event-stream} and {@code application/x-ndjson}: the server holds
 *       the connection open and immediately sends a {@code scheduled} event/line, then waits
 *       until the start time before streaming data.</li>
 *   <li>For {@code application/json}: the server returns HTTP 202 Accepted with a
 *       {@link ScheduledReplayResponse} body containing a scheduled replay ID.</li>
 * </ul>
 *
 * <h2>Error handling</h2>
 * <p>Invalid entity-type names (must match {@code [a-z][a-z0-9_]*}) and
 * malformed cursors return HTTP 400.
 */
@Tag(name = "Cassette Replay",
     description = "Replay recorded Kafka messages stored in DuckLake. " +
                   "Replay endpoints support three response formats via the Accept header: " +
                   "application/json (cursor-paginated), text/event-stream (SSE), and application/x-ndjson (streaming). " +
                   "All replay endpoints support optional start_at / start_delay_ms parameters for scheduled delivery.")
@RestController
@RequestMapping("/cassettes")
public class CassetteController {

    private static final int DEFAULT_LIMIT = 100;

    private final TopicReplayService topicService;
    private final EntityReplayService entityService;
    private final SseReplayHandler sseHandler;
    private final CassetteLifecycleService lifecycle;
    private final ReplayToTopicService replayToTopicService;
    private final ScheduledReplayService scheduledReplayService;
    private final ReplayMetadataInjector metadataInjector;

    public CassetteController(
            TopicReplayService topicService,
            EntityReplayService entityService,
            SseReplayHandler sseHandler,
            CassetteLifecycleService lifecycle,
            ReplayToTopicService replayToTopicService,
            ScheduledReplayService scheduledReplayService,
            ReplayMetadataInjector metadataInjector) {
        this.topicService             = topicService;
        this.entityService            = entityService;
        this.sseHandler               = sseHandler;
        this.lifecycle                = lifecycle;
        this.replayToTopicService     = replayToTopicService;
        this.scheduledReplayService   = scheduledReplayService;
        this.metadataInjector         = metadataInjector;
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
                      "Use `Accept: text/event-stream` for SSE streaming or `Accept: application/x-ndjson` for NDJSON streaming. " +
                      "If `start_at` or `start_delay_ms` is provided, returns HTTP 202 with a scheduled replay ID " +
                      "instead of data."
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
        @ApiResponse(responseCode = "202", description = "Replay scheduled. Data will stream at the given start time.",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = ScheduledReplayResponse.class),
                examples = @ExampleObject(name = "scheduled", value = """
                    {
                      "scheduledReplayId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                      "scheduledAt": "2026-04-12T10:00:00Z"
                    }"""))),
        @ApiResponse(responseCode = "400", description = "Invalid topic, filter value, or malformed cursor",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "429", description = "Max concurrent scheduled replays reached",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/topics/{topic}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> getTopicJson(
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
            @RequestParam(required = false) String cursor,
            @Parameter(description = "ISO-8601 absolute timestamp at which streaming begins (mutually exclusive with start_delay_ms)", name = "start_at")
            @RequestParam(name = "start_at", required = false) Instant startAt,
            @Parameter(description = "Relative delay in milliseconds before streaming begins (mutually exclusive with start_at)", name = "start_delay_ms")
            @RequestParam(name = "start_delay_ms", required = false) Long startDelayMs
    ) throws SQLException {
        Instant scheduledAt = resolveScheduledAt(startAt, startDelayMs);
        if (scheduledAt != null) {
            String id = scheduledReplayService.registerTopicReplay(
                    topic, scheduledAt, from, to, partition, offsetFrom, offsetTo);
            return ResponseEntity.accepted().body(new ScheduledReplayResponse(id, scheduledAt));
        }
        String replayId = newReplayId();
        TransformPipeline pipeline = metadataPipeline();
        return ResponseEntity.ok(
                topicService.query(topic, from, to, partition, offsetFrom, offsetTo,
                                   limit, cursor, pipeline, replayId));
    }

    @Operation(
        operationId = "getTopicRecordsSse",
        summary = "Replay topic records (SSE)",
        description = "Streams all matching messages as Server-Sent Events (Accept: text/event-stream). " +
                      "Each `data:` field contains a single JSON-serialised `CassetteRecord`. " +
                      "The stream ends when all matching records have been sent. " +
                      "Supports the same time, partition, and offset filters as the JSON variant. " +
                      "If `start_at` or `start_delay_ms` is provided, the server first sends a " +
                      "`scheduled` event with `{\"id\":\"…\",\"scheduledAt\":\"…\"}`, then waits " +
                      "until the start time before streaming data. If cancelled before the start time, " +
                      "a `cancelled` event is sent."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Server-Sent Event stream of cassette records",
            content = @Content(mediaType = MediaType.TEXT_EVENT_STREAM_VALUE,
                schema = @Schema(type = "string",
                    description = "Each SSE event: `data: {CassetteRecord JSON}\\n\\n`. " +
                                  "When scheduled: first event is `event: scheduled\\ndata: {\"id\":\"…\",\"scheduledAt\":\"…\"}\\n\\n`"),
                examples = @ExampleObject(name = "event", value =
                    "data: {\"topic\":\"orders\",\"partition\":0,\"offset\":1024," +
                          "\"timestamp\":\"2024-06-01T12:00:00Z\"," +
                          "\"recordedAt\":\"2024-06-01T12:00:00.123Z\"," +
                          "\"key\":\"b3JkZXItNDI\",\"value\":\"eyJvcmRlcklkIjoiNDIifQ\"}\n\n"))),
        @ApiResponse(responseCode = "400", description = "Invalid topic or filter value",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "429", description = "Max concurrent scheduled replays reached",
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
            @RequestParam(name = "offset_to",   required = false) Long offsetTo,
            @Parameter(description = "ISO-8601 absolute timestamp at which streaming begins (mutually exclusive with start_delay_ms)", name = "start_at")
            @RequestParam(name = "start_at", required = false) Instant startAt,
            @Parameter(description = "Relative delay in milliseconds before streaming begins (mutually exclusive with start_at)", name = "start_delay_ms")
            @RequestParam(name = "start_delay_ms", required = false) Long startDelayMs
    ) {
        Instant scheduledAt = resolveScheduledAt(startAt, startDelayMs);
        if (scheduledAt != null) {
            String id = scheduledReplayService.registerTopicReplay(
                    topic, scheduledAt, from, to, partition, offsetFrom, offsetTo);
            return sseHandler.<CassetteRecord>streamSseScheduled(id, scheduledAt, scheduledReplayService,
                    sink -> topicService.streamAll(topic, from, to, partition, offsetFrom, offsetTo, sink));
        }
        String replayId = newReplayId();
        TransformPipeline pipeline = metadataPipeline();
        return sseHandler.<CassetteRecord>streamSse(
                sink -> topicService.streamAll(topic, from, to, partition, offsetFrom, offsetTo,
                                               sink, pipeline, replayId));
    }

    @Operation(
        operationId = "getTopicRecordsNdjson",
        summary = "Replay topic records (NDJSON)",
        description = "Streams all matching messages as newline-delimited JSON (Accept: application/x-ndjson). " +
                      "Each line is a complete JSON-serialised `CassetteRecord`, flushed incrementally. " +
                      "Supports the same time, partition, and offset filters as the JSON variant. " +
                      "If `start_at` or `start_delay_ms` is provided, the first line written is " +
                      "`{\"event\":\"scheduled\",\"id\":\"…\",\"scheduledAt\":\"…\"}` and the stream " +
                      "waits until the start time before data lines begin."
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
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "429", description = "Max concurrent scheduled replays reached",
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
            @RequestParam(name = "offset_to",   required = false) Long offsetTo,
            @Parameter(description = "ISO-8601 absolute timestamp at which streaming begins (mutually exclusive with start_delay_ms)", name = "start_at")
            @RequestParam(name = "start_at", required = false) Instant startAt,
            @Parameter(description = "Relative delay in milliseconds before streaming begins (mutually exclusive with start_at)", name = "start_delay_ms")
            @RequestParam(name = "start_delay_ms", required = false) Long startDelayMs
    ) {
        Instant scheduledAt = resolveScheduledAt(startAt, startDelayMs);
        StreamingResponseBody body;
        if (scheduledAt != null) {
            String id = scheduledReplayService.registerTopicReplay(
                    topic, scheduledAt, from, to, partition, offsetFrom, offsetTo);
            body = sseHandler.<CassetteRecord>streamNdjsonScheduled(id, scheduledAt, scheduledReplayService,
                    sink -> topicService.streamAll(topic, from, to, partition, offsetFrom, offsetTo, sink));
        } else {
            String replayId = newReplayId();
            TransformPipeline pipeline = metadataPipeline();
            body = sseHandler.<CassetteRecord>streamNdjson(
                    sink -> topicService.streamAll(topic, from, to, partition, offsetFrom, offsetTo,
                                                   sink, pipeline, replayId));
        }
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
                      "Use `Accept: text/event-stream` for SSE streaming or `Accept: application/x-ndjson` for NDJSON streaming. " +
                      "If `start_at` or `start_delay_ms` is provided, returns HTTP 202 with a scheduled replay ID " +
                      "instead of data."
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
        @ApiResponse(responseCode = "202", description = "Replay scheduled. Data will stream at the given start time.",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = ScheduledReplayResponse.class),
                examples = @ExampleObject(name = "scheduled", value = """
                    {
                      "scheduledReplayId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                      "scheduledAt": "2026-04-12T10:00:00Z"
                    }"""))),
        @ApiResponse(responseCode = "400", description = "Invalid entity type or malformed cursor",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "429", description = "Max concurrent scheduled replays reached",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/entities/{entityType}/{entityId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> getEntityJson(
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
            @RequestParam(required = false) String cursor,
            @Parameter(description = "ISO-8601 absolute timestamp at which streaming begins (mutually exclusive with start_delay_ms)", name = "start_at")
            @RequestParam(name = "start_at", required = false) Instant startAt,
            @Parameter(description = "Relative delay in milliseconds before streaming begins (mutually exclusive with start_at)", name = "start_delay_ms")
            @RequestParam(name = "start_delay_ms", required = false) Long startDelayMs
    ) throws SQLException {
        Instant scheduledAt = resolveScheduledAt(startAt, startDelayMs);
        if (scheduledAt != null) {
            String id = scheduledReplayService.registerEntityReplay(
                    entityType, entityId, scheduledAt, from, to);
            return ResponseEntity.accepted().body(new ScheduledReplayResponse(id, scheduledAt));
        }
        String replayId = newReplayId();
        TransformPipeline pipeline = metadataPipeline();
        return ResponseEntity.ok(
                entityService.queryEntityEvents(entityType, entityId, from, to,
                                                limit, cursor, pipeline, replayId));
    }

    @Operation(
        operationId = "getEntityRecordsSse",
        summary = "Replay entity events (SSE)",
        description = "Streams all deduplicated events for the given entity as Server-Sent Events " +
                      "(Accept: text/event-stream). Each `data:` field contains a single JSON-serialised `EntityRecord`. " +
                      "Supports the same timestamp filters as the JSON variant. " +
                      "If `start_at` or `start_delay_ms` is provided, the server first sends a " +
                      "`scheduled` event with `{\"id\":\"…\",\"scheduledAt\":\"…\"}` and waits before streaming."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Server-Sent Event stream of entity records",
            content = @Content(mediaType = MediaType.TEXT_EVENT_STREAM_VALUE,
                schema = @Schema(type = "string",
                    description = "Each SSE event: `data: {EntityRecord JSON}\\n\\n`. " +
                                  "When scheduled: first event is `event: scheduled\\ndata: {\"id\":\"…\",\"scheduledAt\":\"…\"}\\n\\n`"),
                examples = @ExampleObject(name = "event", value =
                    "data: {\"entityId\":\"cust-042\",\"entityBucket\":5,\"topic\":\"customer-events\"," +
                          "\"partition\":1,\"offset\":8800," +
                          "\"timestamp\":\"2024-06-01T10:00:00Z\"," +
                          "\"recordedAt\":\"2024-06-01T10:00:00.456Z\"," +
                          "\"key\":\"Y3VzdC0wNDI\",\"value\":\"eyJldmVudCI6InVwZGF0ZWQifQ\"}\n\n"))),
        @ApiResponse(responseCode = "400", description = "Invalid entity type",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "429", description = "Max concurrent scheduled replays reached",
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
            @RequestParam(required = false) Instant to,
            @Parameter(description = "ISO-8601 absolute timestamp at which streaming begins (mutually exclusive with start_delay_ms)", name = "start_at")
            @RequestParam(name = "start_at", required = false) Instant startAt,
            @Parameter(description = "Relative delay in milliseconds before streaming begins (mutually exclusive with start_at)", name = "start_delay_ms")
            @RequestParam(name = "start_delay_ms", required = false) Long startDelayMs
    ) {
        Instant scheduledAt = resolveScheduledAt(startAt, startDelayMs);
        if (scheduledAt != null) {
            String id = scheduledReplayService.registerEntityReplay(
                    entityType, entityId, scheduledAt, from, to);
            return sseHandler.<EntityRecord>streamSseScheduled(id, scheduledAt, scheduledReplayService,
                    sink -> entityService.streamEntityEvents(entityType, entityId, from, to, sink));
        }
        String replayId = newReplayId();
        TransformPipeline pipeline = metadataPipeline();
        return sseHandler.<EntityRecord>streamSse(
                sink -> entityService.streamEntityEvents(entityType, entityId, from, to,
                                                        sink, pipeline, replayId));
    }

    @Operation(
        operationId = "getEntityRecordsNdjson",
        summary = "Replay entity events (NDJSON)",
        description = "Streams all deduplicated events for the given entity as newline-delimited JSON " +
                      "(Accept: application/x-ndjson). Each line is a complete JSON-serialised `EntityRecord`, " +
                      "flushed incrementally. Supports the same timestamp filters as the JSON variant. " +
                      "If `start_at` or `start_delay_ms` is provided, the first line written is " +
                      "`{\"event\":\"scheduled\",\"id\":\"…\",\"scheduledAt\":\"…\"}` and the stream " +
                      "waits until the start time before data lines begin."
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
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "429", description = "Max concurrent scheduled replays reached",
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
            @RequestParam(required = false) Instant to,
            @Parameter(description = "ISO-8601 absolute timestamp at which streaming begins (mutually exclusive with start_delay_ms)", name = "start_at")
            @RequestParam(name = "start_at", required = false) Instant startAt,
            @Parameter(description = "Relative delay in milliseconds before streaming begins (mutually exclusive with start_at)", name = "start_delay_ms")
            @RequestParam(name = "start_delay_ms", required = false) Long startDelayMs
    ) {
        Instant scheduledAt = resolveScheduledAt(startAt, startDelayMs);
        StreamingResponseBody body;
        if (scheduledAt != null) {
            String id = scheduledReplayService.registerEntityReplay(
                    entityType, entityId, scheduledAt, from, to);
            body = sseHandler.<EntityRecord>streamNdjsonScheduled(id, scheduledAt, scheduledReplayService,
                    sink -> entityService.streamEntityEvents(entityType, entityId, from, to, sink));
        } else {
            String replayId = newReplayId();
            TransformPipeline pipeline = metadataPipeline();
            body = sseHandler.<EntityRecord>streamNdjson(
                    sink -> entityService.streamEntityEvents(entityType, entityId, from, to,
                                                            sink, pipeline, replayId));
        }
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
    // Scheduled replay management
    // =========================================================================

    @Operation(
        operationId = "listScheduledReplays",
        summary = "List pending scheduled replays",
        description = "Returns all replay requests that are currently pending (waiting for their start time) " +
                      "or actively streaming. Scheduled replays are held in memory only; they are lost on service restart. " +
                      "Results are ordered by scheduled start time ascending."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "List of pending and streaming scheduled replays",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(type = "array", implementation = ScheduledReplay.class),
                examples = @ExampleObject(name = "list", value = """
                    [
                      {
                        "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                        "kind": "topic",
                        "topic": "orders",
                        "scheduledAt": "2026-04-12T10:00:00Z",
                        "createdAt": "2026-04-12T09:55:00Z",
                        "status": "pending"
                      }
                    ]""")))
    })
    @GetMapping(value = "/scheduled", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<ScheduledReplay> listScheduledReplays() {
        return scheduledReplayService.list();
    }

    @Operation(
        operationId = "cancelScheduledReplay",
        summary = "Cancel a pending scheduled replay",
        description = "Cancels a scheduled replay before its start time. " +
                      "If the replay is in SSE or NDJSON mode the server sends a `cancelled` event/line " +
                      "and closes the stream. Cannot cancel a replay that is already streaming."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "204", description = "Replay cancelled successfully"),
        @ApiResponse(responseCode = "404", description = "Scheduled replay not found",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "400", description = "Replay cannot be cancelled in its current state",
            content = @Content(schema = @Schema(type = "string")))
    })
    @DeleteMapping("/scheduled/{id}")
    public ResponseEntity<Void> cancelScheduledReplay(
            @Parameter(description = "Scheduled replay ID returned when the replay was registered",
                       required = true, example = "a1b2c3d4-e5f6-7890-abcd-ef1234567890")
            @PathVariable String id
    ) {
        scheduledReplayService.cancel(id);
        return ResponseEntity.noContent().build();
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

    @Operation(
        operationId = "getEntityStorageStats",
        summary = "Entity cassette storage statistics",
        description = "Returns per-bucket row counts and proportional size estimates for the given entity type's " +
                      "cassette table (`lake.main.entity_{entityType}`). " +
                      "Useful for detecting bucket skew and estimating storage usage."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Per-bucket storage statistics",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = EntityStorageStats.class),
                examples = @ExampleObject(name = "stats", value = """
                    {
                      "entityType": "order",
                      "tableName": "lake.main.entity_order",
                      "totalRows": 500000,
                      "totalEstimatedSizeBytes": 134217728,
                      "buckets": [
                        {"bucket": 0, "rowCount": 2012, "estimatedSizeBytes": 539648},
                        {"bucket": 1, "rowCount": 1987, "estimatedSizeBytes": 532941}
                      ]
                    }"""))),
        @ApiResponse(responseCode = "400", description = "Invalid entity type name",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/entities/{entityType}/storage", produces = MediaType.APPLICATION_JSON_VALUE)
    public EntityStorageStats getEntityStorageStats(
            @Parameter(description = "Entity type name (must match `[a-z][a-z0-9_]*`)", required = true, example = "order")
            @PathVariable String entityType
    ) throws SQLException {
        return lifecycle.getEntityTypeStorageStats(entityType);
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

    /** Deletes all rows from {@code lake.main.entity_{entityType}}. */
    @Operation(
        operationId = "truncateEntityCassette",
        summary = "Truncate entity cassette",
        description = "Permanently deletes all recorded events for the given entity type from `lake.main.entity_{entityType}`. " +
                      "Returns the number of deleted rows. This operation cannot be undone."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Number of deleted rows",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(type = "object"),
                examples = @ExampleObject(name = "result", value = "{\"deleted\": 17000}"))),
        @ApiResponse(responseCode = "400", description = "Invalid entity type name",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @PostMapping(value = "/entities/{entityType}/truncate", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Long>> truncateEntity(
            @Parameter(description = "Entity type name (must match `[a-z][a-z0-9_]*`)", required = true, example = "order")
            @PathVariable String entityType
    ) throws SQLException {
        long deleted = lifecycle.truncateEntityCassette(entityType);
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
        operationId = "exportSnapshotToObjectStore",
        summary = "Export snapshot to object store",
        description = "Creates a named DuckDB `EXPORT DATABASE` snapshot, uploads all exported files to the " +
                      "configured S3-compatible object store, then removes the local snapshot directory. " +
                      "Requires `joxette.object-store.bucket` to be set. " +
                      "Returns the snapshot metadata including the S3 base URI."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "201", description = "Snapshot exported to object store",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = ObjectStoreSnapshotInfo.class),
                examples = @ExampleObject(name = "exported", value = """
                    {
                      "name": "snap-before-deploy",
                      "createdAt": "2024-06-01T00:00:00Z",
                      "sizeBytes": 1073741824,
                      "objectStoreUri": "s3://my-bucket/snapshots/snap-before-deploy/"
                    }"""))),
        @ApiResponse(responseCode = "500", description = "Object store not configured or upload failed",
            content = @Content(schema = @Schema(type = "string")))
    })
    @PostMapping(value = "/snapshots/export-to-object-store",
                 consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<ObjectStoreSnapshotInfo> exportSnapshotToObjectStore(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                description = "Optional snapshot name. If omitted a timestamp-based name is used.",
                content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(type = "object"),
                    examples = @ExampleObject(value = "{\"name\": \"snap-before-deploy\"}")))
            @RequestBody Map<String, String> body
    ) throws SQLException {
        String name = body.getOrDefault("name", "snapshot-" + Instant.now().toEpochMilli());
        ObjectStoreSnapshotInfo info = lifecycle.exportSnapshotToObjectStore(name);
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

    @Operation(
        operationId = "rebuildKnownEntities",
        summary = "Rebuild known_entities registry from cassette data",
        description = "Scans all entity cassette tables (`lake.main.entity_*`) on object storage and " +
                      "rebuilds the `known_entities` registry from scratch. " +
                      "Use this to recover after the local `.ducklake` catalog file was lost. " +
                      "Returns the number of entity rows upserted."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Rebuild complete",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(type = "object"),
                examples = @ExampleObject(value = "{\"rebuilt\": 4217}"))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @PostMapping(value = "/entities/rebuild-known-entities",
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Long>> rebuildKnownEntities() throws SQLException {
        long rebuilt = lifecycle.rebuildKnownEntities();
        return ResponseEntity.ok(Map.of("rebuilt", rebuilt));
    }

    // =========================================================================
    // Replay-to-topic
    // =========================================================================

    @Operation(
        operationId = "replayTopicToTopicJson",
        summary     = "Replay general cassette back to a Kafka topic (JSON)",
        description = "Reads every matching record from the general cassette for `sourceTopic` in " +
                      "`kafka_timestamp ASC` order and produces each one to `targetTopic`, " +
                      "preserving the original Kafka timestamp. " +
                      "Blocks until all records have been sent, then returns the final " +
                      "`ReplayProgress` summary. " +
                      "For a live progress stream use `Accept: text/event-stream`."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Replay complete – final progress summary",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = ReplayProgress.class),
                examples = @ExampleObject(name = "done", value = """
                    {
                      "status": "completed",
                      "targetTopic": "orders-replay",
                      "sentCount": 42000,
                      "errorCount": 0,
                      "currentTimestamp": "2024-06-01T12:00:00Z"
                    }"""))),
        @ApiResponse(responseCode = "400", description = "Missing or invalid targetTopic",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "500", description = "Database or Kafka error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @PostMapping(value = "/topics/{topic}/replay-to-topic",
                 consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ReplayProgress replayTopicToTopicJson(
            @Parameter(description = "Source Kafka topic name (cassette to replay from)",
                       required = true, example = "orders")
            @PathVariable String topic,
            @Parameter(description = "Replay speed multiplier: 1.0 = real-time, 2.0 = double speed, 0.5 = half speed. " +
                                     "Inter-message delay = (next_ts − prev_ts) / speed",
                       example = "1.0")
            @RequestParam(name = "speed", defaultValue = "1.0") double speed,
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                description = "Target topic and optional filters",
                content = @Content(schema = @Schema(implementation = ReplayToTopicRequest.class)))
            @RequestBody ReplayToTopicRequest req
    ) throws SQLException {
        if (speed <= 0) {
            throw new IllegalArgumentException("speed must be greater than 0");
        }
        ReplayProgress[] result = {null};
        replayToTopicService.replayTopicToKafka(topic, req, speed, p -> result[0] = p);
        return result[0];
    }

    @Operation(
        operationId = "replayTopicToTopicSse",
        summary     = "Replay general cassette back to a Kafka topic (SSE)",
        description = "Streams `ReplayProgress` events as Server-Sent Events while replaying " +
                      "the general cassette for `sourceTopic` to `targetTopic`. " +
                      "A progress event is emitted every 100 records. " +
                      "The final event has `status: completed` (or `status: failed` on error). " +
                      "Use `Accept: application/json` for a blocking single-response variant."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "SSE stream of ReplayProgress events",
            content = @Content(mediaType = MediaType.TEXT_EVENT_STREAM_VALUE,
                schema = @Schema(type = "string"),
                examples = @ExampleObject(name = "event", value =
                    "data: {\"status\":\"in_progress\",\"targetTopic\":\"orders-replay\"," +
                          "\"sentCount\":100,\"errorCount\":0," +
                          "\"currentTimestamp\":\"2024-01-01T00:01:40Z\"}\n\n"))),
        @ApiResponse(responseCode = "400", description = "Missing or invalid targetTopic",
            content = @Content(schema = @Schema(type = "string")))
    })
    @PostMapping(value = "/topics/{topic}/replay-to-topic",
                 consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter replayTopicToTopicSse(
            @Parameter(description = "Source Kafka topic name (cassette to replay from)",
                       required = true, example = "orders")
            @PathVariable String topic,
            @Parameter(description = "Replay speed multiplier: 1.0 = real-time, 2.0 = double speed, 0.5 = half speed. " +
                                     "Inter-message delay = (next_ts − prev_ts) / speed",
                       example = "1.0")
            @RequestParam(name = "speed", defaultValue = "1.0") double speed,
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                description = "Target topic and optional filters",
                content = @Content(schema = @Schema(implementation = ReplayToTopicRequest.class)))
            @RequestBody ReplayToTopicRequest req
    ) {
        if (speed <= 0) {
            throw new IllegalArgumentException("speed must be greater than 0");
        }
        return sseHandler.<ReplayProgress>streamSse(
                sink -> replayToTopicService.replayTopicToKafka(topic, req, speed, sink));
    }

    @Operation(
        operationId = "replayEntityToTopicJson",
        summary     = "Replay entity cassette back to a Kafka topic (JSON)",
        description = "Reads every matching event for `entityId` from the entity cassette " +
                      "`lake.main.entity_{entityType}` and produces each one to `targetTopic`. " +
                      "Events span multiple source topics and are merge-sorted by " +
                      "`kafka_timestamp ASC` before producing. " +
                      "Blocks until all records have been sent, then returns the final " +
                      "`ReplayProgress` summary. " +
                      "For a live progress stream use `Accept: text/event-stream`."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Replay complete – final progress summary",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = ReplayProgress.class),
                examples = @ExampleObject(name = "done", value = """
                    {
                      "status": "completed",
                      "targetTopic": "customer-replay",
                      "sentCount": 17,
                      "errorCount": 0,
                      "currentTimestamp": "2024-06-01T10:00:00Z"
                    }"""))),
        @ApiResponse(responseCode = "400", description = "Invalid entity type or missing targetTopic",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "500", description = "Database or Kafka error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @PostMapping(value = "/entities/{entityType}/{entityId}/replay-to-topic",
                 consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ReplayProgress replayEntityToTopicJson(
            @Parameter(description = "Entity type (must match `[a-z][a-z0-9_]*`)",
                       required = true, example = "customer")
            @PathVariable String entityType,
            @Parameter(description = "Entity identifier", required = true, example = "cust-042")
            @PathVariable String entityId,
            @Parameter(description = "Replay speed multiplier: 1.0 = real-time, 2.0 = double speed, 0.5 = half speed. " +
                                     "Inter-message delay = (next_ts − prev_ts) / speed",
                       example = "1.0")
            @RequestParam(name = "speed", defaultValue = "1.0") double speed,
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                description = "Target topic and optional timestamp filters",
                content = @Content(schema = @Schema(implementation = ReplayToTopicRequest.class)))
            @RequestBody ReplayToTopicRequest req
    ) throws SQLException {
        if (speed <= 0) {
            throw new IllegalArgumentException("speed must be greater than 0");
        }
        ReplayProgress[] result = {null};
        replayToTopicService.replayEntityToKafka(entityType, entityId, req, speed, p -> result[0] = p);
        return result[0];
    }

    @Operation(
        operationId = "replayEntityToTopicSse",
        summary     = "Replay entity cassette back to a Kafka topic (SSE)",
        description = "Streams `ReplayProgress` events as Server-Sent Events while replaying " +
                      "the entity cassette for `entityId` to `targetTopic`. " +
                      "Events from all source topics are merge-sorted by `kafka_timestamp` before " +
                      "being produced. " +
                      "A progress event is emitted every 100 records. " +
                      "The final event has `status: completed` (or `status: failed` on error). " +
                      "Use `Accept: application/json` for a blocking single-response variant."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "SSE stream of ReplayProgress events",
            content = @Content(mediaType = MediaType.TEXT_EVENT_STREAM_VALUE,
                schema = @Schema(type = "string"),
                examples = @ExampleObject(name = "event", value =
                    "data: {\"status\":\"completed\",\"targetTopic\":\"customer-replay\"," +
                          "\"sentCount\":17,\"errorCount\":0," +
                          "\"currentTimestamp\":\"2024-06-01T10:00:00Z\"}\n\n"))),
        @ApiResponse(responseCode = "400", description = "Invalid entity type or missing targetTopic",
            content = @Content(schema = @Schema(type = "string")))
    })
    @PostMapping(value = "/entities/{entityType}/{entityId}/replay-to-topic",
                 consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter replayEntityToTopicSse(
            @Parameter(description = "Entity type (must match `[a-z][a-z0-9_]*`)",
                       required = true, example = "customer")
            @PathVariable String entityType,
            @Parameter(description = "Entity identifier", required = true, example = "cust-042")
            @PathVariable String entityId,
            @Parameter(description = "Replay speed multiplier: 1.0 = real-time, 2.0 = double speed, 0.5 = half speed. " +
                                     "Inter-message delay = (next_ts − prev_ts) / speed",
                       example = "1.0")
            @RequestParam(name = "speed", defaultValue = "1.0") double speed,
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                description = "Target topic and optional timestamp filters",
                content = @Content(schema = @Schema(implementation = ReplayToTopicRequest.class)))
            @RequestBody ReplayToTopicRequest req
    ) {
        if (speed <= 0) {
            throw new IllegalArgumentException("speed must be greater than 0");
        }
        return sseHandler.<ReplayProgress>streamSse(
                sink -> replayToTopicService.replayEntityToKafka(entityType, entityId, req, speed, sink));
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /**
     * Creates a metadata-only pipeline for a browse/stream request: no user steps,
     * but the six replay-provenance headers are always injected.
     */
    private TransformPipeline metadataPipeline() {
        return new TransformPipeline(List.of(), metadataInjector);
    }

    /** Mints a fresh UUID string to identify this replay session. */
    private static String newReplayId() {
        return UUID.randomUUID().toString();
    }

    /**
     * Resolves {@code start_at} / {@code start_delay_ms} to an absolute {@link Instant}.
     *
     * @return the resolved start time, or {@code null} if neither parameter was supplied
     * @throws IllegalArgumentException if both params are supplied, or {@code start_delay_ms < 0}
     */
    private Instant resolveScheduledAt(Instant startAt, Long startDelayMs) {
        if (startAt != null && startDelayMs != null) {
            throw new IllegalArgumentException(
                    "Specify at most one of start_at and start_delay_ms");
        }
        if (startAt != null) return startAt;
        if (startDelayMs != null) {
            if (startDelayMs < 0) {
                throw new IllegalArgumentException("start_delay_ms must be non-negative");
            }
            return Instant.now().plusMillis(startDelayMs);
        }
        return null;
    }

    // =========================================================================
    // Error handling
    // =========================================================================

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> handleBadRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(ex.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<String> handleConflict(IllegalStateException ex) {
        return ResponseEntity.status(429).body(ex.getMessage());
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
