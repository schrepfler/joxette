package com.joxette.streams;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.joxette.api.error.ResourceNotFoundException;
import com.joxette.replay.DedupPolicy;
import com.joxette.replay.ReplayOutputMode;
import com.joxette.replay.SolOutput;
import com.joxette.replay.StateFoldStrategy;
import com.joxette.replay.transform.TransformStep;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;

/**
 * CRUD endpoints for named derived stream definitions.
 *
 * <p>A stream definition is a stored, lazy query over an entity's event history.
 * Once created it can be consumed as a bounded pull query
 * ({@code GET /streams/{id}}) or a live push subscription
 * ({@code GET /streams/{id}/events}) — not yet wired in this release.
 *
 * <p>A stream with a null {@code entityId} is an entity-type stream; the entity
 * ID is supplied at consumption time as a query parameter.
 */
@Tag(name = "Streams",
     description = "CRUD for named derived stream definitions. " +
                   "Each definition stores source filters, optional SOL sequence processing, " +
                   "a transform pipeline, and an output mode under a reusable slug.")
@RestController
@RequestMapping("/streams")
public class StreamController {

    private final StreamDefinitionRepository repository;

    public StreamController(StreamDefinitionRepository repository) {
        this.repository = repository;
    }

    // =========================================================================
    // CRUD
    // =========================================================================

    @Operation(
        operationId = "listStreams",
        summary     = "List all stream definitions",
        description = "Returns all saved stream definitions ordered by id. " +
                      "Pass `?entity_type=<type>` to scope to a specific entity type.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "List of stream definitions",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(type = "array", implementation = StreamDefinition.class)))
    })
    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public List<StreamDefinition> listStreams(
            @Parameter(description = "Filter by entity type", example = "order")
            @RequestParam(name = "entity_type", required = false) String entityType) {
        return entityType != null ? repository.listByEntityType(entityType) : repository.listAll();
    }

    @Operation(
        operationId = "createStream",
        summary     = "Create a stream definition",
        description = "Saves a new named derived stream definition. Returns HTTP 409 if the id is already taken.")
    @ApiResponses({
        @ApiResponse(responseCode = "201", description = "Stream definition created",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = StreamDefinition.class))),
        @ApiResponse(responseCode = "400", description = "Invalid request body",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "409", description = "Stream id already exists",
            content = @Content(schema = @Schema(type = "string")))
    })
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<StreamDefinition> createStream(
            @Valid @RequestBody CreateStreamRequest req) {
        StreamDefinition created = repository.create(
                req.id(), req.name(), req.entityType(), req.entityId(),
                req.source(), req.sol(), req.solOutput(),
                req.transform() != null ? req.transform() : List.of(),
                req.output(), req.stateFold());
        return ResponseEntity.status(201).body(created);
    }

    @Operation(
        operationId = "getStream",
        summary     = "Get a stream definition by id",
        description = "Returns the full stream definition.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Stream definition found",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = StreamDefinition.class))),
        @ApiResponse(responseCode = "404", description = "Stream not found",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public StreamDefinition getStream(
            @Parameter(description = "Stream id", required = true, example = "order-lifecycle")
            @PathVariable String id) {
        return repository.findById(id)
                .orElseThrow(() -> ResourceNotFoundException.stream(id));
    }

    @Operation(
        operationId = "updateStream",
        summary     = "Replace a stream definition",
        description = "Replaces the full definition of an existing stream. " +
                      "Returns HTTP 404 if the stream does not exist.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Stream definition updated",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = StreamDefinition.class))),
        @ApiResponse(responseCode = "404", description = "Stream not found",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "400", description = "Invalid request body",
            content = @Content(schema = @Schema(type = "string")))
    })
    @PutMapping(value = "/{id}",
                consumes = MediaType.APPLICATION_JSON_VALUE,
                produces = MediaType.APPLICATION_JSON_VALUE)
    public StreamDefinition updateStream(
            @Parameter(description = "Stream id", required = true, example = "order-lifecycle")
            @PathVariable String id,
            @Valid @RequestBody UpdateStreamRequest req) {
        return repository.update(
                id, req.name(), req.entityType(), req.entityId(),
                req.source(), req.sol(), req.solOutput(),
                req.transform() != null ? req.transform() : List.of(),
                req.output(), req.stateFold());
    }

    @Operation(
        operationId = "deleteStream",
        summary     = "Delete a stream definition",
        description = "Permanently deletes the named stream definition.")
    @ApiResponses({
        @ApiResponse(responseCode = "204", description = "Stream definition deleted"),
        @ApiResponse(responseCode = "404", description = "Stream not found",
            content = @Content(schema = @Schema(type = "string")))
    })
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteStream(
            @Parameter(description = "Stream id", required = true, example = "order-lifecycle")
            @PathVariable String id) {
        if (!repository.delete(id)) {
            throw ResourceNotFoundException.stream(id);
        }
        return ResponseEntity.noContent().build();
    }

    // =========================================================================
    // Request DTOs
    // =========================================================================

    /** Request body for {@code POST /streams}. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record CreateStreamRequest(

            @Schema(description = "Unique URL-safe slug for this stream, e.g. 'order-lifecycle'",
                    example = "order-lifecycle")
            @NotBlank
            @Pattern(regexp = "^[a-z][a-z0-9_-]*$",
                     message = "id must start with a lowercase letter and contain only lowercase letters, digits, hyphens, or underscores")
            String id,

            @Schema(description = "Human-readable display name", example = "Order Lifecycle")
            @NotBlank
            String name,

            @Schema(description = "Entity type this stream operates on", example = "order")
            @NotBlank
            String entityType,

            @Schema(description = "Entity ID to bind at definition time. Null = entity-type stream.",
                    example = "order-789")
            String entityId,

            @Schema(description = "Source filter options")
            StreamDefinition.SourceOptions source,

            @Schema(description = "SOL sequence-level expression. Null = no sequence processing.",
                    example = "MATCH order_created THEN payment WITHIN 24h THEN shipped")
            String sol,

            @Schema(description = "Controls SOL output when sol is non-null",
                    defaultValue = "events")
            SolOutput solOutput,

            @Schema(description = "Per-event transform pipeline steps applied after SOL")
            List<TransformStep> transform,

            @Schema(description = "Output mode: events | state", defaultValue = "events")
            ReplayOutputMode output,

            @Schema(description = "State fold strategy, only relevant when output=state",
                    name = "state_fold")
            StateFoldStrategy stateFold
    ) {}

    /** Request body for {@code PUT /streams/{id}}. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record UpdateStreamRequest(

            @Schema(description = "Updated display name", example = "Order Lifecycle v2")
            @NotBlank
            String name,

            @Schema(description = "Entity type this stream operates on", example = "order")
            @NotBlank
            String entityType,

            @Schema(description = "Entity ID to bind. Null = entity-type stream.")
            String entityId,

            @Schema(description = "Source filter options")
            StreamDefinition.SourceOptions source,

            @Schema(description = "SOL sequence-level expression")
            String sol,

            @Schema(description = "Controls SOL output when sol is non-null")
            SolOutput solOutput,

            @Schema(description = "Per-event transform pipeline steps")
            List<TransformStep> transform,

            @Schema(description = "Output mode: events | state")
            ReplayOutputMode output,

            @Schema(description = "State fold strategy, only relevant when output=state",
                    name = "state_fold")
            StateFoldStrategy stateFold
    ) {}
}
