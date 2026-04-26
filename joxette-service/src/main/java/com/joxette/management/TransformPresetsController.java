package com.joxette.management;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.joxette.api.error.ResourceNotFoundException;
import com.joxette.replay.transform.TransformPreset;
import com.joxette.replay.transform.TransformPresetRepository;
import com.joxette.replay.transform.TransformStep;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * CRUD endpoints for named transform pipeline presets.
 *
 * <p>Presets store a reusable {@link TransformStep} pipeline under a memorable name.
 * Once saved, a preset can be referenced in replay requests via
 * {@code ?transform_preset=<name>} instead of repeating the full JSON array.
 *
 * <p>Presets are stored in the plain-DuckDB {@code transform_presets} table and
 * are not replicated — they live in the service's local catalog.
 */
@Tag(name = "Transform Presets",
     description = "CRUD for named transform pipelines. " +
                   "Presets can be referenced by name in replay requests using the " +
                   "`transform_preset` query parameter, avoiding repetition of large pipeline JSON.")
@RestController
@RequestMapping("/transforms")
public class TransformPresetsController {

    private final TransformPresetRepository repository;

    public TransformPresetsController(TransformPresetRepository repository) {
        this.repository = repository;
    }

    // =========================================================================
    // CRUD
    // =========================================================================

    @Operation(
        operationId = "listTransformPresets",
        summary     = "List all transform presets",
        description = "Returns all saved transform pipeline presets ordered by name.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "List of presets",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(type = "array", implementation = TransformPreset.class),
                examples = @ExampleObject(name = "list", value = """
                    [
                      {
                        "name": "staging-sanitize",
                        "description": "Redact PII and redirect to staging",
                        "steps": [
                          {"type": "redact", "field": "$.value.email"},
                          {"type": "redirect_topic", "topic": "orders-staging"}
                        ],
                        "createdAt": "2026-04-13T00:00:00Z",
                        "updatedAt": "2026-04-13T00:00:00Z"
                      }
                    ]""")))
    })
    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public List<TransformPreset> listPresets() {
        return repository.listAll();
    }

    @Operation(
        operationId = "createTransformPreset",
        summary     = "Create a transform preset",
        description = "Saves a new named transform pipeline. Returns HTTP 409 if the name is already taken.")
    @ApiResponses({
        @ApiResponse(responseCode = "201", description = "Preset created",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = TransformPreset.class))),
        @ApiResponse(responseCode = "400",
            description = "Invalid steps or request body",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "409",
            description = "Preset name already exists",
            content = @Content(schema = @Schema(type = "string")))
    })
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<TransformPreset> createPreset(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                description = "Preset name, optional description, and pipeline steps",
                content = @Content(schema = @Schema(implementation = CreatePresetRequest.class),
                    examples = @ExampleObject(value = """
                        {
                          "name": "staging-sanitize",
                          "description": "Redact PII and redirect to staging",
                          "steps": [
                            {"type": "redact", "field": "$.value.email"},
                            {"type": "redirect_topic", "topic": "orders-staging"}
                          ]
                        }""")))
            @Valid @RequestBody CreatePresetRequest req) {
        TransformPreset created = repository.create(
                req.name(), req.description(),
                req.steps() != null ? req.steps() : List.of());
        return ResponseEntity.status(201).body(created);
    }

    @Operation(
        operationId = "getTransformPreset",
        summary     = "Get a transform preset by name",
        description = "Returns the full preset including all pipeline steps.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Preset found",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = TransformPreset.class))),
        @ApiResponse(responseCode = "404", description = "Preset not found",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/{name}", produces = MediaType.APPLICATION_JSON_VALUE)
    public TransformPreset getPreset(
            @Parameter(description = "Preset name", required = true, example = "staging-sanitize")
            @PathVariable String name) {
        return repository.findByName(name)
                .orElseThrow(() -> ResourceNotFoundException.transformPreset(name));
    }

    @Operation(
        operationId = "updateTransformPreset",
        summary     = "Update a transform preset",
        description = "Replaces the description and steps of an existing preset. " +
                      "Returns HTTP 404 if the preset does not exist.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Preset updated",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = TransformPreset.class))),
        @ApiResponse(responseCode = "404", description = "Preset not found",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "400", description = "Invalid steps or request body",
            content = @Content(schema = @Schema(type = "string")))
    })
    @PutMapping(value = "/{name}",
                consumes = MediaType.APPLICATION_JSON_VALUE,
                produces = MediaType.APPLICATION_JSON_VALUE)
    public TransformPreset updatePreset(
            @Parameter(description = "Preset name", required = true, example = "staging-sanitize")
            @PathVariable String name,
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                description = "Updated description and pipeline steps",
                content = @Content(schema = @Schema(implementation = UpdatePresetRequest.class)))
            @Valid @RequestBody UpdatePresetRequest req) {
        return repository.update(
                name, req.description(),
                req.steps() != null ? req.steps() : List.of());
    }

    @Operation(
        operationId = "deleteTransformPreset",
        summary     = "Delete a transform preset",
        description = "Permanently deletes the named preset. " +
                      "Replay requests that reference this preset by name will subsequently fail.")
    @ApiResponses({
        @ApiResponse(responseCode = "204", description = "Preset deleted"),
        @ApiResponse(responseCode = "404", description = "Preset not found",
            content = @Content(schema = @Schema(type = "string")))
    })
    @DeleteMapping("/{name}")
    public ResponseEntity<Void> deletePreset(
            @Parameter(description = "Preset name", required = true, example = "staging-sanitize")
            @PathVariable String name) {
        if (!repository.delete(name)) {
            throw ResourceNotFoundException.transformPreset(name);
        }
        return ResponseEntity.noContent().build();
    }

    // =========================================================================
    // Request DTOs
    // =========================================================================

    /** Request body for {@code POST /transforms}. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record CreatePresetRequest(
            @Schema(description = "Unique preset name", example = "staging-sanitize")
            @NotBlank String name,
            @Schema(description = "Optional human-readable description",
                    example = "Redact PII and redirect to staging")
            String description,
            @Schema(description = "Ordered list of transform pipeline steps")
            List<TransformStep> steps) {}

    /** Request body for {@code PUT /transforms/{name}}. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record UpdatePresetRequest(
            @Schema(description = "Updated human-readable description")
            String description,
            @Schema(description = "Replacement ordered list of transform pipeline steps")
            List<TransformStep> steps) {}
}
