package com.joxette.exports;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.joxette.api.error.ValidationException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/exports")
@Tag(name = "Exports", description = "Async bulk entity export API")
public class ExportController {

    private final ExportService service;

    public ExportController(ExportService service) {
        this.service = service;
    }

    @Operation(
        summary     = "Submit an async export job",
        description = "Accepts up to " + ExportService.MAX_ENTITY_IDS + " entity IDs and "
                    + "writes the result to the configured object-storage path as Parquet or NDJSON. "
                    + "Poll GET /exports/{id} to track progress.")
    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ExportJob submit(@Valid @RequestBody CreateExportRequest req) {
        return service.submit(req.entityType(), req.entityIds(),
                              req.from(), req.to(), req.messageTypes(),
                              req.outputFormat() != null ? req.outputFormat() : ExportOutputFormat.PARQUET);
    }

    @Operation(summary = "List export jobs", description = "Returns all jobs, newest first.")
    @GetMapping
    public List<ExportJob> list(
            @Parameter(description = "Filter by entity type")
            @RequestParam(required = false) String entityType) {
        return service.list(entityType);
    }

    @Operation(summary = "Get export job status")
    @GetMapping("/{id}")
    public ExportJob get(
            @Parameter(description = "Export job ID", required = true)
            @PathVariable String id) {
        return service.get(id);
    }

    @Operation(summary = "Delete an export job record",
               description = "Only completed or failed jobs may be deleted.")
    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(
            @Parameter(description = "Export job ID", required = true)
            @PathVariable String id) {
        service.delete(id);
    }

    // -------------------------------------------------------------------------
    // Request body
    // -------------------------------------------------------------------------

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record CreateExportRequest(
            @NotBlank String entityType,
            @NotEmpty List<String> entityIds,
            Instant from,
            Instant to,
            List<String> messageTypes,
            ExportOutputFormat outputFormat
    ) {}
}
