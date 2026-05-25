package com.joxette.cluster;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * REST controller for the shared instance registry.
 *
 * <pre>
 * GET /instances — returns all rows from joxette_instances with a computed
 *                  'status' field ('alive' | 'stale').
 * </pre>
 *
 * <p>In embedded DuckDB mode (single-process) this endpoint always returns a
 * single entry.  In Quack or PostgreSQL catalog mode it shows every Joxette
 * instance connected to the shared catalog.
 */
@Tag(name = "Observability",
     description = "Liveness probe and observability endpoints for monitoring the Joxette recording pipeline.")
@RestController
public class InstanceController {

    private final InstanceRegistry registry;

    public InstanceController(InstanceRegistry registry) {
        this.registry = registry;
    }

    @Operation(
        operationId  = "listInstances",
        summary      = "List running Joxette instances",
        description  = "Returns all rows from the shared joxette_instances registry table. " +
                       "Each row represents one Joxette process. The 'status' field is computed: " +
                       "'alive' if last_heartbeat is within 90 seconds of now, 'stale' otherwise. " +
                       "Stale rows are reaped automatically on the next Joxette startup."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Instance list",
            content = @Content(
                mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema    = @Schema(implementation = InstanceRecord.class),
                examples  = @ExampleObject(name = "two-node cluster", value = """
                    [
                      {
                        "instanceId": "recorder-pod-1:1234",
                        "roles": ["compaction", "entity-router", "recorder", "replay"],
                        "catalogBackend": "POSTGRESQL",
                        "startedAt": "2025-01-15T08:00:00Z",
                        "lastHeartbeat": "2025-01-15T10:30:00Z",
                        "kafkaAssignments": {"orders.events": [], "payments.events": []},
                        "status": "alive"
                      },
                      {
                        "instanceId": "compaction-pod-1:5678",
                        "roles": ["compaction"],
                        "catalogBackend": "POSTGRESQL",
                        "startedAt": "2025-01-15T08:00:05Z",
                        "lastHeartbeat": "2025-01-15T10:30:05Z",
                        "kafkaAssignments": {},
                        "status": "alive"
                      }
                    ]""")))
    })
    @GetMapping(value = "/instances", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<InstanceRecord> listInstances() {
        return registry.listAll();
    }
}
