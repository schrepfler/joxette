package com.joxette.cluster;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * REST controller exposing two cluster-membership views.
 *
 * <pre>
 * GET /instances          — DB-table view (joxette_instances); 30 s heartbeat resolution
 * GET /instances/topology — Pekko cluster view; phi-accrual, ~10 s failure detection,
 *                           independent of the DuckDB catalog
 * </pre>
 *
 * <p>The Pekko topology endpoint is the preferred view for health checks and
 * infrastructure automation. The DB-table endpoint is useful when querying
 * historical assignment data or when running against a shared PostgreSQL catalog.
 */
@Tag(name = "Observability",
     description = "Liveness probe and observability endpoints for monitoring the Joxette recording pipeline.")
@RestController
public class InstanceController {

    private final InstanceRegistry registry;
    private final ClusterEventListener clusterEventListener;

    public InstanceController(InstanceRegistry registry, ClusterEventListener clusterEventListener) {
        this.registry = registry;
        this.clusterEventListener = clusterEventListener;
    }

    @Operation(
        operationId  = "listInstances",
        summary      = "List running Joxette instances (DB-table view)",
        description  = "Returns all rows from the shared joxette_instances registry table. " +
                       "Staleness is detected via a 30-second heartbeat with a 90-second window. " +
                       "Stale rows are reaped automatically on the next Joxette startup. " +
                       "For real-time cluster membership, see GET /instances/topology."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Instance list (DB-table view)",
            content = @Content(
                mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema    = @Schema(implementation = InstanceRecord.class)))
    })
    @GetMapping(value = "/instances", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<InstanceRecord> listInstances() {
        return registry.listAll();
    }

    @Operation(
        operationId  = "listClusterTopology",
        summary      = "List cluster members (Pekko real-time view)",
        description  = "Returns the live Pekko cluster membership state. " +
                       "Node failures are detected in ~10 s via the phi-accrual failure detector — " +
                       "much faster than the 30-second heartbeat used by the DB table. " +
                       "This view is independent of the DuckDB catalog: it survives catalog file loss."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Cluster topology (Pekko view)",
            content = @Content(
                mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema    = @Schema(implementation = ClusterEventListener.MemberView.class)))
    })
    @GetMapping(value = "/instances/topology", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<ClusterEventListener.MemberView> clusterTopology() {
        return clusterEventListener.currentMembers();
    }
}
