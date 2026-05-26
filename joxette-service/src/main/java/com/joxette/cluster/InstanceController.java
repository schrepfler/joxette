package com.joxette.cluster;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.config.InstanceRoles;
import com.joxette.recording.RecorderStatus;
import com.joxette.recording.RecordingCoordinator;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
    private final InstanceRoles instanceRoles;
    private final RecordingCoordinator recordingCoordinator;
    private final ObjectMapper objectMapper;

    public InstanceController(
            InstanceRegistry registry,
            ClusterEventListener clusterEventListener,
            InstanceRoles instanceRoles,
            @Lazy RecordingCoordinator recordingCoordinator,
            ObjectMapper objectMapper) {
        this.registry = registry;
        this.clusterEventListener = clusterEventListener;
        this.instanceRoles = instanceRoles;
        this.recordingCoordinator = recordingCoordinator;
        this.objectMapper = objectMapper;
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

    @Operation(
        operationId  = "clusterState",
        summary      = "Unified cluster state (self + all instances + Pekko topology)",
        description  = "Returns a single document combining three views: the local node enriched " +
                       "with live recorder status and Pekko reachability, all registered instances " +
                       "from the DB heartbeat table, and all Pekko cluster members. " +
                       "Use this endpoint to see at a glance what every node is doing and whether " +
                       "the cluster is healthy."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Unified cluster state",
            content = @Content(
                mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema    = @Schema(implementation = ClusterStateView.class)))
    })
    @GetMapping(value = "/instances/cluster-state", produces = MediaType.APPLICATION_JSON_VALUE)
    public ClusterStateView clusterState() {
        return buildClusterState();
    }

    @Operation(
        operationId  = "liveMetrics",
        summary      = "Live recorder metrics stream (SSE)",
        description  = "Streams a snapshot of all running recorder metrics every 2 seconds as " +
                       "server-sent events. Each event contains the full cluster-state payload. " +
                       "The stream continues until the client disconnects."
    )
    @GetMapping(value = "/instances/live-metrics", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter liveMetrics() {
        SseEmitter emitter = new SseEmitter(0L);

        Thread vt = Thread.ofVirtual().name("live-metrics-sse").unstarted(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    ClusterStateView snapshot = buildClusterState();
                    String json;
                    try {
                        json = objectMapper.writeValueAsString(snapshot);
                    } catch (JsonProcessingException e) {
                        emitter.completeWithError(e);
                        return;
                    }
                    try {
                        emitter.send(SseEmitter.event().name("metrics").data(json, MediaType.APPLICATION_JSON));
                    } catch (IOException e) {
                        // Client disconnected — exit cleanly without calling complete().
                        return;
                    }
                    Thread.sleep(2_000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                // Only complete if the emitter hasn't been closed externally already.
                try { emitter.complete(); } catch (IllegalStateException ignored) {}
            }
        });

        // Interrupt the VT when Tomcat closes the connection (client disconnect, graceful
        // shutdown, or async timeout) so the thread exits promptly without blocking Tomcat.
        Runnable interrupt = vt::interrupt;
        emitter.onCompletion(interrupt);
        emitter.onTimeout(interrupt);
        emitter.onError(t -> interrupt.run());

        vt.start();
        return emitter;
    }

    private ClusterStateView buildClusterState() {
        String selfId = registry.getInstanceId();
        List<InstanceRecord> instances = registry.listAll();
        List<ClusterEventListener.MemberView> topology = clusterEventListener.currentMembers();

        InstanceRecord selfRecord = instances.stream()
                .filter(r -> r.instanceId().equals(selfId))
                .findFirst().orElse(null);

        String selfHost = selfId.contains(":") ? selfId.substring(0, selfId.lastIndexOf(':')) : selfId;
        ClusterEventListener.MemberView selfMember = topology.stream()
                .filter(m -> m.address().contains("@" + selfHost + ":"))
                .findFirst().orElse(null);

        Map<String, RecorderStatus> recorders = instanceRoles.isRecorder()
                ? recordingCoordinator.listRunning()
                : Map.of();

        ClusterStateView.SelfNodeView self = new ClusterStateView.SelfNodeView(
                selfId,
                selfRecord != null
                        ? selfRecord.roles()
                        : instanceRoles.resolvedRoles().stream().sorted().toList(),
                selfRecord != null ? selfRecord.catalogBackend() : "unknown",
                selfRecord != null ? selfRecord.startedAt() : null,
                selfRecord != null ? selfRecord.lastHeartbeat() : null,
                selfRecord != null ? selfRecord.status() : "unknown",
                selfMember != null ? selfMember.address() : null,
                selfMember != null ? selfMember.status() : null,
                selfMember == null || selfMember.reachable(),
                recorders
        );

        return new ClusterStateView(self, instances, topology);
    }
}
