package com.joxette.cluster;

import com.joxette.recording.RecorderStatus;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Unified cluster state response for {@code GET /instances/cluster-state}.
 *
 * <p>Combines three existing views:
 * <ul>
 *   <li>{@link #self} — this node, enriched with live recorder status and Pekko reachability</li>
 *   <li>{@link #instances} — all nodes from the DB heartbeat table (30 s resolution)</li>
 *   <li>{@link #topology} — all members from the Pekko phi-accrual view (~10 s failure detection)</li>
 * </ul>
 *
 * @param self      enriched view of the node serving this request
 * @param instances all rows from {@code joxette_instances} (same as {@code GET /instances})
 * @param topology  live Pekko cluster members (same as {@code GET /instances/topology})
 */
public record ClusterStateView(
        SelfNodeView self,
        List<InstanceRecord> instances,
        List<ClusterEventListener.MemberView> topology
) {

    /**
     * Enriched view of the local node combining DB registration data, Pekko membership,
     * and live per-topic recorder status.
     *
     * @param instanceId      stable {@code hostname:pid} identifier
     * @param roles           active subsystem roles on this node
     * @param catalogBackend  one of {@code EMBEDDED_DUCKDB}, {@code QUACK}, {@code POSTGRESQL}
     * @param startedAt       when this JVM process started
     * @param lastHeartbeat   timestamp of the most recent 30-second heartbeat ({@code null} if
     *                        this node's DB row is not yet visible)
     * @param heartbeatStatus {@code "alive"}, {@code "stale"}, or {@code "unknown"}
     * @param pekkoAddress    Pekko actor-system address ({@code pekko://joxette@host:port});
     *                        {@code null} if self not yet seen by {@link ClusterEventListener}
     * @param pekkoStatus     Pekko member status ({@code "up"}, {@code "leaving"}, etc.);
     *                        {@code null} when {@code pekkoAddress} is {@code null}
     * @param pekkoReachable  {@code true} unless the phi-accrual detector marks self unreachable
     * @param recorders       live per-topic recorder status; empty when {@code recorder} role
     *                        is not active on this node
     */
    public record SelfNodeView(
            String instanceId,
            List<String> roles,
            String catalogBackend,
            Instant startedAt,
            Instant lastHeartbeat,
            String heartbeatStatus,
            String pekkoAddress,
            String pekkoStatus,
            boolean pekkoReachable,
            Map<String, RecorderStatus> recorders
    ) {}
}
