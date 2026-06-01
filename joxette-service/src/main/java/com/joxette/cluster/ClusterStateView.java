package com.joxette.cluster;

import com.joxette.recording.RecorderStatus;
import com.joxette.replay.ActiveReplayTracker;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Unified cluster state response for {@code GET /instances/cluster-state}.
 */
public record ClusterStateView(
        SelfNodeView self,
        List<InstanceRecord> instances,
        List<ClusterEventListener.MemberView> topology,
        List<ActiveReplayTracker.ActiveReplay> activeReplays
) {

    /**
     * Enriched view of the local node combining DB registration data, Pekko membership,
     * and live per-topic recorder status.
     *
     * @param instanceId         stable {@code hostname:pid} identifier
     * @param recordingEnabled   whether Kafka consumers are started on this node
     * @param compactionEnabled  whether scheduled compaction/retention runs on this node
     * @param catalogBackend     one of {@code EMBEDDED_DUCKDB}, {@code QUACK}, {@code POSTGRESQL}
     * @param startedAt          when this JVM process started
     * @param lastHeartbeat      timestamp of the most recent 30-second heartbeat
     * @param heartbeatStatus    {@code "alive"}, {@code "stale"}, or {@code "unknown"}
     * @param pekkoAddress       Pekko actor-system address; {@code null} if not yet seen
     * @param pekkoStatus        Pekko member status; {@code null} when address is {@code null}
     * @param pekkoReachable     {@code true} unless phi-accrual marks self unreachable
     * @param recorders          live per-topic recorder status
     */
    public record SelfNodeView(
            String instanceId,
            boolean recordingEnabled,
            boolean compactionEnabled,
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
