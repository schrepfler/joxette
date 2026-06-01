package com.joxette.cluster;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * One row from {@code joxette_instances} as returned by {@code GET /instances}.
 *
 * <p>The {@code status} field is computed server-side:
 * <ul>
 *   <li>{@code "alive"} — {@code lastHeartbeat} is within 90 seconds of now.</li>
 *   <li>{@code "stale"} — {@code lastHeartbeat} is older than 90 seconds.</li>
 * </ul>
 *
 * @param instanceId         stable {@code hostname:pid} identifier
 * @param recordingEnabled   whether Kafka consumers start on this node
 * @param compactionEnabled  whether scheduled compaction/retention runs on this node
 * @param catalogBackend     one of {@code EMBEDDED_DUCKDB}, {@code QUACK}, {@code POSTGRESQL}
 * @param startedAt          when this JVM process started
 * @param lastHeartbeat      timestamp of the most recent 30-second heartbeat
 * @param kafkaAssignments   map of topic → assigned partition numbers
 * @param status             {@code "alive"} or {@code "stale"} (computed)
 */
public record InstanceRecord(
        String instanceId,
        boolean recordingEnabled,
        boolean compactionEnabled,
        String catalogBackend,
        Instant startedAt,
        Instant lastHeartbeat,
        Map<String, List<Integer>> kafkaAssignments,
        String status
) {}
