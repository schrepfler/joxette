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
 *   <li>{@code "stale"} — {@code lastHeartbeat} is older than 90 seconds
 *       (instance crashed or is slow to heartbeat; it will be reaped on the next
 *       Joxette startup).</li>
 * </ul>
 *
 * @param instanceId        stable {@code hostname:pid} identifier
 * @param roles             subsystem roles active on this instance
 * @param catalogBackend    one of {@code EMBEDDED_DUCKDB}, {@code QUACK}, {@code POSTGRESQL}
 * @param startedAt         when this JVM process started recording
 * @param lastHeartbeat     timestamp of the most recent 30-second heartbeat
 * @param kafkaAssignments  map of topic → assigned partition numbers (empty list means
 *                          topic is being recorded but partition list is not tracked at
 *                          this granularity)
 * @param status            {@code "alive"} or {@code "stale"} (computed)
 */
public record InstanceRecord(
        String instanceId,
        List<String> roles,
        String catalogBackend,
        Instant startedAt,
        Instant lastHeartbeat,
        Map<String, List<Integer>> kafkaAssignments,
        String status
) {}
