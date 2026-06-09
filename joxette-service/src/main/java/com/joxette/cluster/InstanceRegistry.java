package com.joxette.cluster;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.config.JoxetteProperties;
import com.joxette.db.DuckLakeManager;
import com.joxette.recording.RecordingCoordinator;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Tracks running Joxette instances in the shared {@code joxette_instances} DuckDB table.
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *   <li>On startup ({@link #initialize()}): reap stale instances whose
 *       {@code last_heartbeat} is older than 2 minutes (crashed pods / previous runs),
 *       then upsert this instance's own row.</li>
 *   <li>Every 30 seconds (background virtual thread): update {@code last_heartbeat}
 *       and {@code kafka_assignments} with the currently active topic set.</li>
 *   <li>On clean shutdown ({@link #deregister()}): interrupt the heartbeat thread and
 *       delete this instance's row so the table stays clean after graceful pod restarts.</li>
 * </ol>
 *
 * <h2>Status semantics</h2>
 * <ul>
 *   <li>{@code alive} — {@code last_heartbeat} within 90 seconds of now.</li>
 *   <li>{@code stale} — {@code last_heartbeat} older than 90 seconds (visible via
 *       {@code GET /instances} until the next Joxette startup reaps it).</li>
 * </ul>
 *
 * <h2>Multi-instance note</h2>
 * <p>In embedded DuckDB mode this table always holds exactly one row (the local instance).
 * In Quack or PostgreSQL catalog mode the table is shared across all Joxette processes,
 * giving a cluster-wide view of which instances are running and what they are doing.
 *
 * <h2>Thread safety</h2>
 * <p>All JDBC operations are wrapped in {@code synchronized (connection)} to prevent
 * concurrent statement execution errors on the shared DuckDB connection.
 *
 * @see InstanceController
 */
@Component
@DependsOn("dbSchemaManager")   // ensures joxette_instances table exists before we write to it
public class InstanceRegistry {

    private static final Logger log = LoggerFactory.getLogger(InstanceRegistry.class);

    /** Instances with heartbeat older than this are reaped at startup. */
    static final Duration STALE_THRESHOLD = Duration.ofMinutes(2);

    /** Instances with heartbeat within this window are reported as {@code "alive"}. */
    static final Duration ALIVE_THRESHOLD = Duration.ofSeconds(90);

    private final Connection connection;
    private final JoxetteProperties properties;
    private final DuckLakeManager duckLakeManager;
    private final RecordingCoordinator coordinator;
    private final ObjectMapper objectMapper;

    /** Stable {@code hostname:pid} for this JVM process. Computed once at construction. */
    private final String instanceId;
    private final Instant startedAt = Instant.now();

    /** Background virtual thread driving the 30-second heartbeat loop. */
    private volatile Thread heartbeatThread;

    public InstanceRegistry(
            Connection connection,
            JoxetteProperties properties,
            DuckLakeManager duckLakeManager,
            @Lazy RecordingCoordinator coordinator,
            ObjectMapper objectMapper) {
        this.connection      = connection;
        this.properties      = properties;
        this.duckLakeManager = duckLakeManager;
        this.coordinator     = coordinator;
        this.objectMapper    = objectMapper;
        this.instanceId      = resolveInstanceId();
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @PostConstruct
    public void initialize() {
        reapStaleInstances();
        upsertSelf();
        heartbeatThread = Thread.startVirtualThread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(30_000);
                    sendHeartbeat();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        log.info("Instance registry active: instanceId={}, recording={}, compaction={}",
                instanceId,
                properties.getRecording().isEnabled(),
                properties.getCompaction().isEnabled());
    }

    @PreDestroy
    public void deregister() {
        log.info("Deregistering instance from registry: {}", instanceId);
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
        }
        deleteSelf();
    }

    // -------------------------------------------------------------------------
    // DB operations (public for testability — all are idempotent)
    // -------------------------------------------------------------------------

    /**
     * Deletes rows whose {@code last_heartbeat} is older than {@link #STALE_THRESHOLD}.
     * Called at startup to clean up rows left by crashed pods or previous runs.
     */
    public void reapStaleInstances() {
        try {
            synchronized (connection) {
                try (PreparedStatement ps = connection.prepareStatement(
                        "DELETE FROM joxette_instances " +
                        "WHERE last_heartbeat < now() - INTERVAL '2 minutes'")) {
                    int deleted = ps.executeUpdate();
                    if (deleted > 0) {
                        log.info("Reaped {} stale instance row(s) from registry", deleted);
                    } else {
                        log.debug("No stale instances to reap");
                    }
                }
            }
        } catch (SQLException e) {
            log.warn("Failed to reap stale instances: {}", e.getMessage());
        }
    }

    /**
     * Updates {@code last_heartbeat} and {@code kafka_assignments} for this instance.
     * Called every 30 seconds from the background virtual thread.
     *
     * <p>If the row is missing (e.g. dropped during an external maintenance op),
     * re-inserts it via {@link #upsertSelf()}.
     */
    public void sendHeartbeat() {
        String assignmentsJson = buildKafkaAssignmentsJson();
        try {
            synchronized (connection) {
                try (PreparedStatement ps = connection.prepareStatement(
                        "UPDATE joxette_instances " +
                        "SET last_heartbeat = now(), kafka_assignments = ? " +
                        "WHERE instance_id = ?")) {
                    ps.setString(1, assignmentsJson);
                    ps.setString(2, instanceId);
                    int updated = ps.executeUpdate();
                    if (updated == 0) {
                        log.warn("Heartbeat found no row for instanceId={}; re-inserting", instanceId);
                        upsertSelf();
                    }
                }
            }
        } catch (SQLException e) {
            // Log SQL error detail and attempt recovery via re-insert — a transient
            // failure (e.g. lock contention after sleep/wake) should not leave this
            // instance permanently invisible in the registry.
            log.warn("Failed to send heartbeat for instanceId={} [sqlErrorCode={}, sqlState={}]; attempting re-insert",
                    instanceId, e.getErrorCode(), e.getSQLState());
            upsertSelf();
        }
    }

    // -------------------------------------------------------------------------
    // Query (used by InstanceController and HealthController)
    // -------------------------------------------------------------------------

    /** Returns all rows from {@code joxette_instances} with the computed {@code status} field. */
    public List<InstanceRecord> listAll() {
        Instant aliveAfter = Instant.now().minus(ALIVE_THRESHOLD);
        List<InstanceRecord> result = new ArrayList<>();
        try {
            synchronized (connection) {
                try (Statement st = connection.createStatement();
                     ResultSet rs = st.executeQuery(
                             "SELECT instance_id, recording_enabled, compaction_enabled, " +
                             "catalog_backend, started_at, last_heartbeat, kafka_assignments " +
                             "FROM joxette_instances ORDER BY instance_id")) {
                    while (rs.next()) {
                        result.add(mapRow(rs, aliveAfter));
                    }
                }
            }
        } catch (SQLException e) {
            log.warn("Failed to list instances: {}", e.getMessage());
        }
        return Collections.unmodifiableList(result);
    }

    /** Returns the stable {@code hostname:pid} identifier for this JVM process. */
    public String getInstanceId() {
        return instanceId;
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void upsertSelf() {
        boolean recEnabled  = properties.getRecording().isEnabled();
        boolean cmpEnabled  = properties.getCompaction().isEnabled();
        String  backend     = duckLakeManager.getBackend().name();
        String  assignmentsJson = buildKafkaAssignmentsJson();

        String sql =
                "INSERT INTO joxette_instances " +
                "    (instance_id, recording_enabled, compaction_enabled, catalog_backend," +
                "     started_at, last_heartbeat, kafka_assignments) " +
                "VALUES (?, ?, ?, ?, ?, now(), ?) " +
                "ON CONFLICT (instance_id) DO UPDATE SET " +
                "    recording_enabled  = EXCLUDED.recording_enabled, " +
                "    compaction_enabled = EXCLUDED.compaction_enabled, " +
                "    catalog_backend    = EXCLUDED.catalog_backend, " +
                "    started_at         = EXCLUDED.started_at, " +
                "    last_heartbeat     = EXCLUDED.last_heartbeat, " +
                "    kafka_assignments  = EXCLUDED.kafka_assignments";

        try {
            synchronized (connection) {
                try (PreparedStatement ps = connection.prepareStatement(sql)) {
                    ps.setString(1, instanceId);
                    ps.setBoolean(2, recEnabled);
                    ps.setBoolean(3, cmpEnabled);
                    ps.setString(4, backend);
                    ps.setTimestamp(5, Timestamp.from(startedAt));
                    ps.setString(6, assignmentsJson);
                    ps.executeUpdate();
                }
            }
            log.debug("Instance row upserted: instanceId={}", instanceId);
        } catch (SQLException e) {
            log.error("Failed to register instance '{}': {}", instanceId, e.getMessage());
        }
    }

    private void deleteSelf() {
        try {
            synchronized (connection) {
                try (PreparedStatement ps = connection.prepareStatement(
                        "DELETE FROM joxette_instances WHERE instance_id = ?")) {
                    ps.setString(1, instanceId);
                    int deleted = ps.executeUpdate();
                    if (deleted > 0) {
                        log.info("Instance '{}' removed from registry", instanceId);
                    } else {
                        log.debug("Instance '{}' had already been removed from registry", instanceId);
                    }
                }
            }
        } catch (SQLException e) {
            log.warn("Failed to deregister instance '{}': {}", instanceId, e.getMessage());
        }
    }

    /**
     * Serialises the current Kafka topic assignments to JSON.
     *
     * <p>The active topics come from {@link RecordingCoordinator#activeTopics()}.  Each topic
     * maps to an empty partition list: partition-level tracking is not yet exposed at this
     * layer — the presence of the key indicates that this instance owns the topic.
     */
    private String buildKafkaAssignmentsJson() {
        Set<String> activeTopics = coordinator.activeTopics();
        Map<String, List<Integer>> assignments = new LinkedHashMap<>();
        activeTopics.stream().sorted().forEach(t -> assignments.put(t, List.of()));
        try {
            return objectMapper.writeValueAsString(assignments);
        } catch (JsonProcessingException e) {
            log.debug("Could not serialise kafka_assignments: {}", e.getMessage());
            return "{}";
        }
    }

    private InstanceRecord mapRow(ResultSet rs, Instant aliveAfter) throws SQLException {
        String  id         = rs.getString("instance_id");
        boolean recEnabled = rs.getBoolean("recording_enabled");
        boolean cmpEnabled = rs.getBoolean("compaction_enabled");
        String  backend    = rs.getString("catalog_backend");
        Instant started    = rs.getTimestamp("started_at").toInstant();
        Instant heartbeat  = rs.getTimestamp("last_heartbeat").toInstant();

        Map<String, List<Integer>> kafkaAssignments = null;
        String assignmentsStr = rs.getString("kafka_assignments");
        if (assignmentsStr != null && !assignmentsStr.isBlank() && !"{}".equals(assignmentsStr.strip())) {
            try {
                kafkaAssignments = objectMapper.readValue(assignmentsStr,
                        new TypeReference<>() {});
            } catch (Exception e) {
                log.debug("Could not parse kafka_assignments for instance '{}': {}", id, e.getMessage());
            }
        }
        if (kafkaAssignments == null) {
            kafkaAssignments = Map.of();
        }

        String status = heartbeat.isAfter(aliveAfter) ? "alive" : "stale";
        return new InstanceRecord(id, recEnabled, cmpEnabled, backend, started, heartbeat, kafkaAssignments, status);
    }

    private static String resolveInstanceId() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "unknown";
        }
        return host + ":" + ProcessHandle.current().pid();
    }
}
