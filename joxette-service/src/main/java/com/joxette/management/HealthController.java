package com.joxette.management;

import com.joxette.cluster.InstanceRegistry;
import com.joxette.config.InstanceRoles;
import com.joxette.config.JoxetteProperties;
import com.joxette.recording.RecordingCoordinator;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Liveness and observability endpoints.
 *
 * <pre>
 * GET /health   – liveness probe with consumer lag, catalog size, inlined data size
 * GET /metrics  – Prometheus scrape endpoint (mirrors /actuator/prometheus)
 * </pre>
 */
@Tag(name = "Observability",
     description = "Liveness probe and observability endpoints for monitoring the Joxette recording pipeline.")
@RestController
public class HealthController {

    /**
     * Consumer-lag summary for one active topic.
     *
     * @param totalLag         sum of per-partition lag; {@code -1} if Kafka is unreachable
     * @param lagByPartition   per-partition lag values (empty when totalLag is -1)
     * @param protocol         negotiated Kafka group protocol ({@code consumer} or {@code classic});
     *                         {@code "unknown"} until the first rebalance fires
     * @param partitions       currently assigned partition IDs; empty before the first rebalance
     */
    @Schema(description = "Consumer-lag summary for one active Kafka topic",
            example = "{\"topic\": \"orders\", \"totalLag\": 150, \"lagByPartition\": {\"0\": 80, \"1\": 70}, \"protocol\": \"consumer\", \"partitions\": [0, 1, 2]}")
    public record TopicLag(
            @Schema(description = "Kafka topic name", example = "orders")
            String topic,
            @Schema(description = "Sum of per-partition lag; -1 if Kafka is unreachable", example = "150")
            long totalLag,
            @Schema(description = "Per-partition lag values; empty when totalLag is -1",
                    example = "{\"0\": 80, \"1\": 70}")
            Map<Integer, Long> lagByPartition,
            @Schema(description = "Negotiated Kafka group protocol; 'consumer' for KIP-848, 'classic' for legacy",
                    example = "consumer")
            String protocol,
            @Schema(description = "Currently assigned partition IDs on this instance",
                    example = "[0, 1, 2]")
            List<Integer> partitions) {}

    /**
     * Cluster-level summary derived from the {@code joxette_instances} registry table.
     *
     * @param instanceCount total number of rows in the registry
     * @param alive         rows whose {@code last_heartbeat} is within 90 seconds of now
     * @param stale         rows whose {@code last_heartbeat} is older than 90 seconds
     */
    @Schema(description = "Cluster-level instance count from the joxette_instances registry",
            example = "{\"instanceCount\": 3, \"alive\": 3, \"stale\": 0}")
    public record ClusterSummary(
            @Schema(description = "Total number of registered instances", example = "3")
            int instanceCount,
            @Schema(description = "Instances with a recent heartbeat (within 90 s)", example = "3")
            int alive,
            @Schema(description = "Instances with a stale heartbeat (older than 90 s)", example = "0")
            int stale) {}

    @Schema(description = "Liveness and observability response from GET /health", example = """
            {
              "status": "UP",
              "activeRecorders": ["orders", "payments"],
              "consumerLag": [
                {"topic": "orders", "totalLag": 150, "lagByPartition": {"0": 80, "1": 70}},
                {"topic": "payments", "totalLag": 0, "lagByPartition": {"0": 0}}
              ],
              "catalogSizeBytes": 1073741824,
              "inlinedDataSizeBytes": 52428800,
              "catalogPath": "./data/joxette.ducklake",
              "roles": ["compaction", "entity-router", "recorder", "replay"],
              "instanceId": "myhost:12345",
              "cluster": {"instanceCount": 1, "alive": 1, "stale": 0}
            }""")
    public record HealthStatus(
            @Schema(description = "Liveness status; always 'UP' when the endpoint responds", example = "UP")
            String status,
            @Schema(description = "Sorted list of Kafka topics currently being recorded",
                    example = "[\"orders\", \"payments\"]")
            List<String> activeRecorders,
            @Schema(description = "Consumer lag per active topic; empty when no topics are active")
            List<TopicLag> consumerLag,
            @Schema(description = "Size of the DuckDB catalog file in bytes; -1 if unreadable", example = "1073741824")
            long catalogSizeBytes,
            @Schema(description = "Estimated bytes of data buffered inline in the DuckDB catalog (not yet flushed to Parquet); -1 on error",
                    example = "52428800")
            long inlinedDataSizeBytes,
            @Schema(description = "Filesystem path to the DuckDB catalog file", example = "./data/joxette.ducklake")
            String catalogPath,
            @Schema(description = "Active subsystem roles on this instance (expanded from joxette.roles); sorted alphabetically",
                    example = "[\"compaction\", \"entity-router\", \"recorder\", \"replay\"]")
            List<String> roles,
            @Schema(description = "Unique identifier for this instance in hostname:pid format",
                    example = "myhost:12345")
            String instanceId,
            @Schema(description = "Cluster-level instance count derived from joxette_instances registry",
                    example = "{\"instanceCount\": 1, \"alive\": 1, \"stale\": 0}")
            ClusterSummary cluster
    ) {}

    private final RecordingCoordinator coordinator;
    private final JoxetteProperties properties;
    private final InstanceRoles instanceRoles;
    private final com.joxette.config.BrokerConnectionFactory brokerConnectionFactory;
    private final Connection duckDB;
    private final PrometheusMeterRegistry metricsRegistry;
    private final InstanceRegistry instanceRegistry;

    /**
     * Cached consumer lag result — refreshed at most once every 15 seconds
     * via a short-lived AdminClient that is created, used, and immediately closed.
     * No persistent AdminClient thread → no reconnect storm when Kafka is down.
     */
    private final AtomicReference<List<TopicLag>> lagCache = new AtomicReference<>(List.of());
    /** Epoch-ms timestamp of the last successful lag query. */
    private final AtomicLong lagCacheTime = new AtomicLong(0);
    private static final long LAG_CACHE_TTL_MS = 15_000;

    public HealthController(
            @Lazy RecordingCoordinator coordinator,
            JoxetteProperties properties,
            InstanceRoles instanceRoles,
            com.joxette.config.BrokerConnectionFactory brokerConnectionFactory,
            Connection duckDB,
            PrometheusMeterRegistry metricsRegistry,
            InstanceRegistry instanceRegistry) {
        this.coordinator              = coordinator;
        this.properties               = properties;
        this.instanceRoles            = instanceRoles;
        this.brokerConnectionFactory  = brokerConnectionFactory;
        this.duckDB                   = duckDB;
        this.metricsRegistry          = metricsRegistry;
        this.instanceRegistry         = instanceRegistry;
    }

    @Operation(
        operationId = "getHealth",
        summary = "Liveness probe",
        description = "Returns the liveness status of the Joxette service along with observability metrics: " +
                      "active recorder topics, per-topic Kafka consumer lag, catalog file size, and the amount " +
                      "of data buffered inline in the DuckDB catalog that has not yet been flushed to Parquet."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Service is alive",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = HealthStatus.class),
                examples = @ExampleObject(name = "healthy", value = """
                    {
                      "status": "UP",
                      "activeRecorders": ["orders", "payments"],
                      "consumerLag": [
                        {"topic": "orders", "totalLag": 150, "lagByPartition": {"0": 80, "1": 70}},
                        {"topic": "payments", "totalLag": 0, "lagByPartition": {"0": 0}}
                      ],
                      "catalogSizeBytes": 1073741824,
                      "inlinedDataSizeBytes": 52428800,
                      "catalogPath": "./data/joxette.ducklake"
                    }""")))
    })
    @GetMapping(value = "/health", produces = MediaType.APPLICATION_JSON_VALUE)
    public HealthStatus health() {
        Map<String, com.joxette.recording.RecorderStatus> running = coordinator.listRunning();
        List<String> active      = running.keySet().stream().sorted().toList();
        List<String> activeRoles = instanceRoles.resolvedRoles().stream().sorted().toList();
        ClusterSummary cluster   = buildClusterSummary();
        return new HealthStatus(
                "UP",
                active,
                computeConsumerLag(active, running),
                catalogSizeBytes(),
                inlinedDataSizeBytes(),
                properties.getCatalog().getPath(),
                activeRoles,
                instanceRegistry.getInstanceId(),
                cluster);
    }

    /** Reads the instance registry and computes alive/stale counts. */
    private ClusterSummary buildClusterSummary() {
        var instances = instanceRegistry.listAll();
        long alive = instances.stream().filter(r -> "alive".equals(r.status())).count();
        return new ClusterSummary(instances.size(), (int) alive, instances.size() - (int) alive);
    }

    @GetMapping(value = "/metrics", produces = MediaType.TEXT_PLAIN_VALUE)
    public String metrics() {
        return metricsRegistry.scrape();
    }

    // -------------------------------------------------------------------------
    // Consumer lag
    // -------------------------------------------------------------------------

    /**
     * Returns consumer lag for all active topics, enriched with protocol and
     * partition assignment from live {@link com.joxette.recording.RecorderStatus} snapshots.
     *
     * <p>Uses a 15-second result cache. When the cache is stale, opens a
     * short-lived {@link AdminClient}, queries all topics, then closes it
     * immediately. There is no persistent AdminClient background thread — the
     * source of the reconnect storm when Kafka is down.
     */
    private List<TopicLag> computeConsumerLag(
            List<String> topics,
            Map<String, com.joxette.recording.RecorderStatus> running) {
        if (topics.isEmpty()) return List.of();
        long now = System.currentTimeMillis();
        if (now - lagCacheTime.get() < LAG_CACHE_TTL_MS) {
            return lagCache.get();
        }
        List<TopicLag> result = new ArrayList<>();
        try (AdminClient client = brokerConnectionFactory.adminClient(
                com.joxette.management.BrokerConfig.DEFAULT_BROKER_ID)) {
            for (String topic : topics) {
                result.add(lagForTopic(client, topic, running.get(topic)));
            }
        } catch (Exception e) {
            // Kafka unreachable — return cached (possibly empty) values
            return lagCache.get();
        }
        lagCache.set(List.copyOf(result));
        lagCacheTime.set(now);
        return result;
    }

    private TopicLag lagForTopic(AdminClient client, String topic,
            com.joxette.recording.RecorderStatus status) {
        String protocol   = status != null ? status.protocol()           : "unknown";
        List<Integer> pts = status != null
                ? status.assignedPartitions().stream().sorted().toList()
                : List.of();

        String groupId = properties.getKafka().getConsumerGroup() + "-" + topic;
        try {
            var committed = client
                    .listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get(5, TimeUnit.SECONDS);
            if (committed.isEmpty()) {
                return new TopicLag(topic, -1, Map.of(), protocol, pts);
            }
            Map<TopicPartition, OffsetSpec> offsetRequest = new LinkedHashMap<>();
            for (TopicPartition tp : committed.keySet()) {
                offsetRequest.put(tp, OffsetSpec.latest());
            }
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = client
                    .listOffsets(offsetRequest)
                    .all()
                    .get(5, TimeUnit.SECONDS);

            Map<Integer, Long> lagByPartition = new TreeMap<>();
            long total = 0;
            for (var entry : committed.entrySet()) {
                TopicPartition tp = entry.getKey();
                long end = endOffsets.getOrDefault(tp,
                        new ListOffsetsResult.ListOffsetsResultInfo(-1, -1, Optional.empty())).offset();
                long lag = (end < 0) ? -1 : Math.max(0, end - entry.getValue().offset());
                lagByPartition.put(tp.partition(), lag);
                if (lag >= 0) total += lag;
            }
            return new TopicLag(topic, total, lagByPartition, protocol, pts);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new TopicLag(topic, -1, Map.of(), protocol, pts);
        } catch (ExecutionException | TimeoutException e) {
            return new TopicLag(topic, -1, Map.of(), protocol, pts);
        }
    }

    // -------------------------------------------------------------------------
    // Storage sizes
    // -------------------------------------------------------------------------

    private long catalogSizeBytes() {
        try {
            return Files.size(Path.of(properties.getCatalog().getPath()));
        } catch (IOException e) {
            return -1;
        }
    }

    /**
     * Returns the sum of DuckDB's estimated table sizes across the {@code lake}
     * schema.  This approximates the amount of data stored inline in the DuckDB
     * catalog file (i.e. not yet flushed to object storage as Parquet files).
     */
    private long inlinedDataSizeBytes() {
        try {
            synchronized (duckDB) {
                try (Statement st = duckDB.createStatement();
                     ResultSet rs = st.executeQuery(
                             "SELECT COALESCE(SUM(estimated_size), 0) AS total " +
                             "FROM duckdb_tables() WHERE schema_name = 'lake'")) {
                    return rs.next() ? rs.getLong("total") : 0;
                }
            }
        } catch (SQLException e) {
            return -1;
        }
    }
}
