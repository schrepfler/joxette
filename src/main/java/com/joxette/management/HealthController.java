package com.joxette.management;

import com.joxette.config.JoxetteProperties;
import com.joxette.recording.RecordingCoordinator;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
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

/**
 * Liveness and observability endpoints.
 *
 * <pre>
 * GET /health   – liveness probe with consumer lag, catalog size, inlined data size
 * GET /metrics  – Prometheus scrape endpoint (mirrors /actuator/prometheus)
 * </pre>
 */
@RestController
public class HealthController {

    /**
     * Consumer-lag summary for one active topic.
     *
     * @param totalLag       sum of per-partition lag; {@code -1} if Kafka is unreachable
     * @param lagByPartition per-partition lag values (empty when totalLag is -1)
     */
    public record TopicLag(String topic, long totalLag, Map<Integer, Long> lagByPartition) {}

    public record HealthStatus(
            String status,
            List<String> activeRecorders,
            List<TopicLag> consumerLag,
            long catalogSizeBytes,
            long inlinedDataSizeBytes,
            String catalogPath
    ) {}

    private final RecordingCoordinator coordinator;
    private final JoxetteProperties properties;
    private final AdminClient adminClient;
    private final Connection duckDB;
    private final PrometheusMeterRegistry metricsRegistry;

    public HealthController(
            RecordingCoordinator coordinator,
            JoxetteProperties properties,
            AdminClient adminClient,
            Connection duckDB,
            PrometheusMeterRegistry metricsRegistry) {
        this.coordinator     = coordinator;
        this.properties      = properties;
        this.adminClient     = adminClient;
        this.duckDB          = duckDB;
        this.metricsRegistry = metricsRegistry;
    }

    @GetMapping(value = "/health", produces = MediaType.APPLICATION_JSON_VALUE)
    public HealthStatus health() {
        List<String> active = coordinator.activeTopics().stream().sorted().toList();
        return new HealthStatus(
                "UP",
                active,
                computeConsumerLag(active),
                catalogSizeBytes(),
                inlinedDataSizeBytes(),
                properties.getCatalog().getPath());
    }

    @GetMapping(value = "/metrics", produces = MediaType.TEXT_PLAIN_VALUE)
    public String metrics() {
        return metricsRegistry.scrape();
    }

    // -------------------------------------------------------------------------
    // Consumer lag
    // -------------------------------------------------------------------------

    private List<TopicLag> computeConsumerLag(List<String> topics) {
        if (topics.isEmpty()) return List.of();
        List<TopicLag> result = new ArrayList<>();
        for (String topic : topics) {
            result.add(lagForTopic(topic));
        }
        return result;
    }

    private TopicLag lagForTopic(String topic) {
        String groupId = "joxette-recorder-" + topic;
        try {
            // Committed offsets for this consumer group.
            var committed = adminClient
                    .listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get(5, TimeUnit.SECONDS);
            if (committed.isEmpty()) {
                return new TopicLag(topic, -1, Map.of());
            }
            // End offsets for the same set of topic-partitions.
            Map<TopicPartition, OffsetSpec> offsetRequest = new LinkedHashMap<>();
            for (TopicPartition tp : committed.keySet()) {
                offsetRequest.put(tp, OffsetSpec.latest());
            }
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = adminClient
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
            return new TopicLag(topic, total, lagByPartition);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new TopicLag(topic, -1, Map.of());
        } catch (ExecutionException | TimeoutException e) {
            return new TopicLag(topic, -1, Map.of());
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
