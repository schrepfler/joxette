package com.joxette.metrics;

import io.micrometer.core.instrument.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Central Micrometer instrumentation for Joxette.
 *
 * <h2>Recording metrics</h2>
 * <ul>
 *   <li>{@code joxette.messages.consumed} — cumulative messages consumed per topic</li>
 *   <li>{@code joxette.messages.written}  — cumulative messages written to DuckLake per topic</li>
 *   <li>{@code joxette.consumer.lag}      — current consumer lag per topic (gauge)</li>
 *   <li>{@code joxette.write.channel.depth} — current depth of the write-channel backpressure buffer</li>
 *   <li>{@code joxette.write.duration}    — DuckDB batch write latency (timer)</li>
 *   <li>{@code joxette.write.batch.size}  — records per batch (distribution summary)</li>
 *   <li>{@code joxette.recorder.restarts} — per-topic recorder restart counter</li>
 * </ul>
 *
 * <h2>Kafka client metrics (bridged from KafkaMetric)</h2>
 * Consumer: records-consumed-rate, bytes-consumed-rate, records-lag-max,
 *           fetch-latency-avg, commit-latency-avg, join-rate, incoming-byte-rate
 * Producer: record-send-rate, byte-rate, record-error-rate, request-latency-avg,
 *           batch-size-avg, record-queue-time-avg
 *
 * <h2>Compaction / retention</h2>
 * <ul>
 *   <li>{@code joxette.compaction.files.processed} — cumulative input files merged</li>
 *   <li>{@code joxette.compaction.files.created}   — cumulative output files written</li>
 *   <li>{@code joxette.compaction.duration}        — compaction run duration (timer)</li>
 *   <li>{@code joxette.retention.rows.deleted}     — rows deleted, tagged by table_type</li>
 *   <li>{@code joxette.retention.duration}         — retention run duration (timer)</li>
 * </ul>
 *
 * <h2>Replay</h2>
 * <ul>
 *   <li>{@code joxette.replay.records.sent}  — cumulative records sent per source_type</li>
 *   <li>{@code joxette.replay.active}        — current active replay count (gauge)</li>
 *   <li>{@code joxette.replay.duration}      — completed replay duration (timer)</li>
 * </ul>
 *
 * <h2>Catalog</h2>
 * <ul>
 *   <li>{@code joxette.catalog.size.bytes}   — DuckLake catalog file size (gauge)</li>
 *   <li>{@code joxette.catalog.inlined.bytes} — estimated inlined data not yet in Parquet (gauge)</li>
 * </ul>
 */
@Component
public class JoxetteMetrics {

    private static final Logger log = LoggerFactory.getLogger(JoxetteMetrics.class);

    // Kafka consumer metrics we bridge into Micrometer
    private static final Set<String> CONSUMER_METRIC_NAMES = Set.of(
            "records-consumed-rate",
            "bytes-consumed-rate",
            "records-lag-max",
            "fetch-latency-avg",
            "commit-latency-avg",
            "join-rate",
            "incoming-byte-rate"
    );

    // Kafka producer metrics we bridge into Micrometer
    private static final Set<String> PRODUCER_METRIC_NAMES = Set.of(
            "record-send-rate",
            "byte-rate",
            "record-error-rate",
            "request-latency-avg",
            "batch-size-avg",
            "record-queue-time-avg"
    );

    private final MeterRegistry registry;

    // Track registered gauge IDs to avoid duplicate registration on recorder restart
    private final Set<String> registeredGaugeIds = ConcurrentHashMap.newKeySet();

    public JoxetteMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    // =========================================================================
    // Recording pipeline
    // =========================================================================

    /**
     * Registers per-topic recording counters and returns a handle used by
     * {@link com.joxette.recording.TopicRecorder} to update them.
     * Safe to call multiple times for the same topic (idempotent via Micrometer registry).
     */
    public RecordingMetrics recordingMetrics(String topic) {
        Counter consumed = Counter.builder("joxette.messages.consumed")
                .description("Cumulative Kafka messages consumed")
                .tag("topic", topic)
                .register(registry);

        Counter written = Counter.builder("joxette.messages.written")
                .description("Cumulative messages written to DuckLake")
                .tag("topic", topic)
                .register(registry);

        Counter restarts = Counter.builder("joxette.recorder.restarts")
                .description("TopicRecorder restart count due to failure")
                .tag("topic", topic)
                .register(registry);

        Timer writeDuration = Timer.builder("joxette.write.duration")
                .description("DuckDB batch write latency")
                .tag("topic", topic)
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        DistributionSummary batchSize = DistributionSummary.builder("joxette.write.batch.size")
                .description("Records per DuckDB batch")
                .tag("topic", topic)
                .publishPercentiles(0.5, 0.95)
                .register(registry);

        return new RecordingMetrics(consumed, written, restarts, writeDuration, batchSize);
    }

    /**
     * Registers a gauge for consumer lag. The gauge reads from the supplied
     * {@code AtomicLong} which is updated by the recorder on each poll cycle.
     * Safe to call multiple times — the gauge is only registered once per topic.
     */
    public void registerLagGauge(String topic, AtomicLong lagHolder) {
        String id = "lag:" + topic;
        if (registeredGaugeIds.add(id)) {
            Gauge.builder("joxette.consumer.lag", lagHolder, AtomicLong::get)
                    .description("Kafka consumer lag (sum across assigned partitions)")
                    .tag("topic", topic)
                    .register(registry);
        }
    }

    /**
     * Registers a gauge for the write-channel backpressure buffer depth.
     * Called once at startup by {@link com.joxette.recording.DuckLakeWriteChannel}.
     */
    public void registerWriteChannelDepthGauge(Supplier<Integer> depthSupplier) {
        Gauge.builder("joxette.write.channel.depth", depthSupplier, Supplier::get)
                .description("Current depth of the DuckDB write-channel backpressure buffer")
                .register(registry);
    }

    // =========================================================================
    // Kafka client metric bridging
    // =========================================================================

    /**
     * Bridges relevant Kafka consumer {@link KafkaMetric} instances into Micrometer gauges.
     * Each gauge holds a weak reference to the metric so it deregisters automatically
     * when the consumer is closed.
     *
     * @param consumerMetrics the map returned by {@code KafkaConsumer.metrics()}
     * @param topic           the topic this consumer is recording
     */
    public void bindKafkaConsumerMetrics(Map<MetricName, ? extends Metric> consumerMetrics,
                                         String topic) {
        for (var entry : consumerMetrics.entrySet()) {
            MetricName name = entry.getKey();
            if (!CONSUMER_METRIC_NAMES.contains(name.name())) continue;

            Metric kafkaMetric = entry.getValue();
            String metricKey = "consumer:" + topic + ":" + name.name();
            if (!registeredGaugeIds.add(metricKey)) continue;  // already registered

            String meterName = "joxette.kafka.consumer." + name.name().replace('-', '.');
            try {
                Gauge.builder(meterName, kafkaMetric,
                              m -> {
                                  Object val = m.metricValue();
                                  return val instanceof Number ? ((Number) val).doubleValue() : Double.NaN;
                              })
                        .description(name.description())
                        .tag("topic", topic)
                        .register(registry);
            } catch (Exception e) {
                log.debug("Could not register Kafka consumer metric '{}' for topic '{}': {}",
                        name.name(), topic, e.getMessage());
            }
        }
        log.debug("Bound Kafka consumer metrics for topic '{}'", topic);
    }

    /**
     * Bridges relevant Kafka producer {@link KafkaMetric} instances into Micrometer gauges.
     *
     * @param producerMetrics the map returned by {@code KafkaProducer.metrics()}
     * @param brokerId        broker id tag value
     */
    public void bindKafkaProducerMetrics(Map<MetricName, ? extends Metric> producerMetrics,
                                          String brokerId) {
        for (var entry : producerMetrics.entrySet()) {
            MetricName name = entry.getKey();
            if (!PRODUCER_METRIC_NAMES.contains(name.name())) continue;

            Metric kafkaMetric = entry.getValue();
            String metricKey = "producer:" + brokerId + ":" + name.name();
            if (!registeredGaugeIds.add(metricKey)) continue;

            String meterName = "joxette.kafka.producer." + name.name().replace('-', '.');
            try {
                Gauge.builder(meterName, kafkaMetric,
                              m -> {
                                  Object val = m.metricValue();
                                  return val instanceof Number ? ((Number) val).doubleValue() : Double.NaN;
                              })
                        .description(name.description())
                        .tag("broker", brokerId)
                        .register(registry);
            } catch (Exception e) {
                log.debug("Could not register Kafka producer metric '{}' for broker '{}': {}",
                        name.name(), brokerId, e.getMessage());
            }
        }
        log.debug("Bound Kafka producer metrics for broker '{}'", brokerId);
    }

    // =========================================================================
    // Compaction / retention
    // =========================================================================

    public Counter compactionFilesProcessed() {
        return Counter.builder("joxette.compaction.files.processed")
                .description("Cumulative input files merged by compaction")
                .register(registry);
    }

    public Counter compactionFilesCreated() {
        return Counter.builder("joxette.compaction.files.created")
                .description("Cumulative output files written by compaction")
                .register(registry);
    }

    public Timer compactionDuration() {
        return Timer.builder("joxette.compaction.duration")
                .description("Compaction run duration")
                .publishPercentiles(0.5, 0.95)
                .register(registry);
    }

    public Counter retentionRowsDeleted(String tableType) {
        return Counter.builder("joxette.retention.rows.deleted")
                .description("Rows deleted by retention enforcement")
                .tag("table_type", tableType)
                .register(registry);
    }

    public Timer retentionDuration() {
        return Timer.builder("joxette.retention.duration")
                .description("Retention enforcement run duration")
                .publishPercentiles(0.5, 0.95)
                .register(registry);
    }

    // =========================================================================
    // Replay
    // =========================================================================

    public Counter replayRecordsSent(String sourceType) {
        return Counter.builder("joxette.replay.records.sent")
                .description("Cumulative records sent during replay-to-topic")
                .tag("source_type", sourceType)   // "topic" or "entity"
                .register(registry);
    }

    public void registerActiveReplaysGauge(Supplier<Integer> supplier) {
        if (registeredGaugeIds.add("replay:active")) {
            Gauge.builder("joxette.replay.active", supplier, Supplier::get)
                    .description("Number of currently running replay-to-topic operations")
                    .register(registry);
        }
    }

    public Timer replayDuration(String sourceType) {
        return Timer.builder("joxette.replay.duration")
                .description("Replay-to-topic run duration")
                .tag("source_type", sourceType)
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
    }

    // =========================================================================
    // Catalog sizes
    // =========================================================================

    public void registerCatalogSizeGauge(Supplier<Long> bytesSupplier) {
        if (registeredGaugeIds.add("catalog:size")) {
            Gauge.builder("joxette.catalog.size.bytes", bytesSupplier, Supplier::get)
                    .description("DuckLake catalog file size in bytes")
                    .baseUnit("bytes")
                    .register(registry);
        }
    }

    public void registerInlinedDataGauge(Supplier<Long> bytesSupplier) {
        if (registeredGaugeIds.add("catalog:inlined")) {
            Gauge.builder("joxette.catalog.inlined.bytes", bytesSupplier, Supplier::get)
                    .description("Estimated bytes of data inlined in the catalog (not yet flushed to Parquet)")
                    .baseUnit("bytes")
                    .register(registry);
        }
    }

    // =========================================================================
    // Inner record
    // =========================================================================

    /** Handles returned by {@link #recordingMetrics(String)} for use in hot paths. */
    public record RecordingMetrics(
            Counter messagesConsumed,
            Counter messagesWritten,
            Counter restarts,
            Timer writeDuration,
            DistributionSummary batchSize
    ) {}
}
