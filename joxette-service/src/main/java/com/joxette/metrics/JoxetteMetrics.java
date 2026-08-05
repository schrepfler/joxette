package com.joxette.metrics;

import io.micrometer.core.instrument.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Central Micrometer instrumentation for Joxette.
 *
 * <h2>Recording metrics</h2>
 * <ul>
 *   <li>{@code joxette.messages.consumed}          — cumulative messages consumed per topic</li>
 *   <li>{@code joxette.messages.written}            — cumulative messages written to DuckLake per topic</li>
 *   <li>{@code joxette.consumer.lag}                — fetch-position lag per topic (gauge, zero network cost)</li>
 *   <li>{@code joxette.consumer.committed_lag}      — committed-offset lag per topic (gauge, updated on health-cache refresh every 15 s)</li>
 *   <li>{@code joxette.write.channel.depth} — current depth of the write-channel backpressure buffer</li>
 *   <li>{@code joxette.write.duration}    — DuckDB batch write latency (timer)</li>
 *   <li>{@code joxette.write.batch.size}  — records per batch (distribution summary)</li>
 *   <li>{@code joxette.recorder.restarts} — per-topic recorder restart counter</li>
 *   <li>{@code joxette.sink.state}        — {@code DuckLakeWriteChannel} sink health (gauge):
 *       0=HEALTHY, 1=DEGRADED, 2=FAILED. See {@code docs/write-resilience.md}. Micrometer
 *       gauge tags are fixed at registration time, not evaluated per-scrape, so the state
 *       name is not exposed as a dynamic label — only the numeric encoding is; the
 *       human-readable name remains available via {@code /health}.</li>
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

    // Kafka consumer metrics we bridge into Micrometer.
    // Network-diagnostic additions:
    //   fetch-latency-avg / fetch-latency-max — round-trip time per FETCH request
    //   fetch-throttle-time-avg               — broker-side throttle; non-zero → quota hit
    //   network-io-rate                       — I/O ops/s; plateau → NIC or TCP bottleneck
    //   outgoing-byte-rate                    — request bytes/s sent to broker
    //   request-latency-avg                   — full request RTT including broker processing
    private static final Set<String> CONSUMER_METRIC_NAMES = Set.of(
            "records-consumed-rate",
            "bytes-consumed-rate",
            "records-lag-max",
            "fetch-latency-avg",
            "fetch-latency-max",
            "fetch-throttle-time-avg",
            "commit-latency-avg",
            "join-rate",
            "incoming-byte-rate",
            "outgoing-byte-rate",
            "network-io-rate",
            "request-latency-avg"
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

    // Gauge.builder(name, stateObject, fn) holds only a WEAK reference to stateObject so
    // metrics don't artificially keep large objects alive. Every gauge below is backed by
    // a bare Supplier lambda / method reference with no other strong reference anywhere —
    // without retaining it here, the JVM collects it shortly after registration and the
    // gauge silently reports NaN forever after. (AtomicLong- and KafkaMetric-backed gauges
    // don't need this: those state objects are already held strongly by their owning
    // recorder, or by the Kafka client's own internal metrics registry.)
    private final List<Object> retainedGaugeState = new CopyOnWriteArrayList<>();

    public JoxetteMetrics(MeterRegistry registry) {
        this.registry = registry;
        Gauge.builder("joxette.jvm.heap.max.bytes", Runtime.getRuntime(), Runtime::maxMemory)
                .description("JVM max heap size as configured by -Xmx (Runtime.maxMemory())")
                .baseUnit("bytes")
                .register(registry);
    }

    // =========================================================================
    // Recording pipeline
    // =========================================================================

    /**
     * Registers recording counters tagged by topic (and optionally partition).
     * When {@code partition} is non-null a {@code partition} tag is added so
     * per-partition metrics are visible alongside the topic-level rollup.
     */
    public RecordingMetrics recordingMetrics(String topic, Integer partition) {
        String partTag = partition != null ? String.valueOf(partition) : null;

        Counter.Builder cb = Counter.builder("joxette.messages.consumed").description("Cumulative Kafka messages consumed").tag("topic", topic);
        if (partTag != null) cb.tag("partition", partTag);
        Counter consumed = cb.register(registry);

        Counter.Builder wb = Counter.builder("joxette.messages.written").description("Cumulative messages written to DuckLake").tag("topic", topic);
        if (partTag != null) wb.tag("partition", partTag);
        Counter written = wb.register(registry);

        Counter.Builder rb = Counter.builder("joxette.recorder.restarts").description("TopicRecorder restart count due to failure").tag("topic", topic);
        if (partTag != null) rb.tag("partition", partTag);
        Counter restarts = rb.register(registry);

        Timer.Builder tb = Timer.builder("joxette.write.duration").description("DuckDB batch write latency").publishPercentiles(0.5, 0.95, 0.99).tag("topic", topic);
        if (partTag != null) tb.tag("partition", partTag);
        Timer writeDuration = tb.register(registry);

        DistributionSummary.Builder sb = DistributionSummary.builder("joxette.write.batch.size").description("Records per DuckDB batch").publishPercentiles(0.5, 0.95).tag("topic", topic);
        if (partTag != null) sb.tag("partition", partTag);
        DistributionSummary batchSize = sb.register(registry);

        // poll-duration: time from kc.poll() call to return, including broker wait.
        // Near POLL_TIMEOUT (100ms) on every call → broker not sending data fast enough.
        // Near 0ms with full batches → local pipeline is the constraint.
        Timer.Builder pb = Timer.builder("joxette.poll.duration").description("kc.poll() wall-clock time per call").publishPercentiles(0.5, 0.95, 0.99).tag("topic", topic);
        if (partTag != null) pb.tag("partition", partTag);
        Timer pollDuration = pb.register(registry);

        return new RecordingMetrics(consumed, written, restarts, writeDuration, batchSize, pollDuration);
    }

    /** Convenience overload for whole-topic (non-partitioned) recorders. */
    public RecordingMetrics recordingMetrics(String topic) {
        return recordingMetrics(topic, null);
    }

    /**
     * Registers a gauge for consumer lag tagged by topic and optional partition.
     */
    public void registerLagGauge(String topic, Integer partition, AtomicLong lagHolder) {
        String id = "lag:" + topic + (partition != null ? ":" + partition : "");
        if (registeredGaugeIds.add(id)) {
            var builder = Gauge.builder("joxette.consumer.lag", lagHolder, AtomicLong::get)
                    .description("Kafka consumer lag")
                    .tag("topic", topic);
            if (partition != null) builder.tag("partition", String.valueOf(partition));
            builder.register(registry);
        }
    }

    /** Convenience overload for whole-topic lag gauges. */
    public void registerLagGauge(String topic, AtomicLong lagHolder) {
        registerLagGauge(topic, null, lagHolder);
    }

    /**
     * Registers a gauge for committed-offset lag (endOffset − committedOffset),
     * updated whenever the health endpoint refreshes its AdminClient-based lag cache.
     * Unlike {@link #registerLagGauge}, this reflects what Kafka would re-deliver after
     * a crash — the number visible in tools like Redpanda Console.
     */
    public void registerCommittedLagGauge(String topic, AtomicLong lagHolder) {
        String id = "committed-lag:" + topic;
        if (registeredGaugeIds.add(id)) {
            Gauge.builder("joxette.consumer.committed_lag", lagHolder, AtomicLong::get)
                    .description("Kafka committed-offset consumer lag (endOffset − committedOffset)")
                    .tag("topic", topic)
                    .register(registry);
        }
    }

    /**
     * Registers a gauge for the write-channel backpressure buffer depth.
     * Called once at startup by {@link com.joxette.recording.DuckLakeWriteChannel}.
     */
    public void registerWriteChannelDepthGauge(Supplier<Integer> depthSupplier) {
        if (registeredGaugeIds.add("write-channel:depth")) {
            retainedGaugeState.add(depthSupplier);
            Gauge.builder("joxette.write.channel.depth", depthSupplier, Supplier::get)
                    .description("Current depth of the DuckDB write-channel backpressure buffer")
                    .register(registry);
        }
    }

    /**
     * Registers a gauge for the {@code DuckLakeWriteChannel} sink health state.
     * Called once at startup by {@link com.joxette.recording.DuckLakeWriteChannel}, which
     * maps its package-private {@code SinkState} enum to the numeric encoding itself
     * (0=HEALTHY, 1=DEGRADED, 2=FAILED) before handing over the supplier — that enum isn't
     * visible outside {@code com.joxette.recording}, and doesn't need to be.
     */
    public void registerSinkStateGauge(Supplier<Integer> stateOrdinalSupplier) {
        if (registeredGaugeIds.add("sink:state")) {
            retainedGaugeState.add(stateOrdinalSupplier);
            Gauge.builder("joxette.sink.state", stateOrdinalSupplier, Supplier::get)
                    .description("DuckLakeWriteChannel sink health: 0=HEALTHY, 1=DEGRADED, 2=FAILED")
                    .register(registry);
        }
    }

    /**
     * Counter incremented when {@link com.joxette.recording.DuckLakeWriteChannel}
     * gives up retrying a deterministically-failing (non-retryable) batch and
     * commits past it so the topic can keep making progress. See
     * {@code joxette.threading.quarantine-after-attempts}.
     */
    public Counter batchesQuarantined(String topic) {
        return Counter.builder("joxette.recording.batches_quarantined")
                .description("Batches permanently skipped after exceeding the non-retryable write-failure quarantine threshold")
                .tag("topic", topic)
                .register(registry);
    }

    /**
     * Counter incremented alongside {@link #batchesQuarantined(String)}, by the
     * number of records actually dropped, not just batches. A single quarantined
     * batch can carry up to {@code batchSize * 4} records (see
     * {@code Flow.batchWeighted} coalescing in {@code TopicRecorder}), so
     * {@code batches_quarantined} alone under-represents the blast radius of
     * data loss — this is the counter to alert on.
     */
    public Counter recordsQuarantined(String topic) {
        return Counter.builder("joxette.recording.records_quarantined")
                .description("Records permanently dropped by quarantined batches after exceeding the non-retryable write-failure threshold")
                .tag("topic", topic)
                .register(registry);
    }

    /**
     * Counter incremented when {@link com.joxette.replay.EntityIdExtractor}
     * throws while extracting an entity id (malformed JSON, JsonPath evaluation
     * error) — excludes the normal "no id present" outcome, which is not an
     * error and must not inflate this counter.
     *
     * <p>Counts per matcher-attempt, not per message: a topic with 3 entity
     * source matchers evaluated against one malformed message increments this
     * counter 3 times, once per matcher that threw.
     */
    public Counter entityExtractionFailures(String topic, String entityType) {
        return Counter.builder("joxette.recording.entity_extraction_failures")
                .description("Entity-id extraction attempts that threw, tagged by topic and entity type")
                .tag("topic", topic)
                .tag("entity_type", entityType)
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
                                         String topic, Integer partition) {
        for (var entry : consumerMetrics.entrySet()) {
            MetricName name = entry.getKey();
            if (!CONSUMER_METRIC_NAMES.contains(name.name())) continue;

            Metric kafkaMetric = entry.getValue();
            String metricKey = "consumer:" + topic + (partition != null ? ":" + partition : "") + ":" + name.name();
            if (!registeredGaugeIds.add(metricKey)) continue;

            String meterName = "joxette.kafka.consumer." + name.name().replace('-', '.');
            try {
                var b = Gauge.builder(meterName, kafkaMetric,
                              m -> {
                                  Object val = m.metricValue();
                                  return val instanceof Number ? ((Number) val).doubleValue() : Double.NaN;
                              })
                        .description(name.description())
                        .tag("topic", topic);
                if (partition != null) b.tag("partition", String.valueOf(partition));
                b.register(registry);
            } catch (Exception e) {
                log.debug("Could not register Kafka consumer metric '{}' for topic '{}': {}",
                        name.name(), topic, e.getMessage());
            }
        }
    }

    /** Convenience overload for whole-topic consumer metrics. */
    public void bindKafkaConsumerMetrics(Map<MetricName, ? extends Metric> consumerMetrics, String topic) {
        bindKafkaConsumerMetrics(consumerMetrics, topic, null);
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
            retainedGaugeState.add(supplier);
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
            retainedGaugeState.add(bytesSupplier);
            Gauge.builder("joxette.catalog.size.bytes", bytesSupplier,
                          s -> { try { Long v = s.get(); return v != null ? v.doubleValue() : 0.0; } catch (Exception e) { return 0.0; } })
                    .description("DuckLake catalog file size in bytes")
                    .baseUnit("bytes")
                    .register(registry);
        }
    }

    public void registerInlinedDataGauge(Supplier<Long> bytesSupplier) {
        if (registeredGaugeIds.add("catalog:inlined")) {
            retainedGaugeState.add(bytesSupplier);
            Gauge.builder("joxette.catalog.inlined.bytes", bytesSupplier,
                          s -> { try { Long v = s.get(); return v != null ? v.doubleValue() : 0.0; } catch (Exception e) { return 0.0; } })
                    .description("Estimated bytes of data inlined in the catalog (not yet flushed to Parquet)")
                    .baseUnit("bytes")
                    .register(registry);
        }
    }

    // =========================================================================
    // Process RSS
    // =========================================================================

    /**
     * Registers a gauge for process resident set size (RSS) in bytes.
     * Useful for detecting native/off-heap memory growth from DuckDB's C++ allocator
     * that does not appear in JVM heap metrics.
     */
    public void registerProcessRssGauge(Supplier<Long> rssSupplier) {
        if (registeredGaugeIds.add("process:rss")) {
            retainedGaugeState.add(rssSupplier);
            Gauge.builder("joxette.process.rss.bytes", rssSupplier,
                          s -> { try { Long v = s.get(); return v != null && v >= 0 ? v.doubleValue() : 0.0; } catch (Exception e) { return 0.0; } })
                    .description("Process resident set size (RSS) in bytes — includes JVM heap, metaspace, and DuckDB native allocations")
                    .baseUnit("bytes")
                    .register(registry);
        }
    }

    // =========================================================================
    // DuckDB memory (duckdb_memory() per-tag gauges)
    // =========================================================================

    /**
     * Registers per-tag memory gauges from {@code duckdb_memory()} and a
     * total gauge summing all tags. Each gauge is backed by a supplier that
     * runs the query on every scrape, so values stay current.
     *
     * <p>The supplier itself synchronizes on the shared DuckDB connection per
     * query (see {@code HealthController.duckDbMemoryByTag()}); this method
     * just wires the gauges up.
     *
     * @param memorySupplier returns {@code Map<tag, memory_usage_bytes>}
     * @param tags           the full set of tags returned by {@code duckdb_memory()}
     */
    public void registerDuckDbMemoryGauges(Supplier<java.util.Map<String, Long>> memorySupplier,
                                           java.util.Set<String> tags) {
        retainedGaugeState.add(memorySupplier);
        // Per-tag gauges — state object is the Supplier; ToDoubleFunction calls .get() then looks up tag
        for (String tag : tags) {
            String gaugeId = "duckdb:mem:" + tag;
            if (registeredGaugeIds.add(gaugeId)) {
                String metricTag = tag.toLowerCase();
                Gauge.builder("joxette.duckdb.memory.bytes",
                              memorySupplier,
                              s -> {
                                  java.util.Map<String, Long> m = s.get();
                                  Long v = m.get(tag);
                                  return v != null ? v.doubleValue() : 0.0;
                              })
                        .description("DuckDB memory usage in bytes for allocation tag " + tag)
                        .tag("tag", metricTag)
                        .baseUnit("bytes")
                        .register(registry);
            }
        }
        // Total across all tags
        if (registeredGaugeIds.add("duckdb:mem:total")) {
            Gauge.builder("joxette.duckdb.memory.total.bytes",
                          memorySupplier,
                          s -> { try { return s.get().values().stream().mapToLong(Long::longValue).sum(); } catch (Exception e) { return 0.0; } })
                    .description("Total DuckDB memory usage in bytes across all allocation tags")
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
            DistributionSummary batchSize,
            Timer pollDuration
    ) {}
}
