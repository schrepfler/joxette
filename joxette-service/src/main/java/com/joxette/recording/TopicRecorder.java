package com.joxette.recording;

import com.joxette.metrics.JoxetteMetrics;
import com.softwaremill.jox.kafka.ConsumerSettings;
import com.joxette.replay.EntityRoute;
import com.joxette.replay.GeneralRoute;
import com.joxette.replay.KafkaMessage;
import com.joxette.replay.KnownEntitiesRepository;
import com.joxette.replay.MessageRouter;
import com.joxette.replay.RouteDecision;
import com.softwaremill.jox.flows.Flows;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Records a single Kafka topic to the DuckLake cassette tables.
 *
 * <h2>Pipeline design</h2>
 * <ol>
 *   <li>A {@link KafkaConsumer} drives the Kafka consumer, emitting one {@link ConsumerRecord}
 *       at a time via a Jox {@code Flow} built with {@code Flows.usingEmit}.</li>
 *   <li>{@code groupedWithin} accumulates records into bounded batches.</li>
 *   <li>For each batch, each record is routed via {@link MessageRouter}.</li>
 *   <li>A {@link WriteBatch} is submitted to {@link DuckLakeWriteChannel}, which serializes
 *       all DuckDB writes through a single virtual thread. The submit call blocks until the
 *       write completes, propagating backpressure into the flow pipeline.</li>
 *   <li>After a successful write, entity routes are upserted into {@code known_entities}
 *       and Kafka offsets are committed synchronously on the next poll cycle.</li>
 * </ol>
 *
 * <h2>Rebalance handling</h2>
 * <p>The {@link ConsumerRebalanceListener} is protocol-aware:
 * <ul>
 *   <li><b>KIP-848 ({@code group.protocol=consumer})</b>: {@code onPartitionsRevoked} fires with
 *       only the specifically revoked partitions.  Only in-flight batches that overlap the revoked
 *       set are drained; non-revoked partitions continue without a pause.</li>
 *   <li><b>Classic protocol</b>: {@code onPartitionsRevoked} may receive all assigned partitions.
 *       All in-flight batches for the topic are drained before returning.</li>
 * </ul>
 *
 * <h2>Graceful shutdown</h2>
 * <p>On {@link #stop()}, consumption is paused first, then all in-flight batches are drained,
 * then pending offsets are committed, and finally the consumer is closed.  This ensures no
 * in-flight writes are lost during K8s rolling restarts.
 */
public class TopicRecorder {

    private static final Logger log = LoggerFactory.getLogger(TopicRecorder.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    private final String topic;
    private final ConsumerSettings<String, byte[]> settings;
    private final DuckLakeWriteChannel writeChannel;
    private final int batchSize;
    private final Duration batchTimeout;
    private final MessageRouter router;
    private final KnownEntitiesRepository knownEntities;
    private final boolean seekToEarliest;
    private final Instant seekToTimestamp;
    private final JoxetteMetrics.RecordingMetrics meters;

    /** The live consumer — set during {@link #run()}, cleared on exit. */
    private volatile KafkaConsumer<String, byte[]> consumer;

    /** Currently assigned partitions (updated inside the rebalance listener). */
    private final Set<TopicPartition> assignedPartitions = ConcurrentHashMap.newKeySet();

    /** Pending offsets to commit on the next poll iteration (thread-safe). */
    private final AtomicReference<Map<TopicPartition, OffsetAndMetadata>> pendingCommit =
            new AtomicReference<>();

    private volatile boolean stopped = false;

    private volatile Instant lastBatchAt;
    private final AtomicLong consumerLag = new AtomicLong(-1);
    private final AtomicLong messagesConsumed = new AtomicLong(0);
    private final AtomicLong messagesWritten  = new AtomicLong(0);

    /** Negotiated Kafka group protocol — set when the first rebalance fires. */
    private volatile String negotiatedProtocol = "unknown";

    /** Kept for lambda capture in {@link #run()} — passed to bindKafkaConsumerMetrics. */
    private final JoxetteMetrics joxetteMetrics;

    public TopicRecorder(
            String topic,
            ConsumerSettings<String, byte[]> baseSettings,
            DuckLakeWriteChannel writeChannel,
            int batchSize,
            long batchTimeoutMs,
            MessageRouter router,
            KnownEntitiesRepository knownEntities,
            String startFrom,
            JoxetteMetrics joxetteMetrics) {
        this.topic          = topic;
        this.writeChannel   = writeChannel;
        this.joxetteMetrics = joxetteMetrics;
        this.meters         = joxetteMetrics.recordingMetrics(topic);
        joxetteMetrics.registerLagGauge(topic, consumerLag);
        this.batchSize    = batchSize;
        this.batchTimeout = Duration.ofMillis(batchTimeoutMs);
        this.router       = router;
        this.knownEntities = knownEntities;
        this.seekToEarliest = "earliest".equals(startFrom);

        Instant ts = null;
        if (!seekToEarliest && startFrom != null && !"latest".equals(startFrom) && !startFrom.isBlank()) {
            try {
                ts = Instant.parse(startFrom);
            } catch (DateTimeParseException e) {
                log.warn("Unrecognised startFrom='{}' for topic '{}'; defaulting to latest", startFrom, topic);
            }
        }
        this.seekToTimestamp = ts;

        ConsumerSettings<String, byte[]> s = baseSettings.groupId(baseSettings.groupId() + "-" + topic);
        if (seekToEarliest) {
            s = s.autoOffsetReset(ConsumerSettings.AutoOffsetReset.EARLIEST);
        }
        this.settings = s;
    }

    // -----------------------------------------------------------------------
    // Pipeline
    // -----------------------------------------------------------------------

    public void run() throws Exception {
        log.info("Starting recorder for topic '{}'", topic);

        try {
            Flows.<ConsumerRecord<String, byte[]>>usingEmit(emit -> {
                // Explicit try-finally instead of try-with-resources so we can pass a
                // close timeout. The default kc.close() waits up to 30 s for a
                // leave-group handshake — that blocks the recorder VT and keeps Kafka's
                // non-daemon background threads alive, preventing JVM exit on shutdown.
                KafkaConsumer<String, byte[]> kc = settings.toConsumer();
                try {
                    this.consumer = kc;
                    // Bridge Kafka client metrics into Micrometer on first consumer creation
                    joxetteMetrics.bindKafkaConsumerMetrics(kc.metrics(), topic);
                    kc.subscribe(List.of(topic), new PartitionAwareRebalanceListener(kc));

                    while (!stopped && !Thread.currentThread().isInterrupted()) {
                        Map<TopicPartition, OffsetAndMetadata> toCommit = pendingCommit.getAndSet(null);
                        if (toCommit != null) {
                            kc.commitSync(toCommit);
                        }
                        try {
                            var records = kc.poll(POLL_TIMEOUT);
                            updateLag(kc);
                            if (records.isEmpty() && kc.assignment().isEmpty()) {
                                // No partitions assigned yet (Kafka unreachable or rebalancing).
                                // Sleep briefly so the poll loop does not spin at CPU speed while
                                // the metadata background thread is retrying.
                                Thread.sleep(POLL_TIMEOUT.toMillis());
                            }
                            for (ConsumerRecord<String, byte[]> record : records) {
                                emit.apply(record);
                            }
                        } catch (WakeupException e) {
                            log.debug("Kafka wakeup received for topic '{}'; stopping poll loop", topic);
                            break;
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }

                    // Graceful shutdown: drain in-flight batches, commit, then close.
                    writeChannel.awaitDrain(null);
                    Map<TopicPartition, OffsetAndMetadata> finalCommit = pendingCommit.getAndSet(null);
                    if (finalCommit != null && !finalCommit.isEmpty()) {
                        try {
                            kc.commitSync(finalCommit);
                        } catch (Exception e) {
                            log.warn("Final offset commit failed for topic '{}': {}", topic, e.getMessage());
                        }
                    }
                } finally {
                    this.consumer = null;
                    assignedPartitions.clear();
                    kc.close(Duration.ofSeconds(5));
                }
            })
            .groupedWithin(batchSize, batchTimeout)
            .runForeach(batch -> {
                writeBatch(batch);
                pendingCommit.set(buildOffsets(batch));
            });

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Recorder for topic '{}' interrupted; treating as clean stop", topic);
        } catch (Exception e) {
            log.error("Recorder for topic '{}' terminated with error", topic, e);
            throw e;
        } finally {
            log.info("Recorder for topic '{}' stopped", topic);
        }
    }

    public void stop() {
        stopped = true;
        KafkaConsumer<String, byte[]> c = consumer;
        if (c != null) c.wakeup();
    }

    public boolean isStopped() { return stopped; }

    public Instant lastBatchAt() { return lastBatchAt; }

    public long consumerLag() { return consumerLag.get(); }

    /** Currently assigned partition numbers; empty when the consumer is not running. */
    public Set<Integer> assignedPartitionIds() {
        Set<Integer> ids = ConcurrentHashMap.newKeySet();
        for (TopicPartition tp : assignedPartitions) ids.add(tp.partition());
        return Collections.unmodifiableSet(ids);
    }

    /** Negotiated Kafka group protocol ({@code consumer} or {@code classic}). */
    public String negotiatedProtocol() { return negotiatedProtocol; }

    public long messagesConsumed() { return messagesConsumed.get(); }
    public long messagesWritten()  { return messagesWritten.get(); }

    // -----------------------------------------------------------------------
    // Batch write
    // -----------------------------------------------------------------------

    /**
     * Routes each record, builds a {@link WriteBatch}, submits it to the
     * {@link DuckLakeWriteChannel}, and upserts the discovered entities into
     * {@code known_entities} after a successful write.
     */
    private void writeBatch(List<ConsumerRecord<String, byte[]>> batch) throws Exception {
        log.trace("Processing batch of {} records for topic '{}'", batch.size(), topic);

        List<ConsumerRecord<String, byte[]>> generalRecords = new ArrayList<>();
        List<String> generalMessageTypes = new ArrayList<>();
        List<WriteBatch.EntityWriteItem> entityItems = new ArrayList<>();
        List<EntityRoute> allRoutes = new ArrayList<>();

        for (ConsumerRecord<String, byte[]> record : batch) {
            KafkaMessage msg = toKafkaMessage(record);
            RouteDecision decision = router.route(msg);

            GeneralRoute gr = decision.generalRoute();
            if (gr != null) {
                generalRecords.add(record);
                generalMessageTypes.add(gr.messageType());
            }

            if (!decision.entityRoutes().isEmpty()) {
                entityItems.add(new WriteBatch.EntityWriteItem(decision.entityRoutes(), msg));
                allRoutes.addAll(decision.entityRoutes());
            }
        }

        messagesConsumed.addAndGet(batch.size());
        meters.messagesConsumed().increment(batch.size());
        meters.batchSize().record(batch.size());

        WriteBatch wb = WriteBatch.of(topic, generalRecords, generalMessageTypes, entityItems);
        meters.writeDuration().record(() -> {
            try { writeChannel.submit(wb); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new RuntimeException(e); }
        });
        messagesWritten.addAndGet(batch.size());
        meters.messagesWritten().increment(batch.size());
        lastBatchAt = Instant.now();

        if (!allRoutes.isEmpty()) {
            try {
                knownEntities.upsertBatch(allRoutes, Instant.now());
            } catch (Exception e) {
                log.warn("Failed to upsert known_entities batch: {}", e.getMessage());
                // Non-fatal: entity registry is best-effort; don't abort the batch
            }
        }
    }

    // -----------------------------------------------------------------------
    // Rebalance listener
    // -----------------------------------------------------------------------

    /**
     * Protocol-aware rebalance listener.
     *
     * <p>Under KIP-848 ({@code group.protocol=consumer}), {@code onPartitionsRevoked}
     * fires with only the partitions being taken away.  We drain only the in-flight
     * batches that overlap the revoked set — non-revoked partitions continue without
     * interruption.
     *
     * <p>Under the classic protocol, {@code onPartitionsRevoked} may carry the full
     * assignment.  We drain all in-flight batches for this topic to be safe.
     */
    private class PartitionAwareRebalanceListener implements ConsumerRebalanceListener {

        private final KafkaConsumer<String, byte[]> kc;

        PartitionAwareRebalanceListener(KafkaConsumer<String, byte[]> kc) {
            this.kc = kc;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> revoked) {
            if (revoked.isEmpty()) return;

            // Detect whether we are under KIP-848 (incremental revoke) or classic
            // (full revoke = all currently assigned partitions handed back at once).
            boolean isIncrementalRevoke = !assignedPartitions.isEmpty()
                    && !assignedPartitions.equals(Set.copyOf(revoked));

            if (isIncrementalRevoke) {
                // KIP-848: drain only batches that overlap the revoked partitions.
                log.info("KIP-848 incremental revoke on topic '{}': draining {} partition(s): {}",
                        topic, revoked.size(), revoked);
                writeChannel.awaitDrain(revoked);
            } else {
                // Classic protocol: drain all in-flight batches before returning.
                log.info("Classic full revoke on topic '{}': draining all in-flight batches ({} partition(s))",
                        topic, revoked.size());
                writeChannel.awaitDrain(null);
            }

            // Commit offsets synchronously before releasing partitions.
            Map<TopicPartition, OffsetAndMetadata> toCommit = pendingCommit.getAndSet(null);
            if (toCommit != null) {
                Map<TopicPartition, OffsetAndMetadata> revokedCommit = new HashMap<>();
                for (TopicPartition tp : revoked) {
                    OffsetAndMetadata om = toCommit.get(tp);
                    if (om != null) revokedCommit.put(tp, om);
                }
                if (!revokedCommit.isEmpty()) {
                    try {
                        kc.commitSync(revokedCommit);
                    } catch (Exception e) {
                        log.warn("Offset commit on revoke failed for topic '{}': {}", topic, e.getMessage());
                    }
                }
                // Keep the non-revoked entries for the next poll cycle.
                if (!isIncrementalRevoke) {
                    // All revoked — nothing to retain.
                } else {
                    Map<TopicPartition, OffsetAndMetadata> retained = new HashMap<>(toCommit);
                    revoked.forEach(retained::remove);
                    if (!retained.isEmpty()) pendingCommit.set(retained);
                }
            }

            assignedPartitions.removeAll(revoked);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> assigned) {
            assignedPartitions.addAll(assigned);

            // Detect negotiated protocol by inspecting the metric the Kafka client exposes.
            // The metric is set before the first rebalance fires; we read it lazily here
            // to avoid a dependency on the private metric name at construction time.
            detectNegotiatedProtocol(kc);

            log.info("Partitions assigned on topic '{}': {} (negotiated protocol: {})",
                    topic, assigned, negotiatedProtocol);

            if (seekToEarliest) {
                kc.seekToBeginning(assigned);
                log.info("Seeked to beginning of {} partition(s) for topic '{}' (startFrom=earliest)",
                        assigned.size(), topic);
            } else if (seekToTimestamp != null) {
                seekToTimestamp(kc, assigned, seekToTimestamp);
                log.info("Seeked to timestamp {} on {} partition(s) for topic '{}'",
                        seekToTimestamp, assigned.size(), topic);
            } else {
                // For start_from=latest, explicitly seek uncommitted partitions to end.
                // Partitions that already have a committed offset are left alone so the
                // consumer resumes from where it left off.  This avoids the
                // CommitRequestManager "Found no committed offset" log cycle — explicit
                // positioning bypasses the committed-offset lookup entirely.
                Map<TopicPartition, OffsetAndMetadata> committed = kc.committed(new java.util.HashSet<>(assigned));
                List<TopicPartition> noOffset = assigned.stream()
                        .filter(tp -> committed.get(tp) == null)
                        .toList();
                if (!noOffset.isEmpty()) {
                    kc.seekToEnd(noOffset);
                    log.info("Seeked to end of {} uncommitted partition(s) for topic '{}': {}",
                            noOffset.size(), topic, noOffset);
                }
            }
        }
    }

    /**
     * Reads the negotiated group protocol from the Kafka consumer metric
     * {@code consumer-coordinator-metrics / group-protocol}.
     *
     * <p>The metric is available from Kafka client 3.7+ when {@code group.protocol=consumer}
     * is set.  On older clients or classic-only brokers the metric is absent; in that case
     * we fall back to reading the configured property.
     */
    private void detectNegotiatedProtocol(KafkaConsumer<String, byte[]> kc) {
        try {
            for (var entry : kc.metrics().entrySet()) {
                if ("group-protocol".equals(entry.getKey().name())
                        && "consumer-coordinator-metrics".equals(entry.getKey().group())) {
                    Object val = entry.getValue().metricValue();
                    if (val instanceof String s && !s.isBlank() && !"".equals(s)) {
                        if (!s.equals(negotiatedProtocol)) {
                            negotiatedProtocol = s;
                            log.info("Kafka consumer group protocol for topic '{}': {}", topic, s);
                        }
                        return;
                    }
                }
            }
        } catch (Exception ignored) {
            // Metric lookup is best-effort; never fail a rebalance because of it.
        }
        // Metric not present — fall back to configured value.
        String configured = settings.toProperties().getProperty("group.protocol", "classic");
        if (!configured.equals(negotiatedProtocol)) {
            negotiatedProtocol = configured;
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private void updateLag(KafkaConsumer<String, byte[]> kc) {
        try {
            var endOffsets = kc.endOffsets(kc.assignment());
            long lag = 0;
            for (var entry : endOffsets.entrySet()) {
                var committed = kc.committed(java.util.Set.of(entry.getKey())).get(entry.getKey());
                long position = committed != null ? committed.offset() : kc.position(entry.getKey());
                lag += Math.max(0, entry.getValue() - position);
            }
            consumerLag.set(lag);
        } catch (Exception e) {
            // Non-fatal: lag tracking is best-effort
        }
    }

    private static KafkaMessage toKafkaMessage(ConsumerRecord<String, byte[]> record) {
        List<KafkaMessage.Header> headers = new ArrayList<>();
        for (Header h : record.headers()) {
            headers.add(new KafkaMessage.Header(h.key(), h.value()));
        }
        return new KafkaMessage(
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                record.key(),
                record.value(),
                List.copyOf(headers));
    }

    private static Map<TopicPartition, OffsetAndMetadata> buildOffsets(
            List<ConsumerRecord<String, byte[]>> batch) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (ConsumerRecord<String, byte[]> r : batch) {
            TopicPartition tp = new TopicPartition(r.topic(), r.partition());
            long nextOffset = r.offset() + 1;
            OffsetAndMetadata existing = offsets.get(tp);
            if (existing == null || nextOffset > existing.offset()) {
                offsets.put(tp, new OffsetAndMetadata(nextOffset));
            }
        }
        return offsets;
    }

    private static void seekToTimestamp(
            KafkaConsumer<String, byte[]> kc,
            Collection<TopicPartition> partitions,
            Instant timestamp) {
        long epochMs = timestamp.toEpochMilli();
        Map<TopicPartition, Long> query = new HashMap<>();
        for (TopicPartition tp : partitions) query.put(tp, epochMs);
        var results = kc.offsetsForTimes(query);
        for (TopicPartition tp : partitions) {
            var ot = results.get(tp);
            if (ot != null) {
                kc.seek(tp, ot.offset());
                log.debug("Seeked {} to offset {} (timestamp {})", tp, ot.offset(), timestamp);
            } else {
                kc.seekToEnd(List.of(tp));
                log.debug("No messages at or after {} on {}; seeking to end", timestamp, tp);
            }
        }
    }
}
