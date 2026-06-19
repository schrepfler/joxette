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
import org.apache.kafka.common.errors.RetriableException;
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
 *   <li>A {@link KafkaConsumer} drives the poll loop; each {@code kc.poll()} result is
 *       emitted as one {@code List<ConsumerRecord>} item via a Jox {@code Flow} built
 *       with {@code Flows.usingEmit}.  Emitting the whole poll result atomically prevents
 *       {@code groupedWithin}-style timers from firing after seeing only the first record
 *       of a large poll batch.</li>
 *   <li>{@code batchWeighted} coalesces consecutive poll results when the writer is the
 *       bottleneck, accumulating up to {@code batchSize * 4} records before routing.
 *       When the writer keeps up, each poll result passes through unchanged.</li>
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
    /**
     * When non-null, this recorder uses manual partition assignment ({@code assign()})
     * rather than group subscription ({@code subscribe()}), giving it exclusive
     * ownership of a single partition and allowing independent parallel fetch.
     */
    private final Integer assignedPartition;

    /** The live consumer — set during {@link #run()}, cleared on exit. */
    private volatile KafkaConsumer<String, byte[]> consumer;

    /** Currently assigned partitions (updated inside the rebalance listener). */
    private final Set<TopicPartition> assignedPartitions = ConcurrentHashMap.newKeySet();

    /** Pending offsets to commit on the next poll iteration (thread-safe). */
    private final AtomicReference<Map<TopicPartition, OffsetAndMetadata>> pendingCommit =
            new AtomicReference<>();

    private volatile boolean stopped = false;

    private volatile Instant lastBatchAt;
    private final AtomicLong consumerLag     = new AtomicLong(-1);
    private final AtomicLong messagesConsumed = new AtomicLong(0);
    private final AtomicLong messagesWritten  = new AtomicLong(0);
    /** Nanos when the last buildWriteBatch call completed — for pipeline queue time logging. */
    private volatile long lastIngestNanos = 0;

    /** Negotiated Kafka group protocol — set when the first rebalance fires. */
    private volatile String negotiatedProtocol = "unknown";

    /** Kept for lambda capture in {@link #run()} — passed to bindKafkaConsumerMetrics. */
    private final JoxetteMetrics joxetteMetrics;

    /** Full constructor used internally. */
    private TopicRecorder(
            String topic,
            Integer assignedPartition,
            ConsumerSettings<String, byte[]> baseSettings,
            DuckLakeWriteChannel writeChannel,
            int batchSize,
            long batchTimeoutMs,
            MessageRouter router,
            KnownEntitiesRepository knownEntities,
            String startFrom,
            JoxetteMetrics joxetteMetrics) {
        this.topic             = topic;
        this.assignedPartition = assignedPartition;
        this.writeChannel      = writeChannel;
        this.joxetteMetrics    = joxetteMetrics;
        // Metrics tagged by topic + optional partition so the UI can aggregate by topic
        // or drill down per partition.
        this.meters = joxetteMetrics.recordingMetrics(topic, assignedPartition);
        joxetteMetrics.registerLagGauge(topic, assignedPartition, consumerLag);
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

    /** Creates a recorder that subscribes to the whole topic via consumer group. */
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
        this(topic, null, baseSettings, writeChannel, batchSize, batchTimeoutMs,
             router, knownEntities, startFrom, joxetteMetrics);
    }

    /**
     * Creates a recorder that uses manual {@code assign()} for a single partition.
     * Independent of the consumer group — fetches this partition in isolation,
     * allowing parallel fetch across partitions without group coordination overhead.
     */
    public TopicRecorder(
            String topic,
            int partition,
            ConsumerSettings<String, byte[]> baseSettings,
            DuckLakeWriteChannel writeChannel,
            int batchSize,
            long batchTimeoutMs,
            MessageRouter router,
            KnownEntitiesRepository knownEntities,
            String startFrom,
            JoxetteMetrics joxetteMetrics) {
        this(topic, (Integer) partition, baseSettings, writeChannel, batchSize, batchTimeoutMs,
             router, knownEntities, startFrom, joxetteMetrics);
    }

    // -----------------------------------------------------------------------
    // Pipeline
    // -----------------------------------------------------------------------

    public void run() throws Exception {
        String label = assignedPartition != null ? topic + "[" + assignedPartition + "]" : topic;
        log.info("Starting recorder for '{}'", label);

        KafkaConsumer<String, byte[]> kc = settings.toConsumer();
        try {
            this.consumer = kc;
            joxetteMetrics.bindKafkaConsumerMetrics(kc.metrics(), topic, assignedPartition);

            if (assignedPartition != null) {
                TopicPartition tp = new TopicPartition(topic, assignedPartition);
                kc.assign(List.of(tp));
                assignedPartitions.add(tp);
                positionPartition(kc, tp);
                log.info("Assigned partition {} of '{}'; startFrom={}", assignedPartition, topic,
                        seekToEarliest ? "earliest" : seekToTimestamp != null ? seekToTimestamp.toString() : "latest");
            } else {
                kc.subscribe(List.of(topic), new PartitionAwareRebalanceListener(kc));
            }

            try {
                // Emit one List<ConsumerRecord> per kc.poll() call — never individual records.
                // Emitting the whole poll result atomically prevents batchWeighted from seeing
                // only the first record before it can accumulate more (the old singleton-batch bug).
                Flows.<List<ConsumerRecord<String, byte[]>>>usingEmit(emit -> {
                    outer:
                    while (!stopped && !Thread.currentThread().isInterrupted()) {
                        try {
                            Map<TopicPartition, OffsetAndMetadata> toCommit = pendingCommit.getAndSet(null);
                            if (toCommit != null) {
                                kc.commitSync(toCommit);
                            }
                            long pollStart = System.nanoTime();
                            var records = kc.poll(POLL_TIMEOUT);
                            meters.pollDuration().record(System.nanoTime() - pollStart,
                                    java.util.concurrent.TimeUnit.NANOSECONDS);
                            updateLag(kc);
                            if (records.isEmpty()) {
                                if (kc.assignment().isEmpty()) {
                                    Thread.sleep(POLL_TIMEOUT.toMillis());
                                }
                                continue;
                            }
                            List<ConsumerRecord<String, byte[]>> pollBatch = new ArrayList<>();
                            for (ConsumerRecord<String, byte[]> r : records) {
                                pollBatch.add(r);
                            }
                            emit.apply(pollBatch);
                        } catch (WakeupException e) {
                            // stop() calls kc.wakeup() — wakeup can fire during commitSync
                            // or poll; both are handled here at the loop level.
                            log.debug("Kafka wakeup received for '{}'; stopping poll loop", label);
                            break outer;
                        } catch (RetriableException e) {
                            // Transient broker/network error (e.g. network loss, broker restart,
                            // laptop sleep/wake). The Kafka client will reconnect automatically;
                            // skip the failed poll and retry on the next iteration rather than
                            // propagating the exception and killing the recorder.
                            log.warn("Transient Kafka error on topic '{}', will retry: {} {}",
                                    label, e.getClass().getSimpleName(), e.getMessage());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break outer;
                        }
                    }
                })
                // Coalesce consecutive poll results when the writer is the bottleneck.
                .batchWeighted(
                    (long) batchSize * 4,
                    records -> (long) records.size(),
                    records -> records,
                    (acc, records) -> { acc.addAll(records); return acc; })
                .map(this::buildWriteBatch)
                .runForeach(wb -> {
                    // If the sink is degraded, pause consumption and spin-wait for recovery
                    // before submitting. This keeps the consumer alive (heartbeats via poll)
                    // without committing offsets or tearing down the pipeline.
                    if (!writeChannel.isHealthy()) {
                        pauseForSinkRecovery(kc, label);
                    }
                    submitWriteBatch(wb);
                    pendingCommit.set(buildOffsets(wb.sourceRecords()));
                });
            } finally {
                // runForeach has returned — all buffered poll batches have been written and
                // their offsets are in pendingCommit. Now it is safe to do the final commit
                // and close the consumer. Doing this inside usingEmit's lambda was wrong:
                // batchWeighted's buffer hadn't been flushed yet when the lambda exited.
                Map<TopicPartition, OffsetAndMetadata> finalCommit = pendingCommit.getAndSet(null);
                if (finalCommit != null && !finalCommit.isEmpty()) {
                    try {
                        kc.commitSync(finalCommit);
                    } catch (Exception e) {
                        log.warn("Final offset commit failed for '{}': {}", label, e.getMessage());
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Recorder for topic '{}' interrupted; treating as clean stop", topic);
        } catch (Exception e) {
            log.error("Recorder for topic '{}' terminated with error", topic, e);
            throw e;
        } finally {
            this.consumer = null;
            assignedPartitions.clear();
            kc.close(Duration.ofSeconds(5));
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
     * Routes each Kafka record and builds a {@link WriteBatch} without performing
     * any I/O. Called from the Jox flow before {@code batchWeighted} coalescing.
     */
    private WriteBatch buildWriteBatch(List<ConsumerRecord<String, byte[]>> batch) {
        long nowMs = System.currentTimeMillis();
        long oldestKafkaTs = batch.stream().mapToLong(ConsumerRecord::timestamp).min().orElse(nowMs);
        long lagMs = nowMs - oldestKafkaTs;
        log.info("Ingested {} record(s) for topic '{}' — oldest record lag {}ms ({}s behind)",
                batch.size(), topic, lagMs, lagMs / 1_000);

        List<ConsumerRecord<String, byte[]>> generalRecords = new ArrayList<>();
        List<String> generalMessageTypes = new ArrayList<>();
        List<WriteBatch.EntityWriteItem> entityItems = new ArrayList<>();

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
            }
        }

        messagesConsumed.addAndGet(batch.size());
        meters.messagesConsumed().increment(batch.size());
        meters.batchSize().record(batch.size());
        lastIngestNanos = System.nanoTime();

        return WriteBatch.of(topic, batch, generalRecords, generalMessageTypes, entityItems);
    }

    /**
     * Pauses all assigned partitions and spin-polls until the write sink recovers
     * to HEALTHY. Keeps the consumer alive (heartbeats via empty poll calls) without
     * advancing offsets or committing, so no rebalance is triggered.
     */
    private void pauseForSinkRecovery(KafkaConsumer<String, byte[]> kc, String label) throws InterruptedException {
        Set<TopicPartition> paused = new java.util.HashSet<>(kc.assignment());
        if (!paused.isEmpty()) {
            kc.pause(paused);
            log.warn("Recorder '{}': sink DEGRADED — paused {} partition(s); waiting for recovery", label, paused.size());
        }
        try {
            while (!writeChannel.isHealthy() && !stopped && !Thread.currentThread().isInterrupted()) {
                // Keep polling to send heartbeats — discard records (consumer is paused)
                kc.poll(POLL_TIMEOUT);
                updateLag(kc);
            }
        } finally {
            if (!paused.isEmpty() && !stopped) {
                kc.resume(paused);
                log.info("Recorder '{}': sink recovered — resumed {} partition(s)", label, paused.size());
            }
        }
    }

    /**
     * Submits a (possibly coalesced) {@link WriteBatch} to DuckDB and upserts
     * discovered entity routes into {@code known_entities}.
     */
    private void submitWriteBatch(WriteBatch wb) throws Exception {
        int recordCount = wb.sourceRecords().size();
        int generalCount = wb.generalRecords().size();
        int entityCount  = wb.entityItems().size();
        long queueMs = lastIngestNanos > 0
                ? (System.nanoTime() - lastIngestNanos) / 1_000_000 : -1;
        log.info("Writing {} record(s) to topic '{}' ({} general, {} entity routes) — pipeline queue {}ms",
                recordCount, topic, generalCount, entityCount, queueMs);

        meters.writeDuration().record(() -> {
            try { writeChannel.submit(wb); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new RuntimeException(e); }
        });
        messagesWritten.addAndGet(recordCount);
        meters.messagesWritten().increment(recordCount);
        lastBatchAt = Instant.now();

        // Collect entity routes across all entity items for known_entities upsert
        List<EntityRoute> allRoutes = new ArrayList<>();
        for (WriteBatch.EntityWriteItem item : wb.entityItems()) {
            allRoutes.addAll(item.routes());
        }
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
                // Only seek uncommitted partitions to beginning; partitions that already
                // have a committed offset resume from there so restarts don't re-read
                // already-processed messages.  Mirrors Kafka's auto.offset.reset=earliest.
                Map<TopicPartition, OffsetAndMetadata> committed = kc.committed(new java.util.HashSet<>(assigned));
                List<TopicPartition> noOffset = assigned.stream()
                        .filter(tp -> committed.get(tp) == null)
                        .toList();
                if (!noOffset.isEmpty()) {
                    kc.seekToBeginning(noOffset);
                    log.info("Seeked to beginning of {} uncommitted partition(s) for topic '{}': {}",
                            noOffset.size(), topic, noOffset);
                }
                List<TopicPartition> resuming = assigned.stream()
                        .filter(tp -> committed.get(tp) != null)
                        .toList();
                if (!resuming.isEmpty()) {
                    log.info("Resuming {} committed partition(s) for topic '{}' from stored offsets",
                            resuming.size(), topic);
                }
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

    /**
     * Positions a manually-assigned partition on startup.
     * Mirrors {@code onPartitionsAssigned} logic for the group-subscription path:
     * - seekToEarliest → seek to beginning if no committed offset, else resume
     * - seekToTimestamp → seek to timestamp if no committed offset, else resume
     * - default → seek to end if no committed offset, else resume
     */
    private void positionPartition(KafkaConsumer<String, byte[]> kc, TopicPartition tp) {
        Map<TopicPartition, OffsetAndMetadata> committed = kc.committed(java.util.Set.of(tp));
        boolean hasCommitted = committed.get(tp) != null;

        if (hasCommitted) {
            // Always resume from committed offset regardless of startFrom setting.
            // startFrom only applies to the very first run (no prior committed offset).
            log.info("Resuming partition {} of '{}' from committed offset {}", tp.partition(), topic,
                    committed.get(tp).offset());
        } else if (seekToEarliest) {
            kc.seekToBeginning(List.of(tp));
            log.info("No committed offset for partition {} of '{}'; seeking to beginning", tp.partition(), topic);
        } else if (seekToTimestamp != null) {
            seekToTimestamp(kc, List.of(tp), seekToTimestamp);
        } else {
            kc.seekToEnd(List.of(tp));
            log.info("No committed offset for partition {} of '{}'; seeking to end", tp.partition(), topic);
        }
    }

    private void updateLag(KafkaConsumer<String, byte[]> kc) {
        try {
            var endOffsets = kc.endOffsets(kc.assignment());
            long lag = 0;
            for (var entry : endOffsets.entrySet()) {
                // Use kc.position() — the local consumer cursor — instead of kc.committed()
                // which issues a synchronous OffsetFetch request to the broker on every poll
                // cycle. position() is accurate for in-flight records and has zero network cost.
                long position = kc.position(entry.getKey());
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
