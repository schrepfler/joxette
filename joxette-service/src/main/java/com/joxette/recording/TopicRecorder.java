package com.joxette.recording;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    /** The live consumer — set during {@link #run()}, cleared on exit. */
    private volatile KafkaConsumer<String, byte[]> consumer;

    /** Pending offsets to commit on the next poll iteration (thread-safe). */
    private final AtomicReference<Map<TopicPartition, OffsetAndMetadata>> pendingCommit =
            new AtomicReference<>();

    private volatile boolean stopped = false;

    private volatile Instant lastBatchAt;
    private final AtomicLong consumerLag = new AtomicLong(-1);

    public TopicRecorder(
            String topic,
            ConsumerSettings<String, byte[]> baseSettings,
            DuckLakeWriteChannel writeChannel,
            int batchSize,
            long batchTimeoutMs,
            MessageRouter router,
            KnownEntitiesRepository knownEntities,
            String startFrom) {
        this.topic        = topic;
        this.writeChannel = writeChannel;
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

        ConsumerSettings<String, byte[]> s = baseSettings.groupId("joxette-recorder-" + topic);
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
                try (KafkaConsumer<String, byte[]> kc = settings.toConsumer()) {
                    this.consumer = kc;
                    kc.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                        @Override
                        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

                        @Override
                        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                            if (seekToEarliest) {
                                kc.seekToBeginning(partitions);
                                log.info("Seeked to beginning of {} partition(s) for topic '{}' (startFrom=earliest)",
                                        partitions.size(), topic);
                            } else if (seekToTimestamp != null) {
                                seekToTimestamp(kc, partitions, seekToTimestamp);
                                log.info("Seeked to timestamp {} on {} partition(s) for topic '{}'",
                                        seekToTimestamp, partitions.size(), topic);
                            }
                        }
                    });

                    while (!stopped && !Thread.currentThread().isInterrupted()) {
                        Map<TopicPartition, OffsetAndMetadata> toCommit = pendingCommit.getAndSet(null);
                        if (toCommit != null) {
                            kc.commitSync(toCommit);
                        }
                        try {
                            var records = kc.poll(POLL_TIMEOUT);
                            updateLag(kc);
                            for (ConsumerRecord<String, byte[]> record : records) {
                                emit.apply(record);
                            }
                        } catch (WakeupException e) {
                            log.debug("Kafka wakeup received for topic '{}'; stopping poll loop", topic);
                            break;
                        }
                    }
                } finally {
                    this.consumer = null;
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

        WriteBatch wb = WriteBatch.of(topic, generalRecords, generalMessageTypes, entityItems);
        writeChannel.submit(wb);
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
