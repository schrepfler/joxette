package com.joxette.recording;

import com.joxette.replay.EntityRoute;
import com.joxette.replay.KafkaMessage;
import com.joxette.replay.KnownEntitiesRepository;
import com.joxette.replay.MessageRouter;
import com.joxette.replay.RouteDecision;
import com.softwaremill.jox.flows.Flows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Records a single Kafka topic to the DuckLake cassette tables.
 *
 * <h2>Pipeline design</h2>
 * <ol>
 *   <li>A Jox {@code Flow} source drives the Kafka consumer, emitting one
 *       {@link ConsumerRecord} at a time.</li>
 *   <li>{@code groupedWithin} accumulates records into bounded batches.</li>
 *   <li>For each batch, each record is routed via {@link MessageRouter}:
 *     <ul>
 *       <li>If {@code routeToGeneral} is true, the record is written to the
 *           general cassette ({@code lake.main.general_{topic}}).</li>
 *       <li>For each {@link EntityRoute}, the record is written to the
 *           corresponding entity cassette ({@code lake.main.entity_{type}}).</li>
 *     </ul>
 *   </li>
 *   <li>After a successful batch write, the distinct entity routes are upserted
 *       into {@code known_entities} and Kafka offsets are committed.</li>
 * </ol>
 */
public class TopicRecorder {

    private static final Logger log = LoggerFactory.getLogger(TopicRecorder.class);

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    private final String topic;
    private final Map<String, Object> kafkaProps;
    private final Connection duckDbConnection;
    private final int batchSize;
    private final Duration batchTimeout;
    private final MessageRouter router;
    private final KnownEntitiesRepository knownEntities;
    private final boolean seekToEarliest;
    private final Instant seekToTimestamp;

    private volatile KafkaConsumer<String, byte[]> consumer;
    private volatile boolean stopped = false;

    private final AtomicReference<Map<TopicPartition, OffsetAndMetadata>> pendingCommit =
            new AtomicReference<>();

    public TopicRecorder(
            String topic,
            Map<String, Object> kafkaProps,
            Connection duckDbConnection,
            int batchSize,
            long batchTimeoutMs,
            MessageRouter router,
            KnownEntitiesRepository knownEntities,
            String startFrom) {
        this.topic          = topic;
        this.duckDbConnection = duckDbConnection;
        this.batchSize      = batchSize;
        this.batchTimeout   = Duration.ofMillis(batchTimeoutMs);
        this.router         = router;
        this.knownEntities  = knownEntities;
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

        Map<String, Object> props = new HashMap<>(kafkaProps);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "joxette-recorder-" + topic);
        if (seekToEarliest) {
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }
        this.kafkaProps = props;
    }

    // -----------------------------------------------------------------------
    // Pipeline
    // -----------------------------------------------------------------------

    public void run() throws Exception {
        log.info("Starting recorder for topic '{}'", topic);

        try (KafkaConsumer<String, byte[]> kafkaConsumer = createConsumer();
             CassetteBatchWriter generalWriter = new CassetteBatchWriter(topic, duckDbConnection);
             EntityCassetteBatchWriter entityWriter = new EntityCassetteBatchWriter(duckDbConnection)) {

            this.consumer = kafkaConsumer;
            kafkaConsumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    if (seekToEarliest) {
                        kafkaConsumer.seekToBeginning(partitions);
                        log.info("Seeked to beginning of {} partition(s) for topic '{}' (startFrom=earliest)",
                                partitions.size(), topic);
                    } else if (seekToTimestamp != null) {
                        seekPartitionsToTimestamp(kafkaConsumer, partitions, seekToTimestamp);
                    }
                }
            });

            Flows.<ConsumerRecord<String, byte[]>>usingEmit(emit -> {
                        try {
                            while (!stopped && !Thread.currentThread().isInterrupted()) {
                                Map<TopicPartition, OffsetAndMetadata> toCommit =
                                        pendingCommit.getAndSet(null);
                                if (toCommit != null) {
                                    kafkaConsumer.commitSync(toCommit);
                                }
                                try {
                                    for (ConsumerRecord<String, byte[]> record :
                                            kafkaConsumer.poll(POLL_TIMEOUT)) {
                                        emit.apply(record);
                                    }
                                } catch (WakeupException e) {
                                    log.info("Kafka wakeup received for topic '{}'; stopping poll loop", topic);
                                    break;
                                }
                            }
                        } finally {
                            this.consumer = null;
                        }
                    })
                    .groupedWithin(batchSize, batchTimeout)
                    .runForeach(batch -> {
                        writeBatch(batch, generalWriter, entityWriter);
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

    /**
     * Routes each record in the batch, writes to general and/or entity cassettes,
     * then upserts the discovered entities into {@code known_entities}.
     */
    private void writeBatch(
            List<ConsumerRecord<String, byte[]>> batch,
            CassetteBatchWriter generalWriter,
            EntityCassetteBatchWriter entityWriter) throws SQLException {

        // Separate general-cassette records from entity-routed records
        List<ConsumerRecord<String, byte[]>> generalBatch = new ArrayList<>();
        List<EntityRoute> allRoutes = new ArrayList<>();

        for (ConsumerRecord<String, byte[]> record : batch) {
            KafkaMessage msg = toKafkaMessage(record);
            RouteDecision decision = router.route(msg);

            if (decision.routeToGeneral()) {
                generalBatch.add(record);
            }
            if (!decision.entityRoutes().isEmpty()) {
                for (EntityRoute route : decision.entityRoutes()) {
                    try {
                        entityWriter.writeRoutes(List.of(route), msg);
                    } catch (SQLException e) {
                        log.error("Failed to write entity route for type={} id={}: {}",
                                route.entityType(), route.entityId(), e.getMessage());
                        throw e;
                    }
                    allRoutes.add(route);
                }
            }
        }

        // Write general cassette batch
        if (!generalBatch.isEmpty()) {
            generalWriter.writeBatch(generalBatch);
        }

        // Upsert discovered entities into known_entities
        if (!allRoutes.isEmpty()) {
            try {
                knownEntities.upsertBatch(allRoutes, java.time.Instant.now());
            } catch (Exception e) {
                log.warn("Failed to upsert known_entities batch: {}", e.getMessage());
                // Non-fatal: entity registry is best-effort; don't abort the batch
            }
        }
    }

    private void seekPartitionsToTimestamp(
            KafkaConsumer<String, byte[]> kafkaConsumer,
            Collection<TopicPartition> partitions,
            Instant timestamp) {
        long epochMs = timestamp.toEpochMilli();
        Map<TopicPartition, Long> query = new HashMap<>();
        for (TopicPartition tp : partitions) {
            query.put(tp, epochMs);
        }
        Map<TopicPartition, OffsetAndTimestamp> results = kafkaConsumer.offsetsForTimes(query);
        for (TopicPartition tp : partitions) {
            OffsetAndTimestamp ot = results.get(tp);
            if (ot != null) {
                kafkaConsumer.seek(tp, ot.offset());
                log.debug("Seeked {} to offset {} (timestamp {})", tp, ot.offset(), timestamp);
            } else {
                // No messages at or after the requested timestamp — seek to end
                kafkaConsumer.seekToEnd(List.of(tp));
                log.debug("No messages at or after {} on {}; seeking to end", timestamp, tp);
            }
        }
        log.info("Seeked {} partition(s) for topic '{}' to timestamp {}",
                partitions.size(), topic, timestamp);
    }

    public void stop() {
        stopped = true;
        KafkaConsumer<String, byte[]> c = consumer;
        if (c != null) c.wakeup();
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private KafkaConsumer<String, byte[]> createConsumer() {
        return new KafkaConsumer<>(kafkaProps);
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
}
