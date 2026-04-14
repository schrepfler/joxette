package com.joxette.recording;

import com.joxette.kafka.ConsumerSettings;
import com.joxette.kafka.KafkaSource;
import com.joxette.replay.EntityRoute;
import com.joxette.replay.GeneralRoute;
import com.joxette.replay.KafkaMessage;
import com.joxette.replay.KnownEntitiesRepository;
import com.joxette.replay.MessageRouter;
import com.joxette.replay.RouteDecision;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * Records a single Kafka topic to the DuckLake cassette tables.
 *
 * <h2>Pipeline design</h2>
 * <ol>
 *   <li>A {@link KafkaSource} drives the Kafka consumer, emitting one
 *       {@link ConsumerRecord} at a time via a Jox {@code Flow}.</li>
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
 *       into {@code known_entities} and Kafka offsets are scheduled for commit.</li>
 * </ol>
 */
public class TopicRecorder {

    private static final Logger log = LoggerFactory.getLogger(TopicRecorder.class);

    private final String topic;
    private final KafkaSource<String, byte[]> source;
    private final Connection duckDbConnection;
    private final int batchSize;
    private final Duration batchTimeout;
    private final MessageRouter router;
    private final KnownEntitiesRepository knownEntities;
    private final boolean seekToEarliest;
    private final Instant seekToTimestamp;

    public TopicRecorder(
            String topic,
            ConsumerSettings<String, byte[]> baseSettings,
            Connection duckDbConnection,
            int batchSize,
            long batchTimeoutMs,
            MessageRouter router,
            KnownEntitiesRepository knownEntities,
            String startFrom) {
        this.topic            = topic;
        this.duckDbConnection = duckDbConnection;
        this.batchSize        = batchSize;
        this.batchTimeout     = Duration.ofMillis(batchTimeoutMs);
        this.router           = router;
        this.knownEntities    = knownEntities;
        this.seekToEarliest   = "earliest".equals(startFrom);

        Instant ts = null;
        if (!seekToEarliest && startFrom != null && !"latest".equals(startFrom) && !startFrom.isBlank()) {
            try {
                ts = Instant.parse(startFrom);
            } catch (DateTimeParseException e) {
                log.warn("Unrecognised startFrom='{}' for topic '{}'; defaulting to latest", startFrom, topic);
            }
        }
        this.seekToTimestamp = ts;

        ConsumerSettings<String, byte[]> settings = baseSettings
                .withProperty(ConsumerConfig.GROUP_ID_CONFIG, "joxette-recorder-" + topic);
        if (seekToEarliest) {
            settings = settings.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }
        this.source = new KafkaSource<>(settings);
    }

    // -----------------------------------------------------------------------
    // Pipeline
    // -----------------------------------------------------------------------

    public void run() throws Exception {
        log.info("Starting recorder for topic '{}'", topic);

        try (CassetteBatchWriter generalWriter = new CassetteBatchWriter(topic, duckDbConnection);
             EntityCassetteBatchWriter entityWriter = new EntityCassetteBatchWriter(duckDbConnection)) {

            source.subscribe(topic, new ConsumerRebalanceListener() {
                        @Override
                        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

                        @Override
                        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                            if (seekToEarliest) {
                                source.seekToBeginning(partitions);
                                log.info("Seeked to beginning of {} partition(s) for topic '{}' (startFrom=earliest)",
                                        partitions.size(), topic);
                            } else if (seekToTimestamp != null) {
                                source.seekToTimestamp(partitions, seekToTimestamp);
                                log.info("Seeked to timestamp {} on {} partition(s) for topic '{}'",
                                        seekToTimestamp, partitions.size(), topic);
                            }
                        }
                    })
                    .groupedWithin(batchSize, batchTimeout)
                    .runForeach(batch -> {
                        writeBatch(batch, generalWriter, entityWriter);
                        source.scheduleCommit(buildOffsets(batch));
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
        source.stop();
    }

    // -----------------------------------------------------------------------
    // Batch write
    // -----------------------------------------------------------------------

    /**
     * Routes each record in the batch, writes to general and/or entity cassettes,
     * then upserts the discovered entities into {@code known_entities}.
     */
    private void writeBatch(
            List<ConsumerRecord<String, byte[]>> batch,
            CassetteBatchWriter generalWriter,
            EntityCassetteBatchWriter entityWriter) throws SQLException {

        log.trace("Processing batch of {} records for topic '{}'", batch.size(), topic);
        List<ConsumerRecord<String, byte[]>> generalBatch = new ArrayList<>();
        List<String> generalMessageTypes = new ArrayList<>();
        List<EntityRoute> allRoutes = new ArrayList<>();

        for (ConsumerRecord<String, byte[]> record : batch) {
            KafkaMessage msg = toKafkaMessage(record);
            RouteDecision decision = router.route(msg);

            GeneralRoute gr = decision.generalRoute();
            if (gr != null) {
                generalBatch.add(record);
                generalMessageTypes.add(gr.messageType());
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

        if (!generalBatch.isEmpty()) {
            generalWriter.writeBatch(generalBatch, generalMessageTypes);
        }

        if (!allRoutes.isEmpty()) {
            try {
                knownEntities.upsertBatch(allRoutes, java.time.Instant.now());
            } catch (Exception e) {
                log.warn("Failed to upsert known_entities batch: {}", e.getMessage());
                // Non-fatal: entity registry is best-effort; don't abort the batch
            }
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

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
