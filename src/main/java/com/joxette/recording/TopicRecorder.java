package com.joxette.recording;

import com.softwaremill.jox.flows.Flows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Records a single Kafka topic to its DuckLake general cassette table.
 *
 * <h2>Pipeline design</h2>
 * <ol>
 *   <li>A Jox {@link Flow} source drives the Kafka consumer in the producer
 *       virtual thread, emitting one {@link ConsumerRecord} at a time.</li>
 *   <li>{@link Flow#grouped(int, Duration)} accumulates records into batches
 *       bounded by {@code batchSize} and {@code batchTimeout}.</li>
 *   <li>The terminal {@code runForeach} writes each batch via
 *       {@link CassetteBatchWriter} then queues the Kafka offset map for the
 *       producer thread to commit synchronously before its next {@code poll()}.</li>
 * </ol>
 *
 * <h2>Threading model</h2>
 * Jox's {@code usingEmit} forks the producer lambda onto a separate virtual
 * thread.  Since {@link KafkaConsumer} is single-threaded, <em>all</em>
 * consumer operations ({@code poll} and {@code commitSync}) are executed
 * exclusively on that producer virtual thread.  The consumer thread (running
 * {@code runForeach}) signals commits via an {@link AtomicReference}; the
 * producer checks and applies them at the top of every poll loop iteration.
 *
 * <h2>Shutdown</h2>
 * {@link #stop()} sets a flag and calls {@link KafkaConsumer#wakeup()},
 * causing the in-flight {@code poll()} to throw {@link WakeupException} so
 * the pipeline terminates cleanly.  If the writer fails, the exception
 * propagates through {@code runForeach}, Jox cancels the producer fork, and
 * the {@link WakeupException} / interrupt causes the poll loop to exit.
 */
public class TopicRecorder {

    private static final Logger log = LoggerFactory.getLogger(TopicRecorder.class);

    /** Poll timeout kept short so that stop requests are noticed quickly. */
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    private final String topic;
    private final Map<String, Object> kafkaProps;
    private final Connection duckDbConnection;
    private final int batchSize;
    private final Duration batchTimeout;

    // Written only from the owner thread; read (wakeup) from an external thread.
    private volatile KafkaConsumer<String, byte[]> consumer;
    private volatile boolean stopped = false;

    /**
     * Cross-thread commit signal: the runForeach thread writes the offset map
     * after a successful batch write; the producer thread reads and commits it
     * before the next {@code poll()}.
     */
    private final AtomicReference<Map<TopicPartition, OffsetAndMetadata>> pendingCommit =
            new AtomicReference<>();

    public TopicRecorder(
            String topic,
            Map<String, Object> kafkaProps,
            Connection duckDbConnection,
            int batchSize,
            long batchTimeoutMs) {
        this.topic = topic;
        this.duckDbConnection = duckDbConnection;
        this.batchSize = batchSize;
        this.batchTimeout = Duration.ofMillis(batchTimeoutMs);

        // Clone base properties and add a deterministic group.id for this topic.
        Map<String, Object> props = new HashMap<>(kafkaProps);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "joxette-recorder-" + topic);
        this.kafkaProps = props;
    }

    // -----------------------------------------------------------------------
    // Pipeline
    // -----------------------------------------------------------------------

    /**
     * Runs the recording pipeline for this topic.  Blocks until the pipeline
     * terminates (clean stop, Kafka disconnect, or writer failure).
     *
     * <p>Intended to be called from a dedicated virtual thread managed by
     * {@link RecordingCoordinator}.
     */
    public void run() throws Exception {
        log.info("Starting recorder for topic '{}'", topic);

        try (KafkaConsumer<String, byte[]> kafkaConsumer = createConsumer();
             CassetteBatchWriter writer = new CassetteBatchWriter(topic, duckDbConnection)) {

            this.consumer = kafkaConsumer;
            kafkaConsumer.subscribe(List.of(topic));

            Flows.<ConsumerRecord<String, byte[]>>usingEmit(emit -> {
                        try {
                            while (!stopped && !Thread.currentThread().isInterrupted()) {
                                // Commit offsets queued by the previous batch write
                                // before consuming more records (at-least-once guarantee).
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
                        writer.writeBatch(batch);
                        pendingCommit.set(buildOffsets(batch));
                    });

        } catch (Exception e) {
            log.error("Recorder for topic '{}' terminated with error", topic, e);
            throw e;
        } finally {
            log.info("Recorder for topic '{}' stopped", topic);
        }
    }

    /**
     * Signals the poll loop to stop and unblocks any in-progress
     * {@link KafkaConsumer#poll} call.  Safe to call from any thread.
     */
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

    /**
     * Builds the offset commit map from a batch: per partition, commit the
     * highest observed offset + 1.
     */
    private static Map<TopicPartition, OffsetAndMetadata> buildOffsets(
            List<ConsumerRecord<String, byte[]>> batch) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (ConsumerRecord<String, byte[]> r : batch) {
            TopicPartition tp = new TopicPartition(r.topic(), r.partition());
            OffsetAndMetadata existing = offsets.get(tp);
            long nextOffset = r.offset() + 1;
            if (existing == null || nextOffset > existing.offset()) {
                offsets.put(tp, new OffsetAndMetadata(nextOffset));
            }
        }
        return offsets;
    }
}
