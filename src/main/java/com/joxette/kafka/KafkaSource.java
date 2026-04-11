package com.joxette.kafka;

import com.softwaremill.jox.flows.Flow;
import com.softwaremill.jox.flows.Flows;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Consumer-side Kafka abstraction that exposes a Jox {@link Flow} of
 * {@link ConsumerRecord} objects.
 *
 * <p>Mirrors the {@code KafkaFlow} class from the unpublished
 * {@code com.softwaremill.jox:kafka} module (v0.5.3). See {@link ConsumerSettings}
 * for the migration path once that module is published to Maven Central.
 *
 * <p>One instance per Kafka consumer (per topic). Not a Spring bean — instantiated
 * by callers per recording session.
 *
 * <h2>Thread-safety contract</h2>
 * <ul>
 *   <li>{@link #subscribe(String, ConsumerRebalanceListener)} drives the Kafka
 *       consumer on the calling virtual thread (the consumer thread).</li>
 *   <li>{@link #scheduleCommit(Map)} may be called from any thread; the commit is
 *       drained on the consumer thread at the top of the next poll iteration,
 *       satisfying Kafka's requirement that {@code commitSync} is called on the
 *       consumer thread.</li>
 *   <li>{@link #seekToBeginning(Collection)} and
 *       {@link #seekToTimestamp(Collection, Instant)} <strong>must</strong> be
 *       called only from a {@link ConsumerRebalanceListener#onPartitionsAssigned}
 *       callback, which Kafka invokes on the consumer thread from within
 *       {@code poll()}.</li>
 * </ul>
 *
 * @param <K> record key type
 * @param <V> record value type
 */
public class KafkaSource<K, V> {

    private static final Logger log = LoggerFactory.getLogger(KafkaSource.class);
    private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);

    private final ConsumerSettings<K, V> settings;
    private final Duration pollTimeout;

    private volatile KafkaConsumer<K, V> consumer;
    private volatile boolean stopped = false;

    private final AtomicReference<Map<TopicPartition, OffsetAndMetadata>> pendingCommit =
            new AtomicReference<>();

    public KafkaSource(ConsumerSettings<K, V> settings) {
        this(settings, DEFAULT_POLL_TIMEOUT);
    }

    public KafkaSource(ConsumerSettings<K, V> settings, Duration pollTimeout) {
        this.settings = settings;
        this.pollTimeout = pollTimeout;
    }

    /**
     * Returns a {@link Flow} that drives the Kafka consumer, emitting one
     * {@link ConsumerRecord} at a time until {@link #stop()} is called.
     *
     * <p>The {@link KafkaConsumer} is created and closed inside the flow's
     * {@code usingEmit} lambda, so its lifecycle is tied to the flow. The
     * consumer subscribes to {@code topic} with the supplied rebalance listener.
     *
     * <p>This method must be called at most once per {@code KafkaSource} instance.
     */
    public Flow<ConsumerRecord<K, V>> subscribe(String topic, ConsumerRebalanceListener listener) {
        return Flows.<ConsumerRecord<K, V>>usingEmit(emit -> {
            try (KafkaConsumer<K, V> kc = new KafkaConsumer<>(
                    settings.toProperties(),
                    settings.keyDeserializer(),
                    settings.valueDeserializer())) {

                this.consumer = kc;
                kc.subscribe(List.of(topic), listener);

                while (!stopped && !Thread.currentThread().isInterrupted()) {
                    Map<TopicPartition, OffsetAndMetadata> toCommit = pendingCommit.getAndSet(null);
                    if (toCommit != null) {
                        kc.commitSync(toCommit);
                    }
                    try {
                        for (ConsumerRecord<K, V> record : kc.poll(pollTimeout)) {
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
        });
    }

    /**
     * Queues {@code offsets} to be committed on the next poll iteration.
     *
     * <p>Safe to call from any thread. The actual {@code commitSync} runs on the
     * consumer thread (inside the {@link #subscribe} poll loop).
     */
    public void scheduleCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        pendingCommit.set(offsets);
    }

    /**
     * Signals the consumer to stop: sets the stopped flag and calls
     * {@code wakeup()} on the active consumer, if any.
     */
    public void stop() {
        stopped = true;
        KafkaConsumer<K, V> c = consumer;
        if (c != null) c.wakeup();
    }

    /**
     * Seeks all {@code partitions} to the beginning.
     *
     * <p><strong>Must only be called from
     * {@link ConsumerRebalanceListener#onPartitionsAssigned}</strong>, which
     * Kafka invokes on the consumer thread from within {@code poll()}.
     */
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        KafkaConsumer<K, V> c = consumer;
        if (c != null) {
            c.seekToBeginning(partitions);
        }
    }

    /**
     * Seeks each partition to the first offset at or after {@code timestamp}.
     * Partitions with no messages at or after the timestamp are seeked to the end.
     *
     * <p><strong>Must only be called from
     * {@link ConsumerRebalanceListener#onPartitionsAssigned}</strong>.
     */
    public void seekToTimestamp(Collection<TopicPartition> partitions, Instant timestamp) {
        KafkaConsumer<K, V> c = consumer;
        if (c == null) return;
        long epochMs = timestamp.toEpochMilli();
        Map<TopicPartition, Long> query = new HashMap<>();
        for (TopicPartition tp : partitions) {
            query.put(tp, epochMs);
        }
        Map<TopicPartition, OffsetAndTimestamp> results = c.offsetsForTimes(query);
        for (TopicPartition tp : partitions) {
            OffsetAndTimestamp ot = results.get(tp);
            if (ot != null) {
                c.seek(tp, ot.offset());
                log.debug("Seeked {} to offset {} (timestamp {})", tp, ot.offset(), timestamp);
            } else {
                c.seekToEnd(List.of(tp));
                log.debug("No messages at or after {} on {}; seeking to end", timestamp, tp);
            }
        }
        log.debug("Seeked {} partition(s) to timestamp {}", partitions.size(), timestamp);
    }
}
