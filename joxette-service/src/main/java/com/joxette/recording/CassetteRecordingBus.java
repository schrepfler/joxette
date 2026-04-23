package com.joxette.recording;

import com.joxette.config.JoxetteProperties;
import com.joxette.replay.CassetteRecord;
import com.joxette.replay.EntityRecord;
import com.joxette.replay.EntityRoute;
import com.joxette.replay.KafkaMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In-process fanout of newly-durable cassette records.
 *
 * <p>The {@link DuckLakeWriteChannel} drain VT calls {@link #publish(WriteBatch)}
 * immediately after a batch's result future completes successfully.  The bus
 * distributes each record to subscribers whose key matches (topic for general
 * cassettes, {@code (entityType, entityId)} for entity cassettes).
 *
 * <p>Delivery is non-blocking: each subscriber owns a bounded
 * {@link java.util.concurrent.ArrayBlockingQueue}, and publication uses
 * {@link BlockingQueue#offer(Object)} — returning {@code false} when the queue
 * is full.  On offer failure the subscription is flagged
 * {@linkplain Subscription#isOverflowed() overflowed}; subsequent publishes to
 * that subscription are dropped so the bus never blocks the drain VT.
 *
 * <p>Consumers (built in Task B) poll or take from
 * {@link TopicSubscription#queue()} / {@link EntitySubscription#queue()} and
 * must call {@link Subscription#close()} when their stream ends.
 */
@Component
public class CassetteRecordingBus {

    private static final Logger log = LoggerFactory.getLogger(CassetteRecordingBus.class);

    private final int defaultCapacity;

    private final ConcurrentMap<String, CopyOnWriteArrayList<TopicSubscription>> topicSubs =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<EntityKey, CopyOnWriteArrayList<EntitySubscription>> entitySubs =
            new ConcurrentHashMap<>();

    public CassetteRecordingBus(JoxetteProperties properties) {
        this.defaultCapacity = properties.getReplay().getFollow().getBufferCapacity();
    }

    // -----------------------------------------------------------------------
    // Subscription API
    // -----------------------------------------------------------------------

    /** Composite key for entity-cassette subscriptions. */
    public record EntityKey(String entityType, String entityId) {
        public EntityKey {
            Objects.requireNonNull(entityType, "entityType");
            Objects.requireNonNull(entityId, "entityId");
        }
    }

    /** Common handle for a live subscription. */
    public sealed interface Subscription permits TopicSubscription, EntitySubscription {
        /** Unregisters this subscription. Idempotent. */
        void close();

        /**
         * True after the bus has attempted to deliver a record and the bounded
         * queue was full.  Subsequent publishes to this subscription are dropped.
         */
        boolean isOverflowed();
    }

    public final class TopicSubscription implements Subscription {

        private final String topic;
        private final BlockingQueue<CassetteRecord> queue;
        private final AtomicBoolean overflowed = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();

        private TopicSubscription(String topic, int capacity) {
            this.topic = topic;
            this.queue = new ArrayBlockingQueue<>(capacity);
        }

        public String topic() { return topic; }

        public BlockingQueue<CassetteRecord> queue() { return queue; }

        @Override
        public boolean isOverflowed() { return overflowed.get(); }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                CopyOnWriteArrayList<TopicSubscription> list = topicSubs.get(topic);
                if (list != null) {
                    list.remove(this);
                    topicSubs.computeIfPresent(topic, (k, v) -> v.isEmpty() ? null : v);
                }
            }
        }
    }

    public final class EntitySubscription implements Subscription {

        private final EntityKey key;
        private final BlockingQueue<EntityRecord> queue;
        private final AtomicBoolean overflowed = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();

        private EntitySubscription(EntityKey key, int capacity) {
            this.key = key;
            this.queue = new ArrayBlockingQueue<>(capacity);
        }

        public EntityKey key() { return key; }

        public BlockingQueue<EntityRecord> queue() { return queue; }

        @Override
        public boolean isOverflowed() { return overflowed.get(); }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                CopyOnWriteArrayList<EntitySubscription> list = entitySubs.get(key);
                if (list != null) {
                    list.remove(this);
                    entitySubs.computeIfPresent(key, (k, v) -> v.isEmpty() ? null : v);
                }
            }
        }
    }

    /** Subscribe to all published records for a given topic using the configured default capacity. */
    public TopicSubscription subscribeTopic(String topic) {
        return subscribeTopic(topic, defaultCapacity);
    }

    public TopicSubscription subscribeTopic(String topic, int capacity) {
        Objects.requireNonNull(topic, "topic");
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be positive");
        TopicSubscription sub = new TopicSubscription(topic, capacity);
        topicSubs.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(sub);
        return sub;
    }

    public EntitySubscription subscribeEntity(String entityType, String entityId) {
        return subscribeEntity(entityType, entityId, defaultCapacity);
    }

    public EntitySubscription subscribeEntity(String entityType, String entityId, int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be positive");
        EntityKey key = new EntityKey(entityType, entityId);
        EntitySubscription sub = new EntitySubscription(key, capacity);
        entitySubs.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(sub);
        return sub;
    }

    // -----------------------------------------------------------------------
    // Publish — called by DuckLakeWriteChannel on the drain VT
    // -----------------------------------------------------------------------

    /**
     * Fans out every record in {@code batch} to matching subscribers.
     *
     * <p>Must be invoked only after {@link WriteBatch#result()} has completed
     * successfully so subscribers never observe a record that failed to commit.
     *
     * <p>This method must not block: it uses non-blocking {@link BlockingQueue#offer}
     * on each subscriber's queue.  A subscriber whose queue is full is marked
     * {@linkplain Subscription#isOverflowed() overflowed} and skipped for
     * subsequent records in this batch and future batches.
     */
    public void publish(WriteBatch batch) {
        // Fast path: no subscribers at all.
        if (topicSubs.isEmpty() && entitySubs.isEmpty()) return;

        Instant recordedAt = Instant.now();

        // General cassette fanout
        List<ConsumerRecord<String, byte[]>> generalRecords = batch.generalRecords();
        if (!generalRecords.isEmpty()) {
            CopyOnWriteArrayList<TopicSubscription> subs = topicSubs.get(batch.topic());
            if (subs != null && !subs.isEmpty()) {
                List<String> messageTypes = batch.generalMessageTypes();
                for (int i = 0; i < generalRecords.size(); i++) {
                    ConsumerRecord<String, byte[]> r = generalRecords.get(i);
                    String messageType = (messageTypes != null && i < messageTypes.size())
                            ? messageTypes.get(i) : null;
                    CassetteRecord cr = toCassetteRecord(batch.topic(), r, messageType, recordedAt);
                    for (TopicSubscription sub : subs) {
                        deliver(sub, cr);
                    }
                }
            }
        }

        // Entity cassette fanout
        for (WriteBatch.EntityWriteItem item : batch.entityItems()) {
            for (EntityRoute route : item.routes()) {
                EntityKey key = new EntityKey(route.entityType(), route.entityId());
                CopyOnWriteArrayList<EntitySubscription> subs = entitySubs.get(key);
                if (subs == null || subs.isEmpty()) continue;
                EntityRecord er = toEntityRecord(route, item.message(), recordedAt);
                for (EntitySubscription sub : subs) {
                    deliver(sub, er);
                }
            }
        }
    }

    private static void deliver(TopicSubscription sub, CassetteRecord record) {
        if (sub.overflowed.get()) return;
        if (!sub.queue.offer(record)) {
            if (sub.overflowed.compareAndSet(false, true)) {
                log.warn("Topic follow subscription for '{}' overflowed its buffer; dropping records",
                        sub.topic);
            }
        }
    }

    private static void deliver(EntitySubscription sub, EntityRecord record) {
        if (sub.overflowed.get()) return;
        if (!sub.queue.offer(record)) {
            if (sub.overflowed.compareAndSet(false, true)) {
                log.warn("Entity follow subscription for {}/{} overflowed its buffer; dropping records",
                        sub.key.entityType(), sub.key.entityId());
            }
        }
    }

    // -----------------------------------------------------------------------
    // Record builders — mirror CassetteBatchWriter / EntityCassetteBatchWriter
    // -----------------------------------------------------------------------

    /**
     * Builds a {@link CassetteRecord} from the same inputs
     * {@link CassetteBatchWriter#writeBatch} uses to INSERT.
     */
    static CassetteRecord toCassetteRecord(String topic,
                                           ConsumerRecord<String, byte[]> r,
                                           String messageType,
                                           Instant recordedAt) {
        List<CassetteRecord.Header> headers = new ArrayList<>();
        for (Header h : r.headers()) {
            headers.add(new CassetteRecord.Header(
                    h.key(),
                    CassetteBatchWriter.decodeHeaderValue(h.value())));
        }
        return new CassetteRecord(
                topic,
                r.partition(),
                r.offset(),
                Instant.ofEpochMilli(r.timestamp()),
                recordedAt,
                r.key(),
                encodeBlob(r.value()),
                headers,
                messageType
        );
    }

    /**
     * Builds an {@link EntityRecord} from the same inputs
     * {@link EntityCassetteBatchWriter#writeRoutes} uses to INSERT.
     */
    static EntityRecord toEntityRecord(EntityRoute route, KafkaMessage message, Instant recordedAt) {
        List<CassetteRecord.Header> headers = new ArrayList<>();
        for (KafkaMessage.Header h : message.headers()) {
            headers.add(new CassetteRecord.Header(
                    h.key(),
                    CassetteBatchWriter.decodeHeaderValue(h.value())));
        }
        return new EntityRecord(
                route.entityId(),
                route.messageType(),
                message.topic(),
                message.partition(),
                message.offset(),
                Instant.ofEpochMilli(message.timestampMs()),
                recordedAt,
                message.key(),
                encodeBlob(message.value()),
                headers
        );
    }

    private static String encodeBlob(byte[] bytes) {
        return bytes == null ? null
                : Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }
}
