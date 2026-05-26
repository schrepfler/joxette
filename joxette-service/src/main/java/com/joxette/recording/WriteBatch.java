package com.joxette.recording;

import com.joxette.replay.EntityRoute;
import com.joxette.replay.KafkaMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * A unit of work submitted to {@link DuckLakeWriteChannel}.
 *
 * <p>Carries both the general-cassette records and the entity routes for a single
 * batch so the drain VT can write both atomically under the serialized connection.
 *
 * <p>{@code partitions} is the set of {@link TopicPartition}s covered by this batch.
 * It is used during rebalance to scope the drain: under KIP-848 only batches whose
 * partition set intersects the revoked set need to be drained before returning from
 * {@code onPartitionsRevoked}; batches for non-revoked partitions continue normally.
 *
 * <p>The {@code result} future is completed by the drain VT once all writes in the
 * batch succeed, or completed exceptionally on the first write failure.  Callers
 * block on {@link CompletableFuture#join()} to propagate backpressure: the Jox flow
 * step that calls {@link DuckLakeWriteChannel#submit(WriteBatch)} will not advance
 * until the drain VT has finished writing.
 */
public record WriteBatch(
        String topic,
        Set<TopicPartition> partitions,
        List<ConsumerRecord<String, byte[]>> generalRecords,
        List<String> generalMessageTypes,
        List<EntityWriteItem> entityItems,
        CompletableFuture<WriteResult> result
) {

    public static WriteBatch of(
            String topic,
            List<ConsumerRecord<String, byte[]>> generalRecords,
            List<String> generalMessageTypes,
            List<EntityWriteItem> entityItems) {
        Set<TopicPartition> partitions = generalRecords.stream()
                .map(r -> new TopicPartition(r.topic(), r.partition()))
                .collect(Collectors.toUnmodifiableSet());
        return new WriteBatch(topic, partitions, generalRecords, generalMessageTypes, entityItems,
                new CompletableFuture<>());
    }

    /**
     * One entity-route write: the routes to persist and the message they came from.
     */
    public record EntityWriteItem(List<EntityRoute> routes, KafkaMessage message) {}
}
