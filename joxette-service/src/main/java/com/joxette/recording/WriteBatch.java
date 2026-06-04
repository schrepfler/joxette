package com.joxette.recording;

import com.joxette.replay.EntityRoute;
import com.joxette.replay.KafkaMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashSet;
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
 * <p>{@code sourceRecords} is the full set of Kafka records before routing — used
 * by {@link TopicRecorder} to build Kafka offset commits after a successful write.
 * It is also the basis for the {@code batchWeighted} cost function so that
 * coalescing is bounded by record count rather than routed-record count.
 *
 * <p>{@code partitions} is derived from {@code sourceRecords} and is used during
 * rebalance to scope the drain under KIP-848.
 *
 * <p>The {@code result} future is completed by the drain VT once all writes succeed,
 * or completed exceptionally on the first write failure.  Callers block on
 * {@link CompletableFuture#join()} to propagate backpressure.
 */
public record WriteBatch(
        String topic,
        Set<TopicPartition> partitions,
        List<ConsumerRecord<String, byte[]>> sourceRecords,   // ALL records (for offset commits)
        List<ConsumerRecord<String, byte[]>> generalRecords,
        List<String> generalMessageTypes,
        List<EntityWriteItem> entityItems,
        CompletableFuture<WriteResult> result
) {

    public static WriteBatch of(
            String topic,
            List<ConsumerRecord<String, byte[]>> sourceRecords,
            List<ConsumerRecord<String, byte[]>> generalRecords,
            List<String> generalMessageTypes,
            List<EntityWriteItem> entityItems) {
        Set<TopicPartition> partitions = sourceRecords.stream()
                .map(r -> new TopicPartition(r.topic(), r.partition()))
                .collect(Collectors.toUnmodifiableSet());
        return new WriteBatch(topic, partitions,
                List.copyOf(sourceRecords),
                generalRecords, generalMessageTypes, entityItems,
                new CompletableFuture<>());
    }

    /**
     * Record count used as the cost function in {@code Flow.batchWeighted}.
     * Counts all source records (before routing) so the coalescing budget is
     * proportional to actual Kafka consumption, not just written records.
     */
    public long recordCount() { return sourceRecords.size(); }

    /**
     * Returns a new {@code WriteBatch} that merges {@code this} and {@code other},
     * concatenating all lists in order. The result future is fresh.
     *
     * <p>Used by {@link TopicRecorder}'s {@code batchWeighted} coalescing step.
     */
    public WriteBatch mergeWith(WriteBatch other) {
        var src  = concat(sourceRecords,       other.sourceRecords);
        var gen  = concat(generalRecords,      other.generalRecords);
        var types = concat(generalMessageTypes, other.generalMessageTypes);
        var ent  = concat(entityItems,         other.entityItems);
        var parts = new HashSet<>(partitions);
        parts.addAll(other.partitions);
        return new WriteBatch(topic, Set.copyOf(parts),
                List.copyOf(src), List.copyOf(gen), List.copyOf(types),
                List.copyOf(ent), new CompletableFuture<>());
    }

    private static <T> List<T> concat(List<T> a, List<T> b) {
        var merged = new ArrayList<T>(a.size() + b.size());
        merged.addAll(a); merged.addAll(b);
        return merged;
    }

    /**
     * One entity-route write: the routes to persist and the message they came from.
     */
    public record EntityWriteItem(List<EntityRoute> routes, KafkaMessage message) {}
}
