package com.joxette.recording;

import com.joxette.replay.EntityRoute;
import com.joxette.replay.KafkaMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
     * Minimum {@code kafka_offset} present in {@link #sourceRecords()} per
     * partition. Used by {@link DuckLakeWriteChannel} as a stable identity for a
     * "poison" batch: the {@code WriteBatch} object itself is a fresh instance on
     * every poll/restart (a new {@link CompletableFuture} each time), but the
     * earliest uncommitted offset per partition is deterministic across restarts
     * because a non-retryable write failure never advances the committed offset.
     */
    public Map<Integer, Long> minOffsetsByPartition() {
        Map<Integer, Long> mins = new HashMap<>();
        for (ConsumerRecord<String, byte[]> r : sourceRecords) {
            mins.merge(r.partition(), r.offset(), Math::min);
        }
        return mins;
    }

    /**
     * Splits this batch into one sub-batch per Kafka partition present in
     * {@link #sourceRecords()}, preserving encounter order within each partition.
     * Each sub-batch's {@code sourceRecords}/{@code generalRecords}/{@code entityItems}
     * are filtered in lockstep so the parallel-indexed general lists stay aligned.
     *
     * <p>Every returned sub-batch has exactly one partition, so
     * {@link #partitions()} on the result is always a singleton set — this is what
     * lets {@link DuckLakeWriteChannel#splitAndProcessByPartition} recurse into
     * {@code processBatch} without ever splitting again.
     *
     * <p>Used by {@link DuckLakeWriteChannel} to isolate a poison partition's
     * records from healthy partitions co-batched with it by upstream coalescing.
     */
    public List<WriteBatch> splitByPartition() {
        Map<Integer, List<ConsumerRecord<String, byte[]>>> byPartition = sourceRecords.stream()
                .collect(Collectors.groupingBy(ConsumerRecord::partition, LinkedHashMap::new, Collectors.toList()));

        List<WriteBatch> subBatches = new ArrayList<>(byPartition.size());
        for (var entry : byPartition.entrySet()) {
            int partition = entry.getKey();
            List<ConsumerRecord<String, byte[]>> partitionSourceRecords = entry.getValue();

            List<ConsumerRecord<String, byte[]>> partitionGeneralRecords = new ArrayList<>();
            List<String> partitionGeneralMessageTypes = new ArrayList<>();
            for (int i = 0; i < generalRecords.size(); i++) {
                if (generalRecords.get(i).partition() == partition) {
                    partitionGeneralRecords.add(generalRecords.get(i));
                    partitionGeneralMessageTypes.add(generalMessageTypes.get(i));
                }
            }

            List<EntityWriteItem> partitionEntityItems = entityItems.stream()
                    .filter(item -> item.message().partition() == partition)
                    .collect(Collectors.toList());

            subBatches.add(WriteBatch.of(topic, partitionSourceRecords,
                    partitionGeneralRecords, partitionGeneralMessageTypes, partitionEntityItems));
        }
        return subBatches;
    }

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
