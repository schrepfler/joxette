package com.joxette.replay;

/**
 * Controls how source Kafka partition numbers are mapped to partitions in the
 * target topic during a replay-to-topic operation.
 *
 * <p>The cassette stores the exact source partition for every recorded message.
 * When replaying into a target topic that may have a different partition count,
 * or whose partition layout has drifted from the original source, this enum
 * lets callers choose the appropriate routing behaviour.
 */
public enum PartitionStrategy {

    /**
     * Let Kafka's default partitioner decide the target partition.
     * When the message key is non-null the key is hashed to select a partition;
     * when the key is null round-robin assignment is used.
     *
     * <p>Safe across any source/target partition count mismatch and across
     * recordings that span multiple source partition layouts.
     * This is the default.
     */
    DEFAULT,

    /**
     * Carry the exact source partition number to the target topic verbatim.
     *
     * <p>Only valid when the source and target topic have the same partition
     * count. The engine validates this upfront for topic replays and per
     * source-topic on first occurrence for entity replays; a mismatch causes
     * the replay to abort with a 400 error.
     *
     * <p>Useful when downstream consumers rely on partition-local ordering
     * guarantees that were established during the original recording.
     */
    PRESERVE,

    /**
     * Map source partition to target partition via modulo:
     * {@code source_partition % target_partition_count}.
     *
     * <p>Works across any partition count mismatch. Records from multiple
     * source partitions may coalesce onto the same target partition, which
     * is acceptable when strict partition preservation is not required but
     * key-hash partitioning is also not desired (e.g. the original messages
     * had no keys).
     */
    MODULO
}
