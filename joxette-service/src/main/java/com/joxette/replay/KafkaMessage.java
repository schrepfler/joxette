package com.joxette.replay;

import java.util.List;

/**
 * Immutable snapshot of a single Kafka record consumed from a topic.
 *
 * <p>Headers mirror the Kafka wire format: a list of key/value pairs where
 * keys are UTF-8 strings and values are raw bytes. Duplicate keys are allowed
 * and preserved in insertion order, consistent with the Kafka protocol.
 *
 * <p>{@code timestampMs} is the Kafka record timestamp in milliseconds since
 * the Unix epoch (CreateTime or LogAppendTime, depending on topic config).
 */
public record KafkaMessage(
        String topic,
        int partition,
        long offset,
        long timestampMs,
        String key,
        byte[] value,
        List<Header> headers
) {

    public record Header(String key, byte[] value) {}
}
