package com.joxette.replay.sink;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.EntityRecord;

import java.time.Instant;

/**
 * Sends cassette records back to some downstream destination (typically a Kafka
 * topic, but anything that can accept a {@link CassetteRecord} or
 * {@link EntityRecord}).
 *
 * <p>This is the reusable seam that lets {@code ReplayEngine} run both inside
 * the Joxette service (against a live Kafka cluster via
 * {@code KafkaRecordSink}) and inside tests (against a capturing sink, a
 * Testcontainers broker, or any other transport) without any code in the engine
 * knowing about Kafka.
 *
 * <h2>Blocking contract</h2>
 * <p>Each {@code send} returns only once the destination has acknowledged the
 * write. Virtual-thread friendly: callers run on a virtual thread and the block
 * is cheap. Use {@link SinkException} to signal transport failure — the engine
 * rethrows it to abort a replay run.
 *
 * <h2>Lifecycle</h2>
 * <p>Implementations are usually shared across many replay runs. Ownership of
 * the underlying resource (e.g. the Kafka producer) lives with whoever created
 * the sink, not with callers — which is why {@link #close()} is a no-op by
 * default.
 */
public interface RecordSink extends AutoCloseable {

    SendResult send(String targetTopic, CassetteRecord record);

    SendResult send(String targetTopic, EntityRecord record);

    /**
     * Sends a cassette record to the given topic and partition.
     *
     * <p>When {@code partition} is {@code null} the behaviour is identical to
     * {@link #send(String, CassetteRecord)}: the transport chooses the partition
     * (e.g. Kafka's default key-hash partitioner).  A non-null value requests
     * that the record be placed in exactly that partition — implementations that
     * do not support explicit partition assignment may ignore it and fall back to
     * the no-partition overload.
     */
    default SendResult send(String targetTopic, Integer partition, CassetteRecord record) {
        return send(targetTopic, record);
    }

    /**
     * Sends an entity record to the given topic and partition.
     *
     * <p>Same semantics as {@link #send(String, Integer, CassetteRecord)}.
     */
    default SendResult send(String targetTopic, Integer partition, EntityRecord record) {
        return send(targetTopic, record);
    }

    default void flush() {}

    @Override
    default void close() {}

    /**
     * Result of a successful send. {@code partition} / {@code offset} reflect
     * the destination; {@code timestamp} is the source record timestamp that
     * was preserved on the wire.
     */
    record SendResult(String topic, int partition, long offset, Instant timestamp) {}
}
