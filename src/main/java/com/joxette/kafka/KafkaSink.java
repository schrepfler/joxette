package com.joxette.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

/**
 * Producer-side Kafka abstraction wrapping a {@link KafkaProducer}.
 *
 * <p>Mirrors the producer utilities from the unpublished
 * {@code com.softwaremill.jox:kafka} module. See {@link ConsumerSettings} for
 * the migration path once that module is published to Maven Central.
 *
 * <p>Business logic (base64 decoding, header encoding, timestamp preservation)
 * stays in the callers; this class is purely an infrastructure lifecycle wrapper.
 *
 * @param <K> record key type
 * @param <V> record value type
 */
public class KafkaSink<K, V> implements AutoCloseable {

    private final KafkaProducer<K, V> producer;

    public KafkaSink(ProducerSettings<K, V> settings) {
        this.producer = new KafkaProducer<>(
                settings.toProperties(),
                settings.keySerializer(),
                settings.valueSerializer());
    }

    /**
     * Sends {@code record} to Kafka asynchronously and returns a future that
     * resolves to the {@link RecordMetadata} once the broker acknowledges.
     */
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return producer.send(record);
    }

    @Override
    public void close() {
        producer.close();
    }
}
