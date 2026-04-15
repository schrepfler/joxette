package com.joxette.replay;

import com.joxette.config.BrokerConnectionFactory;
import com.joxette.management.BrokerConfig;
import com.softwaremill.jox.kafka.ProducerSettings;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

/**
 * Wraps a {@link KafkaProducer KafkaProducer&lt;byte[], byte[]&gt;} for replay-to-topic operations.
 *
 * <p>Both {@link CassetteRecord} and {@link EntityRecord} carry their payload as
 * base64url-encoded strings (value field) and plain-string keys. This service
 * decodes them back to raw bytes before producing so the target topic receives
 * the original wire-format bytes.
 *
 * <p>Original Kafka timestamps are preserved: each {@link ProducerRecord} is
 * constructed with the source record's {@code timestamp} so that downstream
 * consumers see event-time ordering identical to the original topic.
 *
 * <p>The producer is configured with {@code acks=all} and idempotence enabled
 * so that every record is durably written exactly once to the target topic.
 */
@Service
public class KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);
    private static final Base64.Decoder BASE64 = Base64.getUrlDecoder();

    private final BrokerConnectionFactory factory;
    private final ConcurrentHashMap<String, KafkaProducer<byte[], byte[]>> producers =
            new ConcurrentHashMap<>();

    public KafkaProducerService(BrokerConnectionFactory factory) {
        this.factory = factory;
    }

    private KafkaProducer<byte[], byte[]> producerFor(String brokerId) {
        String id = brokerId != null ? brokerId : BrokerConfig.DEFAULT_BROKER_ID;
        return producers.computeIfAbsent(id, k -> {
            ProducerSettings<byte[], byte[]> settings = factory.producerSettings(k);
            return settings.toProducer();
        });
    }

    /**
     * Sends a {@link CassetteRecord} to {@code targetTopic}, preserving the
     * original Kafka timestamp.
     *
     * <p>The record key is encoded as UTF-8 bytes (it is stored as VARCHAR in DuckDB).
     * The record value is decoded from base64url to raw bytes.
     * Header values are encoded as UTF-8 bytes.
     *
     * @return a {@link Future} that resolves to the {@link RecordMetadata} once
     *         the broker acknowledges the write
     */
    public Future<RecordMetadata> send(String targetTopic, CassetteRecord record) {
        byte[] keyBytes   = record.key()   != null ? record.key().getBytes(StandardCharsets.UTF_8)   : null;
        byte[] valueBytes = record.value() != null ? BASE64.decode(record.value()) : null;
        long   timestamp  = record.timestamp().toEpochMilli();
        var pr = new ProducerRecord<>(targetTopic, null, timestamp, keyBytes, valueBytes,
                buildHeaders(record.headers()));
        return producerFor(null).send(pr);
    }

    /**
     * Sends an {@link EntityRecord} to {@code targetTopic}, preserving the
     * original Kafka timestamp.
     */
    public Future<RecordMetadata> send(String targetTopic, EntityRecord record) {
        byte[] keyBytes   = record.key()   != null ? record.key().getBytes(StandardCharsets.UTF_8)   : null;
        byte[] valueBytes = record.value() != null ? BASE64.decode(record.value()) : null;
        long   timestamp  = record.timestamp().toEpochMilli();
        var pr = new ProducerRecord<>(targetTopic, null, timestamp, keyBytes, valueBytes,
                buildHeaders(record.headers()));
        return producerFor(null).send(pr);
    }

    private static RecordHeaders buildHeaders(List<CassetteRecord.Header> headerList) {
        var headers = new RecordHeaders();
        if (headerList != null) {
            for (var h : headerList) {
                if (h.key() != null) {
                    byte[] val = h.value() != null
                            ? h.value().getBytes(StandardCharsets.UTF_8)
                            : new byte[0];
                    headers.add(h.key(), val);
                }
            }
        }
        return headers;
    }

    @PreDestroy
    public void close() {
        log.info("KafkaProducerService closing {} producer(s)", producers.size());
        producers.values().forEach(KafkaProducer::close);
        log.info("KafkaProducerService closed");
    }
}
