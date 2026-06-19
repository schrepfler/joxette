package com.joxette.replay.sink.kafka;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.EntityRecord;
import com.joxette.replay.sink.RecordSink;
import com.joxette.replay.sink.SinkException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Blocking {@link RecordSink} backed by a Kafka {@link Producer}.
 *
 * <p>Each {@code send} produces a record and blocks on the resulting
 * {@link Future} until the broker acknowledges the write (or fails). On a
 * virtual thread this is cheap; batching is still driven by the producer's
 * {@code linger.ms} / {@code batch.size} settings, so sequential sends do not
 * mean one-record-per-request on the wire.
 *
 * <h2>Encoding</h2>
 * <ul>
 *   <li>Record key: stored as VARCHAR in DuckDB, re-encoded as UTF-8 bytes.</li>
 *   <li>Record value: stored base64url-encoded in DuckDB, decoded back to the
 *       original bytes before producing.</li>
 *   <li>Headers: encoded as UTF-8 bytes per entry.</li>
 *   <li>Timestamp: the source record's Kafka timestamp is carried on the
 *       {@link ProducerRecord} so event-time ordering on the destination
 *       matches the source.</li>
 * </ul>
 *
 * <h2>Lifecycle</h2>
 * <p>This class does <em>not</em> own the {@link Producer}. Callers are
 * responsible for closing it — typically the adapter that created it.
 */
public final class KafkaRecordSink implements RecordSink {

    private static final Logger log = LoggerFactory.getLogger(KafkaRecordSink.class);
    private static final Base64.Decoder BASE64 = Base64.getUrlDecoder();

    private static final long RETRY_INITIAL_MS  = 500;
    private static final double RETRY_MULTIPLIER = 2.0;
    private static final long RETRY_MAX_MS      = 30_000;
    private static final int  RETRY_MAX_ATTEMPTS = 5;

    private final Producer<byte[], byte[]> producer;

    public KafkaRecordSink(Producer<byte[], byte[]> producer) {
        this.producer = producer;
    }

    @Override
    public SendResult send(String targetTopic, CassetteRecord record) {
        return doSend(targetTopic, null, record.key(), record.value(), record.timestamp(),
                      toHeaderEntries(record.headers()));
    }

    @Override
    public SendResult send(String targetTopic, EntityRecord record) {
        return doSend(targetTopic, null, record.key(), record.value(), record.timestamp(),
                      toHeaderEntries(record.headers()));
    }

    @Override
    public SendResult send(String targetTopic, Integer partition, CassetteRecord record) {
        return doSend(targetTopic, partition, record.key(), record.value(), record.timestamp(),
                      toHeaderEntries(record.headers()));
    }

    @Override
    public SendResult send(String targetTopic, Integer partition, EntityRecord record) {
        return doSend(targetTopic, partition, record.key(), record.value(), record.timestamp(),
                      toHeaderEntries(record.headers()));
    }

    @Override
    public void flush() {
        producer.flush();
    }

    private SendResult doSend(String targetTopic,
                              Integer partition,
                              String key, String base64Value,
                              Instant timestamp, List<HeaderEntry> headers) {
        byte[] keyBytes   = key != null ? key.getBytes(StandardCharsets.UTF_8) : null;
        byte[] valueBytes = base64Value != null ? BASE64.decode(base64Value) : null;
        Long   ts         = timestamp != null ? timestamp.toEpochMilli() : null;

        var record = new ProducerRecord<>(
                targetTopic, partition, ts, keyBytes, valueBytes, buildHeaders(headers));

        long delayMs = RETRY_INITIAL_MS;
        int attempt  = 0;
        while (true) {
            try {
                RecordMetadata md = producer.send(record).get();
                Instant resultTs = md.hasTimestamp() ? Instant.ofEpochMilli(md.timestamp()) : timestamp;
                if (attempt > 0) {
                    log.info("Kafka send to '{}' succeeded after {} retries", targetTopic, attempt);
                }
                return new SendResult(md.topic(), md.partition(), md.offset(), resultTs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SinkException("Interrupted during Kafka send to " + targetTopic, e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                if (!(cause instanceof RetriableException) || ++attempt >= RETRY_MAX_ATTEMPTS) {
                    throw new SinkException("Kafka send failed after " + attempt + " attempt(s): "
                            + cause.getMessage(), cause);
                }
                log.warn("Transient Kafka send failure to '{}' (attempt {}/{}), retrying in {} ms: {}",
                        targetTopic, attempt, RETRY_MAX_ATTEMPTS, delayMs, cause.getMessage());
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SinkException("Interrupted during Kafka send retry to " + targetTopic, ie);
                }
                delayMs = Math.min((long) (delayMs * RETRY_MULTIPLIER), RETRY_MAX_MS);
            }
        }
    }

    private static List<HeaderEntry> toHeaderEntries(List<CassetteRecord.Header> headers) {
        if (headers == null || headers.isEmpty()) return List.of();
        return headers.stream()
                .map(h -> new HeaderEntry(h.key(), h.value()))
                .toList();
    }

    private static RecordHeaders buildHeaders(List<HeaderEntry> headers) {
        var out = new RecordHeaders();
        for (var h : headers) {
            if (h.key() == null) continue;
            byte[] val = h.value() != null
                    ? h.value().getBytes(StandardCharsets.UTF_8)
                    : new byte[0];
            out.add(h.key(), val);
        }
        return out;
    }

    private record HeaderEntry(String key, String value) {}
}
