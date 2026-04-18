package com.joxette.testkit;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.EntityRecord;
import com.joxette.replay.sink.RecordSink;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Non-Kafka {@link RecordSink} that captures every {@code send} call in order.
 * Useful for asserting engine behaviour — ordering, inter-message delay, transform
 * effects — without spinning up a Kafka broker.
 */
public final class CapturingRecordSink implements RecordSink {

    private final List<SentRecord> sent = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong partitionSeq = new AtomicLong();

    @Override
    public SendResult send(String targetTopic, CassetteRecord record) {
        sent.add(new SentRecord(targetTopic, record.key(), record.value(), record.timestamp(),
                                record.headers().size(), Instant.now()));
        return new SendResult(targetTopic, 0, partitionSeq.getAndIncrement(), record.timestamp());
    }

    @Override
    public SendResult send(String targetTopic, EntityRecord record) {
        sent.add(new SentRecord(targetTopic, record.key(), record.value(), record.timestamp(),
                                record.headers() == null ? 0 : record.headers().size(),
                                Instant.now()));
        return new SendResult(targetTopic, 0, partitionSeq.getAndIncrement(), record.timestamp());
    }

    /** Unmodifiable snapshot of records sent so far, in send order. */
    public List<SentRecord> sent() {
        return List.copyOf(sent);
    }

    public int sentCount() {
        return sent.size();
    }

    /**
     * A single record captured by {@link CapturingRecordSink}.
     * {@code receivedAt} is wall-clock time at the moment of capture, useful for
     * asserting inter-message delay behaviour.
     */
    public record SentRecord(
            String  targetTopic,
            String  key,
            String  value,
            Instant sourceTimestamp,
            int     headerCount,
            Instant receivedAt
    ) {}
}
