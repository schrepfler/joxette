package com.joxette.replay.transform;

import com.joxette.replay.CassetteRecord;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Spring component that injects standard replay-provenance headers into every
 * {@link ReplayMessage} before user-defined {@link TransformStep}s run.
 *
 * <p>The six headers injected are:
 * <ul>
 *   <li>{@code x-replay-id}          — UUID of the replay session</li>
 *   <li>{@code x-original-topic}     — topic at record time</li>
 *   <li>{@code x-original-partition} — partition number (decimal string)</li>
 *   <li>{@code x-original-offset}    — offset (decimal string)</li>
 *   <li>{@code x-original-timestamp} — ISO-8601 original Kafka timestamp</li>
 *   <li>{@code x-replayed-at}        — ISO-8601 current wall-clock time</li>
 * </ul>
 *
 * <p>If a header with the same key already exists it is removed and replaced,
 * so the provenance information always reflects the true origin regardless of
 * what the original message contained.
 */
@Component
public class ReplayMetadataInjector {

    private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_INSTANT;

    /**
     * Injects the six standard replay-provenance headers into {@code msg}.
     *
     * @param msg      the mutable message being prepared for the pipeline
     * @param replayId UUID string identifying this replay session
     */
    public void inject(ReplayMessage msg, String replayId) {
        String now = ISO.format(Instant.now());

        setHeader(msg, "x-replay-id",             replayId);
        setHeader(msg, "x-original-topic",        msg.topic);
        setHeader(msg, "x-original-partition",    String.valueOf(msg.partition));
        setHeader(msg, "x-original-offset",       String.valueOf(msg.offset));
        setHeader(msg, "x-original-timestamp",    ISO.format(msg.timestamp));
        setHeader(msg, "x-replayed-at",            now);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Removes any existing header with {@code key}, then appends the new entry. */
    private static void setHeader(ReplayMessage msg, String key, String value) {
        msg.headers.removeIf(h -> key.equals(h.key()));
        msg.headers.add(new CassetteRecord.Header(key, value));
    }
}
