package com.joxette.replay.transform.steps;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.ReplayMessage;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.function.UnaryOperator;

/**
 * Package-private utilities shared by the four time transform steps
 * ({@code wall_time}, {@code time_shift}, {@code time_compress}, {@code time_freeze}).
 */
final class TimeStepHelper {

    private TimeStepHelper() {}

    /**
     * Applies {@code fn} to the timestamp field(s) indicated by {@code target}.
     *
     * <ul>
     *   <li>{@code "ALL_TIMESTAMPS"} — applies to {@link ReplayMessage#timestamp},
     *       {@link ReplayMessage#recordedAt}, and any header value that parses as
     *       an ISO-8601 instant.</li>
     *   <li>{@code "$.timestamp"} — the Kafka producer timestamp only.</li>
     *   <li>{@code "$.recorded_at"} — the cassette ingestion timestamp only.</li>
     *   <li>Anything else — silently skipped.</li>
     * </ul>
     */
    static void applyToTimestampTarget(
            ReplayMessage msg, String target, UnaryOperator<Instant> fn) {
        switch (target) {
            case "ALL_TIMESTAMPS" -> {
                if (msg.timestamp  != null) msg.timestamp  = fn.apply(msg.timestamp);
                if (msg.recordedAt != null) msg.recordedAt = fn.apply(msg.recordedAt);
                shiftTimestampHeaders(msg, fn);
            }
            case "$.timestamp"   -> { if (msg.timestamp  != null) msg.timestamp  = fn.apply(msg.timestamp); }
            case "$.recorded_at" -> { if (msg.recordedAt != null) msg.recordedAt = fn.apply(msg.recordedAt); }
            default              -> { /* unsupported target — silently skip */ }
        }
    }

    /**
     * Scans all headers; for each header whose value parses as an ISO-8601 instant,
     * applies {@code fn} and writes back the result as an ISO-8601 string.
     * Non-timestamp headers are left untouched.
     */
    static void shiftTimestampHeaders(ReplayMessage msg, UnaryOperator<Instant> fn) {
        for (int i = 0; i < msg.headers.size(); i++) {
            CassetteRecord.Header h = msg.headers.get(i);
            if (h.value() == null) continue;
            try {
                Instant ts      = Instant.parse(h.value());
                Instant shifted = fn.apply(ts);
                msg.headers.set(i, new CassetteRecord.Header(h.key(), shifted.toString()));
            } catch (DateTimeParseException ignored) {
                // Not an ISO-8601 timestamp header — leave untouched
            }
        }
    }

    /**
     * Returns the {@link Instant} named by {@code target} from the message envelope,
     * defaulting to {@link ReplayMessage#timestamp} for unrecognised targets.
     * Used by {@code time_compress} to obtain the anchor field value.
     */
    static Instant resolveTimestamp(ReplayMessage msg, String target) {
        return "$.recorded_at".equals(target) ? msg.recordedAt : msg.timestamp;
    }
}
