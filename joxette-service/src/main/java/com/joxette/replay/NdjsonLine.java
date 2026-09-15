package com.joxette.replay;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.annotation.JsonSerialize;
import tools.jackson.databind.ser.std.StdSerializer;

import java.util.Map;

/**
 * Internal wire-line union for {@link SseReplayHandler}'s NDJSON streaming methods.
 *
 * <p>Wraps a replay record, a control event (preamble/follow/heartbeat/overflow/
 * scheduled/cancelled), or a mid-stream error frame, so all three can flow through
 * one jox-json {@code Flow<NdjsonLine>} while still rendering as flat, unwrapped
 * JSON lines identical to the hand-written NDJSON output this replaces.
 * {@code NdjsonLine} itself is never visible in the JSON — {@link Serializer}
 * unwraps to whichever variant's payload is present.
 */
@JsonSerialize(using = NdjsonLine.Serializer.class)
sealed interface NdjsonLine {

    /** A normal replay record of type {@code T}. */
    record RecordLine<T>(T value) implements NdjsonLine {}

    /** A non-record control line: preamble, follow, heartbeat, overflow, scheduled, cancelled. */
    record ControlEvent(Map<String, Object> payload) implements NdjsonLine {}

    /** The terminal error frame — {@code payload} is already the {@code {"_error": {...}}} wrapper map. */
    record ErrorFrame(Map<String, Object> payload) implements NdjsonLine {}

    final class Serializer extends StdSerializer<NdjsonLine> {

        Serializer() {
            super(NdjsonLine.class);
        }

        @Override
        public void serialize(NdjsonLine line, JsonGenerator gen, SerializationContext provider) {
            Object payload = switch (line) {
                case RecordLine<?> r -> r.value();
                case ControlEvent c -> c.payload();
                case ErrorFrame e -> e.payload();
            };
            provider.writeTree(gen, provider.valueToTree(payload));
        }
    }
}
