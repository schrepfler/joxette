package com.joxette.replay;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Base64;
import java.util.Comparator;

/**
 * Opaque pagination cursor for {@code lake.entity_{type}}.
 *
 * <p>Encodes {@code (timestamp, recorded_at, source_topic, source_partition,
 * source_offset)} — the five columns that define the natural sort order of an
 * entity cassette after deduplication — as URL-safe base64(JSON).
 *
 * <p>Natural ordering matches the {@code ORDER BY} clause of the replay query:
 * {@code (timestamp, recorded_at, source_topic, source_partition, source_offset)}
 * ascending.
 */
public record EntityCursor(
        Instant timestamp,
        Instant recordedAt,
        String sourceTopic,
        int sourcePartition,
        long sourceOffset
) implements Comparable<EntityCursor> {

    private static final Comparator<EntityCursor> NATURAL =
            Comparator.comparing(EntityCursor::timestamp)
                    .thenComparing(EntityCursor::recordedAt)
                    .thenComparing(EntityCursor::sourceTopic)
                    .thenComparingInt(EntityCursor::sourcePartition)
                    .thenComparingLong(EntityCursor::sourceOffset);

    @Override
    public int compareTo(EntityCursor other) {
        return NATURAL.compare(this, other);
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public String encode() {
        try {
            byte[] json = MAPPER.createObjectNode()
                    .put("ts", timestamp.toString())
                    .put("ra", recordedAt.toString())
                    .put("t", sourceTopic)
                    .put("p", sourcePartition)
                    .put("o", sourceOffset)
                    .toString().getBytes();
            return Base64.getUrlEncoder().withoutPadding().encodeToString(json);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to encode EntityCursor", e);
        }
    }

    public static EntityCursor decode(String encoded) {
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(encoded);
            var node = MAPPER.readTree(bytes);
            return new EntityCursor(
                    Instant.parse(node.get("ts").asText()),
                    Instant.parse(node.get("ra").asText()),
                    node.get("t").asText(),
                    node.get("p").asInt(),
                    node.get("o").asLong()
            );
        } catch (Exception e) {
            throw new com.joxette.api.error.InvalidCursorException(
                    "Invalid entity cursor: " + encoded, e);
        }
    }
}
