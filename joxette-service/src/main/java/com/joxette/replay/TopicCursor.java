package com.joxette.replay;

import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Base64;
import java.util.Comparator;

/**
 * Opaque pagination cursor for {@code lake.cassette}.
 *
 * <p>Encodes {@code (timestamp, partition, offset)} — the three columns that
 * define the natural sort order after deduplication — as URL-safe base64(JSON).
 * Clients treat the value as opaque; only the service layer decodes it.
 *
 * <p>Natural ordering matches the {@code ORDER BY} clause of the replay query:
 * {@code (timestamp, partition, offset)} ascending.
 */
public record TopicCursor(Instant timestamp, int partition, long offset)
        implements Comparable<TopicCursor> {

    private static final Comparator<TopicCursor> NATURAL =
            Comparator.comparing(TopicCursor::timestamp)
                    .thenComparingInt(TopicCursor::partition)
                    .thenComparingLong(TopicCursor::offset);

    @Override
    public int compareTo(TopicCursor other) {
        return NATURAL.compare(this, other);
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public String encode() {
        try {
            byte[] json = MAPPER.createObjectNode()
                    .put("ts", timestamp.toString())
                    .put("p", partition)
                    .put("o", offset)
                    .toString().getBytes();
            String payload = Base64.getUrlEncoder().withoutPadding().encodeToString(json);
            return CursorSignature.sign(payload);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to encode TopicCursor", e);
        }
    }

    public static TopicCursor decode(String encoded) {
        try {
            String payload = CursorSignature.verify(encoded);
            byte[] bytes = Base64.getUrlDecoder().decode(payload);
            var node = MAPPER.readTree(bytes);
            return new TopicCursor(
                    Instant.parse(node.get("ts").asText()),
                    node.get("p").asInt(),
                    node.get("o").asLong()
            );
        } catch (com.joxette.api.error.InvalidCursorException e) {
            throw e;
        } catch (Exception e) {
            throw new com.joxette.api.error.InvalidCursorException(
                    "Invalid topic cursor: " + encoded, e);
        }
    }
}
