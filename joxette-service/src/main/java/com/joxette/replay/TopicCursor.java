package com.joxette.replay;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Base64;

/**
 * Opaque pagination cursor for {@code lake.cassette}.
 *
 * <p>Encodes {@code (timestamp, partition, offset)} — the three columns that
 * define the natural sort order after deduplication — as URL-safe base64(JSON).
 * Clients treat the value as opaque; only the service layer decodes it.
 */
public record TopicCursor(Instant timestamp, int partition, long offset) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public String encode() {
        try {
            byte[] json = MAPPER.createObjectNode()
                    .put("ts", timestamp.toString())
                    .put("p", partition)
                    .put("o", offset)
                    .toString().getBytes();
            return Base64.getUrlEncoder().withoutPadding().encodeToString(json);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to encode TopicCursor", e);
        }
    }

    public static TopicCursor decode(String encoded) {
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(encoded);
            var node = MAPPER.readTree(bytes);
            return new TopicCursor(
                    Instant.parse(node.get("ts").asText()),
                    node.get("p").asInt(),
                    node.get("o").asLong()
            );
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid TopicCursor: " + encoded, e);
        }
    }
}
