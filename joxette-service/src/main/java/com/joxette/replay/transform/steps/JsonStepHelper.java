package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.joxette.replay.transform.ReplayMessage;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Package-private utilities shared by JSON-manipulating transform steps.
 *
 * <h2>Message wrapper</h2>
 * <p>All JSONPath expressions in structural / JSON steps are evaluated against
 * a <em>wrapper</em> {@link ObjectNode} that exposes the full message structure:
 * <pre>
 *   {
 *     "topic":      String,
 *     "partition":  int,
 *     "offset":     long,
 *     "timestamp":  ISO-8601 String,
 *     "recordedAt": ISO-8601 String,
 *     "key":        String | null   — Kafka key decoded as UTF-8,
 *     "value":      Object | null   — Kafka value decoded as JSON
 *   }
 * </pre>
 *
 * <p>After transformation, {@link #applyWrapper} writes {@code $.key} and
 * {@code $.value} back into the mutable {@link ReplayMessage}.
 *
 * <h2>Path navigation</h2>
 * <p>{@link #parentAndLeaf} navigates simple dot-notation paths
 * (e.g. {@code "$.value.address.city"}) to the parent {@link ObjectNode}
 * and returns the leaf key name. Bracket notation, array indices and wildcards
 * are not supported.
 */
final class JsonStepHelper {

    static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Base64.Decoder DEC = Base64.getUrlDecoder();
    private static final Base64.Encoder ENC = Base64.getUrlEncoder().withoutPadding();

    private JsonStepHelper() {}

    // -------------------------------------------------------------------------
    // Wrapper build / extract
    // -------------------------------------------------------------------------

    /**
     * Builds a wrapper {@link ObjectNode} from the mutable message.
     *
     * <p>If the message value is not valid JSON (or is null), {@code $.value}
     * is set to either {@code null} or a text node containing the raw string.
     * JSON-path navigation into a text node will fail with
     * {@link IllegalArgumentException}, which callers should treat as a no-op.
     */
    static ObjectNode buildWrapper(ReplayMessage msg) {
        ObjectNode w = MAPPER.createObjectNode();
        w.put("topic",      msg.topic);
        w.put("partition",  msg.partition);
        w.put("offset",     msg.offset);
        w.put("timestamp",  msg.timestamp  != null ? msg.timestamp.toString()  : null);
        w.put("recordedAt", msg.recordedAt != null ? msg.recordedAt.toString() : null);

        if (msg.key != null) {
            w.put("key", new String(DEC.decode(msg.key), StandardCharsets.UTF_8));
        } else {
            w.putNull("key");
        }

        if (msg.value != null) {
            try {
                w.set("value", MAPPER.readTree(DEC.decode(msg.value)));
            } catch (Exception e) {
                // Non-JSON payload — store as opaque text node; path navigation
                // into it will throw IllegalArgumentException (handled by callers)
                w.put("value", new String(DEC.decode(msg.value), StandardCharsets.UTF_8));
            }
        } else {
            w.putNull("value");
        }

        return w;
    }

    /**
     * Writes the {@code $.key} and {@code $.value} fields of the wrapper back
     * into the mutable {@link ReplayMessage}, re-encoding them as base64url.
     */
    static void applyWrapper(ObjectNode wrapper, ReplayMessage msg) {
        JsonNode keyNode = wrapper.get("key");
        if (keyNode == null || keyNode.isNull()) {
            msg.key = null;
        } else {
            msg.key = ENC.encodeToString(
                    keyNode.asText().getBytes(StandardCharsets.UTF_8));
        }

        JsonNode valueNode = wrapper.get("value");
        if (valueNode == null || valueNode.isNull()) {
            msg.value = null;
        } else {
            try {
                msg.value = ENC.encodeToString(MAPPER.writeValueAsBytes(valueNode));
            } catch (JsonProcessingException e) {
                throw new IllegalStateException(
                        "Failed to re-encode value after transformation", e);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Path navigation
    // -------------------------------------------------------------------------

    /**
     * Navigates a dot-notation JSONPath to the parent {@link ObjectNode} and
     * returns a two-element array {@code [parentNode, leafKey]}.
     *
     * <p>Example: {@code "$.value.address.city"} → the {@code ObjectNode} at
     * {@code "$.value.address"} and the string {@code "city"}.
     *
     * <p>The path must begin with {@code "$."} and use only literal dot-separated
     * key names — no bracket notation, no array indices, no wildcards.
     *
     * @throws IllegalArgumentException if the path does not start with {@code "$."},
     *         if an intermediate segment is missing, or if an intermediate node is
     *         not an {@link ObjectNode}
     */
    static Object[] parentAndLeaf(ObjectNode root, String path) {
        if (!path.startsWith("$.")) {
            throw new IllegalArgumentException(
                    "JSONPath must start with '$.' — got: " + path);
        }
        String[] segments = path.substring(2).split("\\.", -1);
        ObjectNode current = root;
        for (int i = 0; i < segments.length - 1; i++) {
            JsonNode child = current.get(segments[i]);
            if (child == null || !child.isObject()) {
                throw new IllegalArgumentException(
                        "Cannot navigate path '" + path + "': segment '"
                        + segments[i] + "' is missing or not a JSON object");
            }
            current = (ObjectNode) child;
        }
        return new Object[]{current, segments[segments.length - 1]};
    }

    // -------------------------------------------------------------------------
    // Key encode/decode
    // -------------------------------------------------------------------------

    /** Encodes a plain string as a base64url Kafka key (no padding). */
    static String encodeKey(String plaintext) {
        return ENC.encodeToString(plaintext.getBytes(StandardCharsets.UTF_8));
    }
}
