package com.joxette.replay.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Package-private utility for reading from and writing to a {@link ReplayMessage}
 * using JSONPath-style expressions.
 *
 * <p>Supported path prefixes and their message-field mappings:
 * <dl>
 *   <dt>{@code $.topic}</dt>      <dd>message topic string</dd>
 *   <dt>{@code $.partition}</dt>  <dd>partition number (int)</dd>
 *   <dt>{@code $.offset}</dt>     <dd>offset (long)</dd>
 *   <dt>{@code $.key}</dt>        <dd>base64url-decoded message key as UTF-8 string</dd>
 *   <dt>{@code $.value}</dt>      <dd>the whole decoded JSON value payload</dd>
 *   <dt>{@code $.value.*}</dt>    <dd>fields within the decoded JSON value payload</dd>
 * </dl>
 *
 * <p>All errors (missing path, non-JSON value body, decode failures) are swallowed
 * and represented as {@code null} returns or no-op writes, consistent with the
 * behaviour of {@link com.joxette.replay.MessageTransformer}.
 */
public final class MessageJsonPath {

    static final Base64.Decoder DECODER = Base64.getUrlDecoder();
    static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private MessageJsonPath() {}

    // -------------------------------------------------------------------------
    // Read
    // -------------------------------------------------------------------------

    /**
     * Reads the value at {@code path} from {@code msg}. Returns {@code null}
     * when the path points to an absent field, a non-JSON value body, or an
     * unmapped top-level segment.
     *
     * @param msg  the message to read from
     * @param path JSONPath expression (e.g. {@code $.value.order_id}, {@code $.partition})
     * @return the value at the path, or {@code null} if not reachable
     */
    public static Object read(ReplayMessage msg, String path) {
        return switch (path) {
            case "$.topic"     -> msg.topic;
            case "$.partition" -> msg.partition;
            case "$.offset"    -> msg.offset;
            case "$.key"       -> decodeKeyString(msg.key);
            default -> {
                if (path.equals("$.value") || path.startsWith("$.value.")) {
                    yield readFromValueJson(msg, path);
                }
                yield null;
            }
        };
    }

    // -------------------------------------------------------------------------
    // Write
    // -------------------------------------------------------------------------

    /**
     * Writes {@code value} to the field at {@code path} in {@code msg}.
     * Returns {@code true} if the write succeeded, {@code false} if the path
     * is not reachable (e.g. non-JSON body, absent nested path).
     *
     * @param msg   the message to mutate
     * @param path  JSONPath expression targeting the field to set
     * @param value the value to write; {@code null} sets the field to JSON null
     * @return {@code true} on success, {@code false} when the write was skipped
     */
    public static boolean write(ReplayMessage msg, String path, Object value) {
        return switch (path) {
            case "$.topic" -> {
                msg.topic = String.valueOf(value);
                yield true;
            }
            case "$.key" -> {
                msg.key = value != null ? encodeKeyString(String.valueOf(value)) : null;
                yield true;
            }
            default -> {
                if (path.equals("$.value") || path.startsWith("$.value.")) {
                    yield writeToValueJson(msg, path, value);
                }
                yield false;
            }
        };
    }

    // -------------------------------------------------------------------------
    // JSON node conversion
    // -------------------------------------------------------------------------

    /**
     * Converts a Jackson {@link JsonNode} to a plain Java value suitable for
     * passing to JsonPath write operations.
     *
     * @param node the Jackson node; may be null
     * @return {@code null} for null/NullNode, or the appropriate Java primitive /
     *         {@code Map} / {@code List} for other node types
     */
    public static Object toNativeValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isTextual()) {
            return node.asText();
        }
        if (node.isBoolean()) {
            return node.asBoolean();
        }
        if (node.isIntegralNumber()) {
            long l = node.asLong();
            return (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) ? (int) l : l;
        }
        if (node.isFloatingPointNumber()) {
            return node.asDouble();
        }
        try {
            return OBJECT_MAPPER.treeToValue(node, Object.class);
        } catch (Exception e) {
            return node.toString();
        }
    }

    // -------------------------------------------------------------------------
    // SHA-256
    // -------------------------------------------------------------------------

    /**
     * Returns the lower-case hex SHA-256 digest of {@code input} (UTF-8 bytes).
     *
     * @throws IllegalStateException if SHA-256 is unavailable in this JVM
     */
    public static String sha256Hex(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                sb.append(String.format("%02x", b & 0xff));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private static Object readFromValueJson(ReplayMessage msg, String path) {
        if (msg.value == null) {
            return null;
        }
        String innerPath = toInnerPath(path);
        try {
            byte[] bytes = DECODER.decode(msg.value);
            String json = new String(bytes, StandardCharsets.UTF_8);
            return JsonPath.read(json, innerPath);
        } catch (PathNotFoundException e) {
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean writeToValueJson(ReplayMessage msg, String path, Object value) {
        String innerPath = toInnerPath(path);
        String json;
        if (msg.value == null) {
            json = "{}";
        } else {
            try {
                byte[] bytes = DECODER.decode(msg.value);
                json = new String(bytes, StandardCharsets.UTF_8);
            } catch (Exception e) {
                return false;
            }
        }
        try {
            DocumentContext ctx = JsonPath.parse(json);
            try {
                ctx.set(innerPath, value);
            } catch (PathNotFoundException e) {
                // Path doesn't exist — create as a direct child of the nearest existing parent.
                // Only supported for simple one-segment terminal paths (e.g. $.env).
                int lastDot = innerPath.lastIndexOf('.');
                if (lastDot > 0) {
                    String parentPath = innerPath.substring(0, lastDot);
                    String fieldName  = innerPath.substring(lastDot + 1);
                    try {
                        ctx.put(parentPath, fieldName, value);
                    } catch (Exception ignored) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            msg.value = ENCODER.encodeToString(
                    ctx.jsonString().getBytes(StandardCharsets.UTF_8));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Converts a message-level path (e.g. {@code $.value.order_id}) to an
     * inner-document path (e.g. {@code $.order_id}) by stripping the
     * {@code $.value} prefix.
     *
     * <p>The root path {@code $.value} maps to {@code "$"}.
     */
    static String toInnerPath(String messagePath) {
        // $.value → $
        // $.value.x → $.x
        return "$" + messagePath.substring("$.value".length());
    }

    private static String decodeKeyString(String b64Key) {
        if (b64Key == null) {
            return null;
        }
        try {
            return new String(DECODER.decode(b64Key), StandardCharsets.UTF_8);
        } catch (Exception e) {
            return b64Key;
        }
    }

    private static String encodeKeyString(String value) {
        if (value == null) {
            return null;
        }
        return ENCODER.encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}
