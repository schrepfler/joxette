package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformStep;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.regex.Pattern;

/**
 * Drops the message from the replay stream when the field predicate evaluates to
 * {@code true}. When the predicate does NOT match, the message passes through unchanged.
 *
 * <p>Top-level message fields (topic, partition, offset, timestamp, key, recorded_at)
 * are matched directly. Nested JSON fields under {@code $.value.*} are matched by
 * decoding the base64 value and evaluating the remaining JSONPath.
 *
 * <p>The {@link Operator#MATCHES} operator pre-compiles the regex {@link Pattern}
 * at construction time.
 *
 * <p>Example — drop messages from partition 3:
 * <pre>{@code
 * {"type": "filter_drop", "field": "$.partition", "operator": "EQ", "value": 3}
 * }</pre>
 *
 * <p>Example — drop messages whose status field is "cancelled":
 * <pre>{@code
 * {"type": "filter_drop", "field": "$.value.status", "operator": "EQ", "value": "cancelled"}
 * }</pre>
 */
public final class FilterDropStep implements TransformStep {

    /** Comparison operators for the filter predicate. */
    public enum Operator {
        /** Drop when field equals value. */
        EQ,
        /** Drop when field does not equal value. */
        NEQ,
        /** Drop when field is greater than value. */
        GT,
        /** Drop when field is greater than or equal to value. */
        GTE,
        /** Drop when field is less than value. */
        LT,
        /** Drop when field is less than or equal to value. */
        LTE,
        /** Drop when field (as string) contains value. */
        CONTAINS,
        /** Drop when field (as string) matches regex value. Pattern pre-compiled at construction. */
        MATCHES,
        /** Drop when field is null. No {@code value} needed. */
        IS_NULL,
        /** Drop when field is non-null. No {@code value} needed. */
        IS_NOT_NULL
    }

    private static final Base64.Decoder B64_DEC = Base64.getUrlDecoder();

    private final String   field;
    private final Operator operator;
    private final Object   value;           // String, Number, or null (from JSON)
    private final Pattern  compiledPattern; // non-null only for MATCHES

    @JsonCreator
    public FilterDropStep(
            @JsonProperty("field")    String   field,
            @JsonProperty("operator") Operator operator,
            @JsonProperty("value")    Object   value
    ) {
        this.field    = field;
        this.operator = operator;
        this.value    = value;
        this.compiledPattern =
                (operator == Operator.MATCHES && value instanceof String s)
                ? Pattern.compile(s) : null;
    }

    public String   field()           { return field; }
    public Operator operator()        { return operator; }
    public Object   value()           { return value; }
    /** Non-null only when operator is {@link Operator#MATCHES}. */
    public Pattern  compiledPattern() { return compiledPattern; }

    // -------------------------------------------------------------------------
    // Predicate evaluation
    // -------------------------------------------------------------------------

    /**
     * Returns {@code true} if the predicate matches {@code msg} (i.e. the message
     * should be dropped).
     */
    public boolean test(ReplayMessage msg) {
        Object extracted = extractField(msg);
        return evaluate(extracted);
    }

    // -------------------------------------------------------------------------
    // Field extraction
    // -------------------------------------------------------------------------

    private Object extractField(ReplayMessage msg) {
        return switch (field) {
            case "$.topic"       -> msg.topic;
            case "$.partition"   -> msg.partition;
            case "$.offset"      -> msg.offset;
            case "$.timestamp"   -> msg.timestamp;
            case "$.key"         -> msg.key;
            case "$.recorded_at" -> msg.recordedAt;
            default              -> extractFromValue(msg);
        };
    }

    private Object extractFromValue(ReplayMessage msg) {
        if (!field.startsWith("$.value") || msg.value == null) return null;
        try {
            byte[] raw  = B64_DEC.decode(msg.value);
            String json = new String(raw, StandardCharsets.UTF_8);
            if (field.equals("$.value")) return json;
            // $.value.foo.bar  →  $.foo.bar
            String jsonPath = "$" + field.substring("$.value".length());
            return JsonPath.read(json, jsonPath);
        } catch (PathNotFoundException e) {
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    // -------------------------------------------------------------------------
    // Evaluation
    // -------------------------------------------------------------------------

    private boolean evaluate(Object extracted) {
        return switch (operator) {
            case IS_NULL     -> extracted == null;
            case IS_NOT_NULL -> extracted != null;
            case EQ          -> compareEq(extracted, value);
            case NEQ         -> !compareEq(extracted, value);
            case GT          -> compareOrder(extracted, value) > 0;
            case GTE         -> compareOrder(extracted, value) >= 0;
            case LT          -> compareOrder(extracted, value) < 0;
            case LTE         -> compareOrder(extracted, value) <= 0;
            case CONTAINS    -> containsCheck(extracted, value);
            case MATCHES     -> matchesCheck(extracted);
        };
    }

    private static boolean compareEq(Object extracted, Object configured) {
        if (extracted == null && configured == null) return true;
        if (extracted == null || configured == null) return false;
        if (extracted instanceof Number n1 && configured instanceof Number n2) {
            return n1.doubleValue() == n2.doubleValue();
        }
        if (extracted instanceof Number n1 && configured instanceof String s) {
            try { return n1.doubleValue() == Double.parseDouble(s); }
            catch (NumberFormatException ignored) { /* fall through */ }
        }
        if (extracted instanceof Instant ts && configured instanceof String s) {
            try { return ts.equals(Instant.parse(s)); }
            catch (Exception ignored) { /* fall through */ }
        }
        return extracted.toString().equals(configured.toString());
    }

    @SuppressWarnings("unchecked")
    private static int compareOrder(Object extracted, Object configured) {
        if (extracted == null || configured == null) return 0;
        if (extracted instanceof Number n1 && configured instanceof Number n2) {
            return Double.compare(n1.doubleValue(), n2.doubleValue());
        }
        if (extracted instanceof Number n1 && configured instanceof String s) {
            try { return Double.compare(n1.doubleValue(), Double.parseDouble(s)); }
            catch (NumberFormatException ignored) { return 0; }
        }
        if (extracted instanceof Instant ts && configured instanceof String s) {
            try { return ts.compareTo(Instant.parse(s)); }
            catch (Exception ignored) { return 0; }
        }
        if (extracted instanceof Comparable c
                && extracted.getClass().isInstance(configured)) {
            return c.compareTo(extracted.getClass().cast(configured));
        }
        return extracted.toString().compareTo(configured.toString());
    }

    private static boolean containsCheck(Object extracted, Object configured) {
        if (extracted == null || configured == null) return false;
        return extracted.toString().contains(configured.toString());
    }

    private boolean matchesCheck(Object extracted) {
        if (extracted == null || compiledPattern == null) return false;
        return compiledPattern.matcher(extracted.toString()).find();
    }
}
