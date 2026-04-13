package com.joxette.replay.transform;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Shared predicate evaluation logic used by {@link TransformPipeline} (per-step {@code when}
 * guard), {@link com.joxette.replay.transform.steps.FilterDropStep}, and
 * {@link com.joxette.replay.transform.steps.ConditionalStep}.
 *
 * <p>The top-level entry point is {@link #evaluate(Predicate, ReplayMessage)}, which
 * dispatches recursively over the sealed {@link Predicate} hierarchy.
 *
 * <h2>Supported field paths</h2>
 * <ul>
 *   <li>{@code $.topic}, {@code $.partition}, {@code $.offset}, {@code $.timestamp},
 *       {@code $.key}, {@code $.recorded_at} — top-level message fields</li>
 *   <li>{@code $.value}, {@code $.value.foo.bar} — base64url-decode the message value
 *       and evaluate the remainder as a JSONPath expression</li>
 *   <li>{@code $.headers[key]} — first header value whose key matches {@code key};
 *       returns the raw header value string (no additional decoding)</li>
 * </ul>
 */
public final class PredicateEvaluator {

    private static final Base64.Decoder B64_DEC     = Base64.getUrlDecoder();
    private static final Pattern        HEADER_PATH = Pattern.compile("^\\$\\.headers\\[(.+)]$");

    private PredicateEvaluator() {}

    // -------------------------------------------------------------------------
    // Top-level Predicate evaluation (sealed hierarchy)
    // -------------------------------------------------------------------------

    /**
     * Evaluates any {@link Predicate} recursively against {@code msg}.
     *
     * @param predicate the predicate to evaluate; must not be {@code null}
     * @param msg       the message to test
     * @return {@code true} when the predicate matches the message
     */
    public static boolean evaluate(Predicate predicate, ReplayMessage msg) {
        return switch (predicate) {
            case Predicate.Leaf leaf -> evaluateLeaf(leaf, msg);
            case Predicate.And and   -> and.predicates().stream().allMatch(p -> evaluate(p, msg));
            case Predicate.Or  or    -> or.predicates().stream().anyMatch(p -> evaluate(p, msg));
            case Predicate.Not not   -> !evaluate(not.predicate(), msg);
        };
    }

    private static boolean evaluateLeaf(Predicate.Leaf leaf, ReplayMessage msg) {
        Object  extracted = extractField(leaf.field(), msg);
        Pattern pattern   = (leaf.operator() == Predicate.Operator.MATCHES
                             && leaf.value() instanceof String s)
                            ? Pattern.compile(s) : null;
        return evaluate(extracted, leaf.operator(), leaf.value(), pattern);
    }

    // -------------------------------------------------------------------------
    // Public API — low-level evaluation (kept for callers that pre-extract)
    // -------------------------------------------------------------------------

    /**
     * Extracts a field value from {@code msg} using the JSONPath-like {@code field}
     * expression described in the class Javadoc.
     *
     * @return the extracted value, or {@code null} when the path is absent / unresolvable
     */
    public static Object extractField(String field, ReplayMessage msg) {
        return switch (field) {
            case "$.topic"       -> msg.topic;
            case "$.partition"   -> msg.partition;
            case "$.offset"      -> msg.offset;
            case "$.timestamp"   -> msg.timestamp;
            case "$.key"         -> msg.key;
            case "$.recorded_at" -> msg.recordedAt;
            default -> {
                if (field.startsWith("$.value")) {
                    yield extractFromValue(field, msg);
                }
                Matcher m = HEADER_PATH.matcher(field);
                if (m.matches()) {
                    yield extractFromHeaders(m.group(1), msg);
                }
                yield null;
            }
        };
    }

    /**
     * Evaluates the predicate against an already-extracted value.
     *
     * @param extracted       value extracted from the message (may be {@code null})
     * @param operator        comparison operator
     * @param value           configured comparison value
     * @param compiledPattern pre-compiled regex; non-null only for
     *                        {@link Predicate.Operator#MATCHES}
     * @return {@code true} when the predicate matches
     */
    public static boolean evaluate(Object extracted,
                                   Predicate.Operator operator,
                                   Object value,
                                   Pattern compiledPattern) {
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
            case MATCHES     -> matchesCheck(extracted, compiledPattern);
        };
    }

    // -------------------------------------------------------------------------
    // Field extraction helpers
    // -------------------------------------------------------------------------

    private static Object extractFromValue(String field, ReplayMessage msg) {
        if (msg.value == null) return null;
        try {
            byte[] raw  = B64_DEC.decode(msg.value);
            String json = new String(raw, StandardCharsets.UTF_8);
            if (field.equals("$.value")) return json;
            // $.value.foo.bar → $.foo.bar
            String jsonPath = "$" + field.substring("$.value".length());
            return JsonPath.read(json, jsonPath);
        } catch (PathNotFoundException e) {
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    private static Object extractFromHeaders(String key, ReplayMessage msg) {
        if (msg.headers == null) return null;
        return msg.headers.stream()
                .filter(h -> key.equals(h.key()))
                .map(h -> h.value())
                .findFirst()
                .orElse(null);
    }

    // -------------------------------------------------------------------------
    // Comparison helpers
    // -------------------------------------------------------------------------

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

    private static boolean matchesCheck(Object extracted, Pattern compiledPattern) {
        if (extracted == null || compiledPattern == null) return false;
        return compiledPattern.matcher(extracted.toString()).find();
    }
}
