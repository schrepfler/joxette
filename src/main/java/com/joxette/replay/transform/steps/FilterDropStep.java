package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformStep;

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
 * at construction time. All field extraction and comparison logic is provided by
 * {@link PredicateEvaluator}, which is also shared with {@link ConditionalStep}.
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
    // Predicate evaluation — delegated to PredicateEvaluator
    // -------------------------------------------------------------------------

    /**
     * Returns {@code true} if the predicate matches {@code msg} (i.e. the message
     * should be dropped).
     */
    public boolean test(ReplayMessage msg) {
        return PredicateEvaluator.test(field, operator, value, compiledPattern, msg);
    }
}
