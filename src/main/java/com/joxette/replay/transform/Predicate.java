package com.joxette.replay.transform;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

/**
 * Sealed predicate hierarchy used by:
 * <ul>
 *   <li>The {@code when} guard on every {@link TransformStep}</li>
 *   <li>{@link com.joxette.replay.transform.steps.FilterDropStep} — drops messages when the predicate is true</li>
 *   <li>{@link com.joxette.replay.transform.steps.ConditionalStep} — selects the {@code then} or {@code else} branch</li>
 * </ul>
 *
 * <p>Jackson uses the {@code "match"} discriminator field to select the concrete type.
 * When {@code "match"} is absent, {@link Leaf} is assumed — simple single-field checks
 * can be written tersely without a wrapper:
 * <pre>{@code
 * { "field": "$.value.event_type", "operator": "EQ", "value": "OrderCreated" }
 * }</pre>
 *
 * <p>Compound predicates use the explicit {@code "match"} key:
 * <pre>{@code
 * {
 *   "match": "and",
 *   "predicates": [
 *     { "field": "$.value.event_type", "operator": "EQ", "value": "OrderCreated" },
 *     { "match": "not",
 *       "predicate": { "field": "$.headers[x-replay]", "operator": "IS_NOT_NULL" } }
 *   ]
 * }
 * }</pre>
 */
@JsonTypeInfo(
    use         = JsonTypeInfo.Id.NAME,
    include     = JsonTypeInfo.As.PROPERTY,
    property    = "match",
    defaultImpl = Predicate.Leaf.class
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = Predicate.Leaf.class, name = "leaf"),
    @JsonSubTypes.Type(value = Predicate.And.class,  name = "and"),
    @JsonSubTypes.Type(value = Predicate.Or.class,   name = "or"),
    @JsonSubTypes.Type(value = Predicate.Not.class,  name = "not")
})
public sealed interface Predicate permits Predicate.Leaf, Predicate.And, Predicate.Or, Predicate.Not {

    /**
     * Comparison operators used by {@link Leaf} predicates.
     */
    enum Operator {
        /** Field equals value. */
        EQ,
        /** Field does not equal value. */
        NEQ,
        /** Field is greater than value. */
        GT,
        /** Field is greater than or equal to value. */
        GTE,
        /** Field is less than value. */
        LT,
        /** Field is less than or equal to value. */
        LTE,
        /** Field (as string) contains value. */
        CONTAINS,
        /** Field (as string) matches a regex value. Pattern is pre-compiled at evaluation time. */
        MATCHES,
        /** Field is null. No {@code value} needed. */
        IS_NULL,
        /** Field is non-null. No {@code value} needed. */
        IS_NOT_NULL
    }

    // -------------------------------------------------------------------------
    // Permitted subtypes
    // -------------------------------------------------------------------------

    /**
     * Single-field predicate check.
     *
     * <p>This is the default type when the {@code "match"} discriminator is absent,
     * allowing terse single-condition expressions without a wrapper:
     * <pre>{@code { "field": "$.value.status", "operator": "EQ", "value": "created" } }</pre>
     *
     * @param field    JSONPath-like field expression (e.g. {@code "$.value.event_type"},
     *                 {@code "$.headers[x-env]"}, {@code "$.partition"})
     * @param operator comparison operator
     * @param value    comparison value — {@code String}, {@code Number}, or {@code null};
     *                 ignored for {@code IS_NULL} and {@code IS_NOT_NULL}
     */
    record Leaf(
            @JsonProperty("field")    String   field,
            @JsonProperty("operator") Operator operator,
            @JsonProperty("value")    Object   value
    ) implements Predicate {}

    /**
     * Conjunction — all sub-predicates must evaluate to {@code true}.
     * Requires at least two predicates.
     *
     * @param predicates the sub-predicates; all must match for this to match
     */
    record And(
            @JsonProperty("predicates") List<Predicate> predicates
    ) implements Predicate {}

    /**
     * Disjunction — at least one sub-predicate must evaluate to {@code true}.
     * Requires at least two predicates.
     *
     * @param predicates the sub-predicates; at least one must match for this to match
     */
    record Or(
            @JsonProperty("predicates") List<Predicate> predicates
    ) implements Predicate {}

    /**
     * Negation — inverts the result of a single sub-predicate.
     *
     * @param predicate the sub-predicate to negate
     */
    record Not(
            @JsonProperty("predicate") Predicate predicate
    ) implements Predicate {}
}
