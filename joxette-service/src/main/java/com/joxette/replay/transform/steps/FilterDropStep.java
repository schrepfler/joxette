package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.joxette.replay.transform.Predicate;
import com.joxette.replay.transform.PredicateEvaluator;
import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformStep;

/**
 * Drops the message from the replay stream when the {@link Predicate} evaluates to
 * {@code true}. When the predicate does NOT match, the message passes through unchanged.
 *
 * <p>The predicate may be a simple leaf check or any compound predicate (and/or/not).
 * Evaluation is delegated to {@link PredicateEvaluator}.
 *
 * <p>Example — drop messages from partition 3:
 * <pre>{@code
 * {"type": "filter_drop", "predicate": {"field": "$.partition", "operator": "EQ", "value": 3}}
 * }</pre>
 *
 * <p>Example — drop messages whose status field is "cancelled":
 * <pre>{@code
 * {"type": "filter_drop", "predicate": {"field": "$.value.status", "operator": "EQ", "value": "cancelled"}}
 * }</pre>
 *
 * <p>Example — compound predicate (drop test messages from staging):
 * <pre>{@code
 * {"type": "filter_drop", "predicate": {
 *   "match": "and",
 *   "predicates": [
 *     {"field": "$.headers[x-env]", "operator": "EQ", "value": "staging"},
 *     {"field": "$.headers[x-test]", "operator": "IS_NOT_NULL"}
 *   ]
 * }}
 * }</pre>
 */
public final class FilterDropStep implements TransformStep {

    private final Predicate predicate;

    @JsonCreator
    public FilterDropStep(@JsonProperty("predicate") Predicate predicate) {
        this.predicate = predicate;
    }

    /** The predicate used to decide whether to drop each message. */
    public Predicate predicate() {
        return predicate;
    }

    // -------------------------------------------------------------------------
    // Convenience accessors — delegate to the Leaf predicate when applicable.
    // Used by SqlPushdownAnalyzer for SQL WHERE pushdown of simple leaf predicates.
    // Returns null for compound predicates (and/or/not), which are not pushable.
    // -------------------------------------------------------------------------

    /**
     * Returns the {@code field} of the underlying {@link Predicate.Leaf}, or {@code null}
     * if the predicate is compound ({@link Predicate.And}, {@link Predicate.Or},
     * {@link Predicate.Not}).
     */
    public String field() {
        return (predicate instanceof Predicate.Leaf leaf) ? leaf.field() : null;
    }

    /**
     * Returns the {@code operator} of the underlying {@link Predicate.Leaf}, or
     * {@code null} if the predicate is compound.
     */
    public Predicate.Operator operator() {
        return (predicate instanceof Predicate.Leaf leaf) ? leaf.operator() : null;
    }

    /**
     * Returns the {@code value} of the underlying {@link Predicate.Leaf}, or
     * {@code null} if the predicate is compound.
     */
    public Object value() {
        return (predicate instanceof Predicate.Leaf leaf) ? leaf.value() : null;
    }

    // -------------------------------------------------------------------------
    // Predicate evaluation
    // -------------------------------------------------------------------------

    /**
     * Returns {@code true} if the predicate matches {@code msg} (i.e. the message
     * should be dropped).
     */
    public boolean test(ReplayMessage msg) {
        return PredicateEvaluator.evaluate(predicate, msg);
    }
}
