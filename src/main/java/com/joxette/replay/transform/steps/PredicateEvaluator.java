package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.Predicate;
import com.joxette.replay.transform.ReplayMessage;

import java.util.regex.Pattern;

/**
 * @deprecated Use {@link com.joxette.replay.transform.PredicateEvaluator} instead.
 *             This class is a thin bridge kept for source-compatibility; new code should
 *             import the top-level evaluator in the {@code transform} package which
 *             supports the full sealed {@link Predicate} hierarchy.
 */
@Deprecated(forRemoval = true)
public final class PredicateEvaluator {

    private PredicateEvaluator() {}

    /**
     * @deprecated Use {@link com.joxette.replay.transform.PredicateEvaluator#evaluate(Predicate, ReplayMessage)}.
     */
    @Deprecated
    public static Object extractField(String field, ReplayMessage msg) {
        return com.joxette.replay.transform.PredicateEvaluator.extractField(field, msg);
    }

    /**
     * @deprecated Use {@link com.joxette.replay.transform.PredicateEvaluator#evaluate(Predicate, ReplayMessage)}.
     */
    @Deprecated
    public static boolean evaluate(Object extracted,
                                   Predicate.Operator operator,
                                   Object value,
                                   Pattern compiledPattern) {
        return com.joxette.replay.transform.PredicateEvaluator.evaluate(
                extracted, operator, value, compiledPattern);
    }
}
