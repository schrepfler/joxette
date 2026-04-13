package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.joxette.replay.transform.TransformStep;

import java.util.List;

/**
 * Applies {@code thenSteps} when the {@code condition} predicate matches the message,
 * otherwise applies {@code elseSteps}. Either branch may be empty — an empty branch
 * is a no-op that passes the message through unchanged.
 *
 * <p>The {@code condition} uses the same predicate model as {@link FilterDropStep}
 * (field / operator / value), but the semantics are inverted: a {@code true} result
 * means the condition matched and the {@code thenSteps} branch executes. Predicate
 * evaluation is handled by the shared {@link PredicateEvaluator}.
 *
 * <p>{@code ConditionalStep} is itself a {@link TransformStep}, so {@code thenSteps}
 * and {@code elseSteps} may contain further {@code ConditionalStep}s. A
 * {@link FilterDropStep} inside either branch can still drop the message.
 *
 * <p>Example — redact email only when the {@code x-env} header is not {@code prod}:
 * <pre>{@code
 * {
 *   "type": "conditional",
 *   "condition": {"field": "$.headers[x-env]", "operator": "NEQ", "value": "prod"},
 *   "then_steps": [{"type": "redact", "target": "$.value.email"}]
 * }
 * }</pre>
 */
public final class ConditionalStep implements TransformStep {

    private final FilterDropStep      condition;
    private final List<TransformStep> thenSteps;
    private final List<TransformStep> elseSteps;

    @JsonCreator
    public ConditionalStep(
            @JsonProperty("condition")  FilterDropStep      condition,
            @JsonProperty("then_steps") List<TransformStep> thenSteps,
            @JsonProperty("else_steps") List<TransformStep> elseSteps
    ) {
        this.condition = condition;
        this.thenSteps = thenSteps != null ? List.copyOf(thenSteps) : List.of();
        this.elseSteps = elseSteps != null ? List.copyOf(elseSteps) : List.of();
    }

    /**
     * The predicate to evaluate. Uses the same field/operator/value model as
     * {@link FilterDropStep}; evaluates to {@code true} when the condition matches.
     */
    public FilterDropStep condition() { return condition; }

    /** Steps to apply when the condition evaluates to {@code true}. Never null. */
    public List<TransformStep> thenSteps() { return thenSteps; }

    /** Steps to apply when the condition evaluates to {@code false}. Never null. */
    public List<TransformStep> elseSteps() { return elseSteps; }
}
