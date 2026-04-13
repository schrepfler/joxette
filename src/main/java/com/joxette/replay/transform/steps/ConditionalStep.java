package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

import java.util.List;

/**
 * Applies {@code thenSteps} when {@code condition} is {@code true}, otherwise
 * applies {@code elseSteps}. Either branch may be empty.
 *
 * <p><b>Stub note</b>: condition evaluation is not yet implemented. In this phase
 * the step is a pass-through (always takes the else/no-op branch).
 *
 * <p>Example:
 * <pre>{@code {
 *   "type": "conditional",
 *   "condition": "$.amount > 1000",
 *   "thenSteps": [{"type": "add_header", "key": "x-high-value", "value": "true"}],
 *   "elseSteps": []
 * }}</pre>
 */
public record ConditionalStep(
        String condition,
        List<TransformStep> thenSteps,
        List<TransformStep> elseSteps
) implements TransformStep {
    public ConditionalStep {
        if (thenSteps == null) thenSteps = List.of();
        if (elseSteps == null) elseSteps = List.of();
    }
}
