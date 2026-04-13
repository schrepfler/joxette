package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

/**
 * Replaces the value at {@code target} (JSONPath) with {@code replacement}.
 * Defaults to {@code "***"} when {@code replacement} is omitted.
 *
 * <p>Example:
 * <pre>{@code {"type": "redact", "target": "$.ssn"}}</pre>
 */
public record RedactStep(String target, String replacement) implements TransformStep {
    public RedactStep {
        if (replacement == null) replacement = "***";
    }
}
