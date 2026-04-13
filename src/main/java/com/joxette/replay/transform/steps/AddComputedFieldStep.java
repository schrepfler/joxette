package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

/**
 * Evaluates {@code expression} against the message and writes the result to
 * {@code target} (JSONPath). Expression language is implementation-defined.
 *
 * <p>Example:
 * <pre>{@code {"type": "add_computed_field", "target": "$.total", "expression": "$.price * $.quantity"}}</pre>
 */
public record AddComputedFieldStep(String target, String expression) implements TransformStep {}
