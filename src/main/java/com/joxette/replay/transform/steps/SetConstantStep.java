package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.joxette.replay.transform.TransformStep;

/**
 * Sets the field at {@code target} (JSONPath) to the literal {@code value}.
 *
 * <p>Example:
 * <pre>{@code {"type": "set_constant", "target": "$.environment", "value": "staging"}}</pre>
 */
public record SetConstantStep(String target, JsonNode value) implements TransformStep {}
