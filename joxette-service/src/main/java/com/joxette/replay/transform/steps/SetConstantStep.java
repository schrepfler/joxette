package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.joxette.replay.transform.MessageJsonPath;
import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformStep;

/**
 * Sets the field at {@code target} (JSONPath) to the literal {@code value}.
 *
 * <p>{@code value} may be any JSON-serialisable type: string, number, boolean,
 * null, object, or array. If the field does not exist in the value body and
 * the path is a single-level child (e.g. {@code $.value.env}), it is created.
 * Deeper missing paths are silently skipped.
 *
 * <p>Example:
 * <pre>{@code {"type": "set_constant", "target": "$.value.env", "value": "staging"}}</pre>
 */
public record SetConstantStep(String target, JsonNode value) implements TransformStep {

    @Override
    public void apply(ReplayMessage msg) {
        MessageJsonPath.write(msg, target, MessageJsonPath.toNativeValue(value));
    }
}
