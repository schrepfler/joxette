package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.joxette.replay.transform.TransformContext;
import com.joxette.replay.transform.TransformStep;
import com.joxette.replay.transform.ReplayMessage;

/**
 * Extracts a value from the message using a JSONPath {@code expression} and
 * sets it as the new Kafka message key.
 *
 * <p>The expression is evaluated against the message wrapper (see
 * {@link JsonStepHelper}), so paths like {@code "$.value.order_id"} navigate
 * into the decoded JSON value of the Kafka message.
 *
 * <p>The extracted node is converted to its text representation via
 * {@link JsonNode#asText()}.  If the path is absent or resolves to JSON
 * {@code null} the key is set to {@code null} (keyless message).
 *
 * <p>If the path cannot be navigated (e.g. the message value is not valid JSON)
 * the step is a silent no-op and the key is left unchanged.
 *
 * <p>Example — use the {@code order_id} field as the Kafka key:
 * <pre>{@code {"type": "key_from_value", "expression": "$.value.order_id"}}</pre>
 */
public record KeyFromValueStep(String expression) implements TransformStep {

    /**
     * Resolves {@code expression} against {@code msg} and writes the result as
     * the new key.
     *
     * @param msg mutable message carrier
     * @param ctx per-session context (unused by this step)
     */
    public void apply(ReplayMessage msg, TransformContext ctx) {
        try {
            ObjectNode wrapper = JsonStepHelper.buildWrapper(msg);
            Object[]   pl      = JsonStepHelper.parentAndLeaf(wrapper, expression);
            ObjectNode parent  = (ObjectNode) pl[0];
            String     leaf    = (String) pl[1];
            JsonNode   node    = parent.get(leaf);

            if (node == null || node.isNull()) {
                msg.key = null;
            } else {
                msg.key = JsonStepHelper.encodeKey(node.asText());
            }
        } catch (IllegalArgumentException e) {
            // Non-navigable path or non-JSON value → leave key unchanged
        }
    }
}
