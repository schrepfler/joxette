package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.joxette.replay.transform.TransformContext;
import com.joxette.replay.transform.TransformStep;
import com.joxette.replay.transform.ReplayMessage;

/**
 * Removes the field at {@code target} (JSONPath) from the message.
 *
 * <p>If the path does not exist the step is a silent no-op.
 *
 * <p>Example — delete a field from the Kafka message value:
 * <pre>{@code {"type": "delete_field", "target": "$.value.internal_debug_info"}}</pre>
 */
public record DeleteFieldStep(String target) implements TransformStep {

    /**
     * Executes the deletion against {@code msg}.
     *
     * @param msg mutable message carrier
     * @param ctx per-session context (unused by this step)
     */
    public void apply(ReplayMessage msg, TransformContext ctx) {
        ObjectNode wrapper;
        try {
            wrapper = JsonStepHelper.buildWrapper(msg);
            Object[]   pl     = JsonStepHelper.parentAndLeaf(wrapper, target);
            ObjectNode parent = (ObjectNode) pl[0];
            String     leaf   = (String) pl[1];
            if (!parent.has(leaf)) return;   // absent → no-op
            parent.remove(leaf);
        } catch (IllegalArgumentException e) {
            return;  // non-navigable path or non-JSON value → no-op
        }
        JsonStepHelper.applyWrapper(wrapper, msg);
    }
}
