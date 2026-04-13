package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.joxette.replay.transform.TransformContext;
import com.joxette.replay.transform.TransformStep;
import com.joxette.replay.transform.ReplayMessage;

/**
 * Renames the field at {@code source} (JSONPath) to {@code newName} within
 * the same parent object.  The value is preserved; only the key changes.
 *
 * <p>{@code source} must point to a key in a JSON object — for example
 * {@code "$.value.orderId"} renames the {@code orderId} field inside the Kafka
 * message value.  {@code newName} is the new key name, <em>not</em> a path —
 * just the bare leaf name (e.g. {@code "order_id"}).
 *
 * <p>If the source path does not exist the step is a silent no-op.
 *
 * <p>Example:
 * <pre>{@code {"type": "rename_field", "source": "$.value.orderId", "new_name": "order_id"}}</pre>
 */
public record RenameFieldStep(
        String source,
        @JsonProperty("new_name") String newName
) implements TransformStep {

    /**
     * Executes the rename against {@code msg}.
     *
     * @param msg mutable message carrier
     * @param ctx per-session context (unused by this step)
     */
    public void apply(ReplayMessage msg, TransformContext ctx) {
        ObjectNode wrapper;
        try {
            wrapper = JsonStepHelper.buildWrapper(msg);
            Object[]   pl      = JsonStepHelper.parentAndLeaf(wrapper, source);
            ObjectNode parent  = (ObjectNode) pl[0];
            String     oldKey  = (String) pl[1];
            JsonNode   value   = parent.get(oldKey);
            if (value == null) return;   // absent → no-op
            parent.remove(oldKey);
            parent.set(newName, value);
        } catch (IllegalArgumentException e) {
            return;  // non-navigable path or non-JSON value → no-op
        }
        JsonStepHelper.applyWrapper(wrapper, msg);
    }
}
