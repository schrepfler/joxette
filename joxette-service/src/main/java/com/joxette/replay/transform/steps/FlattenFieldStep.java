package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.joxette.replay.transform.TransformContext;
import com.joxette.replay.transform.TransformStep;
import com.joxette.replay.transform.ReplayMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Hoists all fields from the nested object at {@code source} (JSONPath) into
 * the parent object, then removes the source key.
 *
 * <p>If {@code prefix} is provided each hoisted key is prepended with it.
 * When a hoisted key collides with an existing key in the parent the incoming
 * value wins (last-write-wins).
 *
 * <p>If the source path is absent, not an object, or the message value is not
 * valid JSON the step is a silent no-op.
 *
 * <p>Example — hoist {@code metadata} fields with a {@code "meta_"} prefix:
 * <pre>{@code {"type": "flatten_field", "source": "$.value.metadata", "prefix": "meta_"}}</pre>
 *
 * <p>Input value:
 * <pre>{@code {"order_id": "42", "metadata": {"correlation_id": "abc", "env": "prod"}}}</pre>
 *
 * <p>Output value:
 * <pre>{@code {"order_id": "42", "meta_correlation_id": "abc", "meta_env": "prod"}}</pre>
 */
public record FlattenFieldStep(String source, String prefix) implements TransformStep {

    /**
     * Executes the flatten against {@code msg}.
     *
     * @param msg mutable message carrier
     * @param ctx per-session context (unused by this step)
     */
    public void apply(ReplayMessage msg, TransformContext ctx) {
        ObjectNode wrapper;
        try {
            wrapper = JsonStepHelper.buildWrapper(msg);
            Object[]   pl     = JsonStepHelper.parentAndLeaf(wrapper, source);
            ObjectNode parent = (ObjectNode) pl[0];
            String     leafKey = (String) pl[1];

            JsonNode nested = parent.get(leafKey);
            if (nested == null || !nested.isObject()) return;   // absent or not object → no-op

            String pfx = (prefix != null) ? prefix : "";

            // Collect entries first to avoid concurrent-modification on the iterator
            List<Map.Entry<String, JsonNode>> entries = new ArrayList<>();
            nested.fields().forEachRemaining(entries::add);

            entries.forEach(e -> parent.set(pfx + e.getKey(), e.getValue()));
            parent.remove(leafKey);
        } catch (IllegalArgumentException e) {
            return;  // non-navigable path or non-JSON value → no-op
        }
        JsonStepHelper.applyWrapper(wrapper, msg);
    }
}
