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
 * Overlays a partial JSON object onto the field at {@code target} (JSONPath)
 * following <a href="https://www.rfc-editor.org/rfc/rfc7396">RFC 7396 JSON
 * Merge Patch</a> semantics.
 *
 * <p>Rules applied recursively:
 * <ul>
 *   <li>If a key in {@code patch} maps to a non-null value, that key is set in
 *       the target (overwriting any existing value).</li>
 *   <li>If a key in {@code patch} maps to JSON {@code null}, that key is
 *       <em>removed</em> from the target.</li>
 *   <li>If both the target and the patch contain an object for the same key,
 *       the merge is applied recursively.</li>
 * </ul>
 *
 * <p>If the field at {@code target} is absent or not a JSON object the step is
 * a silent no-op.
 *
 * <p>Example — add/overwrite one key and remove another:
 * <pre>{@code
 * {
 *   "type":   "merge_patch",
 *   "target": "$.value",
 *   "patch":  {"status": "REPLAYED", "internal_id": null}
 * }
 * }</pre>
 */
public record MergePatchStep(String target, JsonNode patch) implements TransformStep {

    /**
     * Applies the RFC 7396 merge patch against {@code msg}.
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

            JsonNode current = parent.get(leaf);
            if (current == null || !current.isObject()) return;  // absent or not object → no-op

            parent.set(leaf, mergePatch((ObjectNode) current, patch));
        } catch (IllegalArgumentException e) {
            return;  // non-navigable path or non-JSON value → no-op
        }
        JsonStepHelper.applyWrapper(wrapper, msg);
    }

    /**
     * RFC 7396 merge: recursively merges {@code patchNode} into a deep-copy of
     * {@code base}.  Null patch values remove keys; non-null values overwrite
     * or recurse into nested objects.
     */
    static ObjectNode mergePatch(ObjectNode base, JsonNode patchNode) {
        if (!patchNode.isObject()) {
            // RFC 7396 §2: if patch is not an object it replaces the target entirely.
            // We only reach here when the target is an object, so this is a degenerate
            // case; return base unchanged.
            return base.deepCopy();
        }

        ObjectNode result = base.deepCopy();

        // Collect entries to avoid concurrent modification
        List<Map.Entry<String, JsonNode>> entries = new ArrayList<>();
        patchNode.fields().forEachRemaining(entries::add);

        for (Map.Entry<String, JsonNode> entry : entries) {
            String  key  = entry.getKey();
            JsonNode val  = entry.getValue();

            if (val.isNull()) {
                result.remove(key);
            } else if (val.isObject() && result.has(key) && result.get(key).isObject()) {
                result.set(key, mergePatch((ObjectNode) result.get(key), val));
            } else {
                result.set(key, val);
            }
        }

        return result;
    }
}
