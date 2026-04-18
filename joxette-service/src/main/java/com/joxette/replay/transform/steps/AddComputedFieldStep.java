package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.joxette.replay.transform.TransformContext;
import com.joxette.replay.transform.TransformStep;
import com.joxette.replay.transform.ReplayMessage;

import java.time.Instant;

/**
 * Adds a new field at {@code target} (JSONPath) whose value is derived from
 * {@code expression}.
 *
 * <h2>Expression types</h2>
 * <ul>
 *   <li><b>JSONPath</b> — a dot-notation path starting with {@code "$."} that
 *       is read from the current message wrapper (e.g. {@code "$.value.order_id"}).
 *       If the path resolves to {@code null} or is absent the field is set to
 *       JSON {@code null}.</li>
 *   <li><b>{@code REPLAY_SEQUENCE}</b> — monotonically increasing integer
 *       counter within the replay session (0, 1, 2 …), sourced from
 *       {@link TransformContext#sequence()}.</li>
 *   <li><b>{@code NOW_EPOCH_MS}</b> — current wall-clock time as milliseconds
 *       since the Unix epoch (a JSON number).</li>
 *   <li><b>{@code NOW_ISO}</b> — current wall-clock time as an ISO-8601 string.</li>
 * </ul>
 *
 * <p>If the target path cannot be navigated (e.g. an intermediate segment is
 * missing) the step is a silent no-op.
 *
 * <p>Example — stamp each replayed message with a sequence number:
 * <pre>{@code {"type": "add_computed_field", "target": "$.value.replay_seq", "expression": "REPLAY_SEQUENCE"}}</pre>
 *
 * <p>Example — copy an existing field to a new path:
 * <pre>{@code {"type": "add_computed_field", "target": "$.value.order_ref", "expression": "$.value.order_id"}}</pre>
 */
public record AddComputedFieldStep(String target, String expression) implements TransformStep {

    /**
     * Evaluates {@code expression} and writes the result to {@code target} in
     * {@code msg}.
     *
     * @param msg mutable message carrier
     * @param ctx per-session context; provides {@link TransformContext#sequence()}
     *            for the {@code REPLAY_SEQUENCE} token
     */
    public void apply(ReplayMessage msg, TransformContext ctx) {
        ObjectNode wrapper;
        try {
            wrapper = JsonStepHelper.buildWrapper(msg);

            JsonNode computed = evaluate(expression, wrapper, ctx);

            Object[]   targetPl     = JsonStepHelper.parentAndLeaf(wrapper, target);
            ObjectNode targetParent = (ObjectNode) targetPl[0];
            String     targetLeaf   = (String) targetPl[1];
            targetParent.set(targetLeaf, computed);
        } catch (IllegalArgumentException e) {
            return;  // non-navigable target path or non-JSON value → no-op
        }
        JsonStepHelper.applyWrapper(wrapper, msg);
    }

    private static JsonNode evaluate(String expr, ObjectNode wrapper, TransformContext ctx) {
        return switch (expr) {
            case "REPLAY_SEQUENCE" ->
                    JsonNodeFactory.instance.numberNode(ctx.sequence());

            case "NOW_EPOCH_MS" ->
                    JsonNodeFactory.instance.numberNode(System.currentTimeMillis());

            case "NOW_ISO" ->
                    JsonNodeFactory.instance.textNode(Instant.now().toString());

            default -> {
                // Treat as JSONPath — resolve against the message wrapper
                try {
                    Object[]   pl     = JsonStepHelper.parentAndLeaf(wrapper, expr);
                    ObjectNode parent = (ObjectNode) pl[0];
                    String     leaf   = (String) pl[1];
                    JsonNode   node   = parent.get(leaf);
                    yield (node != null) ? node : JsonNodeFactory.instance.nullNode();
                } catch (IllegalArgumentException e) {
                    yield JsonNodeFactory.instance.nullNode();
                }
            }
        };
    }
}
