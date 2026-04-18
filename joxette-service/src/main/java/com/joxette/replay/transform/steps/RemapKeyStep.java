package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.joxette.replay.transform.TransformContext;
import com.joxette.replay.transform.TransformStep;
import com.joxette.replay.transform.ReplayMessage;

/**
 * Replaces the Kafka message key with a new value derived from {@code value}
 * and optionally prepended with {@code prefix}.
 *
 * <h2>Value syntax</h2>
 * <ul>
 *   <li><b>Literal string</b> — used as-is (e.g. {@code "replay-key"}).</li>
 *   <li><b>Template</b> — a string containing one or more
 *       <code>${$.some.path}</code> placeholders.  Each placeholder is resolved
 *       against the message wrapper (see {@link JsonStepHelper}) and replaced
 *       with the string representation of the extracted value.  If a path
 *       cannot be resolved the placeholder is replaced with an empty string.</li>
 * </ul>
 *
 * <p>If {@code prefix} is non-null it is prepended to the resolved value.
 *
 * <p>Examples:
 * <pre>{@code
 * // Literal key with a prefix
 * {"type": "remap_key", "value": "order", "prefix": "replay-"}
 *
 * // Key derived from the message value field, with prefix
 * {"type": "remap_key", "value": "${$.value.order_id}", "prefix": "ord-"}
 * }</pre>
 */
public record RemapKeyStep(String value, String prefix) implements TransformStep {

    /**
     * Resolves the new key and writes it back into {@code msg}.
     *
     * @param msg mutable message carrier
     * @param ctx per-session context (unused by this step)
     */
    public void apply(ReplayMessage msg, TransformContext ctx) {
        String resolved = value.contains("${")
                ? resolveTemplate(value, msg)
                : value;

        if (prefix != null && !prefix.isEmpty()) {
            resolved = prefix + resolved;
        }

        msg.key = JsonStepHelper.encodeKey(resolved);
    }

    /** Expands all {@code ${$.path}} placeholders in {@code template}. */
    private static String resolveTemplate(String template, ReplayMessage msg) {
        ObjectNode wrapper = JsonStepHelper.buildWrapper(msg);
        StringBuilder sb = new StringBuilder(template.length());
        int pos = 0;
        while (pos < template.length()) {
            int start = template.indexOf("${", pos);
            if (start == -1) {
                sb.append(template, pos, template.length());
                break;
            }
            sb.append(template, pos, start);
            int end = template.indexOf("}", start + 2);
            if (end == -1) {
                sb.append(template, start, template.length());
                break;
            }
            String path = template.substring(start + 2, end);
            sb.append(resolvePathAsText(path, wrapper));
            pos = end + 1;
        }
        return sb.toString();
    }

    private static String resolvePathAsText(String path, ObjectNode wrapper) {
        try {
            Object[]   pl   = JsonStepHelper.parentAndLeaf(wrapper, path);
            ObjectNode par  = (ObjectNode) pl[0];
            String     leaf = (String) pl[1];
            JsonNode   node = par.get(leaf);
            return (node != null && !node.isNull()) ? node.asText() : "";
        } catch (IllegalArgumentException e) {
            return "";
        }
    }
}
