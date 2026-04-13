package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.MessageJsonPath;
import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformStep;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Renders a template string and writes the result to {@code target} (JSONPath).
 *
 * <p>The template may reference message fields via {@code ${path}} placeholders,
 * where {@code path} is a dot-notation field reference <em>without</em> the
 * leading {@code $.}. The placeholder is resolved by prepending {@code $.}
 * and reading the resulting JSONPath from the current message. Unresolvable
 * placeholders are replaced with an empty string.
 *
 * <p>Example — build a routing key from value fields and message metadata:
 * <pre>{@code
 * {
 *   "type": "template",
 *   "target": "$.value.routing_key",
 *   "template": "replay-${value.order_id}-${partition}"
 * }
 * }</pre>
 */
public record TemplateStep(String target, String template) implements TransformStep {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^}]+)\\}");

    @Override
    public void apply(ReplayMessage msg) {
        Matcher m = PLACEHOLDER.matcher(template);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String placeholder = m.group(1);          // e.g. "value.order_id"
            String jsonPath    = "$." + placeholder;  // e.g. "$.value.order_id"
            Object val = MessageJsonPath.read(msg, jsonPath);
            m.appendReplacement(sb, Matcher.quoteReplacement(
                    val != null ? String.valueOf(val) : ""));
        }
        m.appendTail(sb);
        MessageJsonPath.write(msg, target, sb.toString());
    }
}
