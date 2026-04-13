package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

/**
 * Renders a template string and writes the result to {@code target} (JSONPath).
 * The template may reference message fields via {@code {{field}}} placeholders.
 *
 * <p>Example:
 * <pre>{@code {"type": "template", "target": "$.label", "template": "order-{{$.order_id}}"}}</pre>
 */
public record TemplateStep(String target, String template) implements TransformStep {}
