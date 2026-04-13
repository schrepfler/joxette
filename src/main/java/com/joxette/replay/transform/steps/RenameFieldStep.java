package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

/**
 * Renames the field at JSONPath {@code from} to JSONPath {@code to},
 * moving the value and removing the original key.
 *
 * <p>Example:
 * <pre>{@code {"type": "rename_field", "from": "$.orderId", "to": "$.order_id"}}</pre>
 */
public record RenameFieldStep(String from, String to) implements TransformStep {}
