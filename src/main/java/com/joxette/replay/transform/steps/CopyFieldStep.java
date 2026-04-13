package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

/**
 * Copies the value at JSONPath {@code from} to JSONPath {@code to}.
 *
 * <p>Example:
 * <pre>{@code {"type": "copy_field", "from": "$.order_id", "to": "$.legacy_id"}}</pre>
 */
public record CopyFieldStep(String from, String to) implements TransformStep {}
