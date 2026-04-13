package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

/**
 * Removes the field at {@code target} (JSONPath) from the message value.
 *
 * <p>Example:
 * <pre>{@code {"type": "delete_field", "target": "$.internal_debug_info"}}</pre>
 */
public record DeleteFieldStep(String target) implements TransformStep {}
