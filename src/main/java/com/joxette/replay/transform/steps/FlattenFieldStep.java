package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

/**
 * Flattens the nested object at {@code source} (JSONPath) into the root of the
 * message value, optionally prefixing each key with {@code prefix}.
 *
 * <p>Example:
 * <pre>{@code {"type": "flatten_field", "source": "$.metadata", "prefix": "meta_"}}</pre>
 */
public record FlattenFieldStep(String source, String prefix) implements TransformStep {}
