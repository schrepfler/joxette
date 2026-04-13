package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

/**
 * Adds a Kafka header with the given {@code key} and {@code value}.
 * If a header with the same key already exists it is overwritten.
 *
 * <p>Example:
 * <pre>{@code {"type": "add_header", "key": "x-env", "value": "staging"}}</pre>
 */
public record AddHeaderStep(String key, String value) implements TransformStep {}
