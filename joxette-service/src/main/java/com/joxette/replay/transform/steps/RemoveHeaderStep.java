package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

/**
 * Removes all Kafka headers whose key matches {@code key}.
 *
 * <p>Example:
 * <pre>{@code {"type": "remove_header", "key": "x-internal-trace"}}</pre>
 */
public record RemoveHeaderStep(String key) implements TransformStep {}
