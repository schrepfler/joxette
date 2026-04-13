package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

/**
 * Sets the Kafka message key to {@code null} (keyless message).
 * Useful when replaying to a topic that uses round-robin partitioning.
 *
 * <p>Example:
 * <pre>{@code {"type": "null_key"}}</pre>
 */
public record NullKeyStep() implements TransformStep {}
