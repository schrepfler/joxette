package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

/**
 * Extracts the Kafka message key from the message value at JSONPath {@code source}.
 * The extracted value is serialised to a string and set as the new key.
 *
 * <p>Example:
 * <pre>{@code {"type": "key_from_value", "source": "$.order_id"}}</pre>
 */
public record KeyFromValueStep(String source) implements TransformStep {}
