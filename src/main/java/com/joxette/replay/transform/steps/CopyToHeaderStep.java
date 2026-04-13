package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

/**
 * Copies the value at JSONPath {@code source} (in the message value) into
 * a Kafka header named {@code headerKey}.
 *
 * <p>Example:
 * <pre>{@code {"type": "copy_to_header", "source": "$.correlationId", "headerKey": "x-correlation-id"}}</pre>
 */
public record CopyToHeaderStep(String source, String headerKey) implements TransformStep {}
