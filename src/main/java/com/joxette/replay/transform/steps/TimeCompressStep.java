package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

/**
 * Compresses the time range of the timestamp at {@code target} by {@code factor}.
 * A {@code factor} of 2.0 halves the duration from the stream's epoch anchor.
 *
 * <p>Example:
 * <pre>{@code {"type": "time_compress", "target": "$.timestamp", "factor": 2.0}}</pre>
 */
public record TimeCompressStep(String target, double factor) implements TransformStep {}
