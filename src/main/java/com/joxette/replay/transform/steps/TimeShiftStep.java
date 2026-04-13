package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

/**
 * Shifts the timestamp at {@code target} (JSONPath) by {@code shiftMs} milliseconds.
 * Positive values move the timestamp forward; negative values move it backward.
 *
 * <p>Example:
 * <pre>{@code {"type": "time_shift", "target": "$.timestamp", "shiftMs": -86400000}}</pre>
 */
public record TimeShiftStep(String target, long shiftMs) implements TransformStep {}
