package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

/**
 * Overwrites the field at {@code target} with the current wall-clock time (ISO-8601).
 * Defaults to {@code "$.timestamp"} when {@code target} is omitted.
 *
 * <p>Example:
 * <pre>{@code {"type": "wall_time", "target": "$.processedAt"}}</pre>
 */
public record WallTimeStep(String target) implements TransformStep {
    public WallTimeStep {
        if (target == null || target.isBlank()) target = "$.timestamp";
    }
}
