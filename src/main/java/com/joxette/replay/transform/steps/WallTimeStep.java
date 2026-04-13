package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformStep;

import java.time.Instant;

/**
 * Overwrites the field at {@code target} with the current wall-clock time (ISO-8601).
 * Defaults to {@code "$.timestamp"} when {@code target} is omitted.
 *
 * <p>The same {@link Instant#now()} snapshot is used for all timestamp fields when
 * {@code target} is {@code "ALL_TIMESTAMPS"}, so both fields get the same instant.
 *
 * <p>Example:
 * <pre>{@code {"type": "wall_time", "target": "$.processedAt"}}</pre>
 */
public record WallTimeStep(String target) implements TransformStep {

    public WallTimeStep {
        if (target == null || target.isBlank()) target = "$.timestamp";
    }

    @Override
    public void apply(ReplayMessage msg) {
        Instant now = Instant.now();
        TimeStepHelper.applyToTimestampTarget(msg, target, __ -> now);
    }
}
