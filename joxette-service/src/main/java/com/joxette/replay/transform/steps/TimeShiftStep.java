package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformStep;

import java.time.Duration;

/**
 * Shifts the timestamp at {@code target} (JSONPath) by {@code shiftMs} milliseconds.
 * Positive values move the timestamp forward; negative values move it backward.
 *
 * <p>When {@code target} is {@code "ALL_TIMESTAMPS"}, shifts {@link ReplayMessage#timestamp},
 * {@link ReplayMessage#recordedAt}, and any header value that parses as ISO-8601.
 *
 * <p>Example:
 * <pre>{@code {"type": "time_shift", "target": "$.timestamp", "shiftMs": -86400000}}</pre>
 */
public record TimeShiftStep(String target, long shiftMs) implements TransformStep {

    @Override
    public void apply(ReplayMessage msg) {
        Duration delta = Duration.ofMillis(shiftMs);
        TimeStepHelper.applyToTimestampTarget(msg, target, ts -> ts.plus(delta));
    }
}
