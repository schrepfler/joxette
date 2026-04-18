package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformContext;
import com.joxette.replay.transform.TransformStep;

import java.time.Instant;

/**
 * Replaces the timestamp at {@code target} (JSONPath) with a fixed instant for every message.
 *
 * <p>{@code frozenAt} may be:
 * <ul>
 *   <li>An ISO-8601 instant string (e.g. {@code "2024-01-01T00:00:00Z"}) — frozen to that value.</li>
 *   <li>{@code "NOW"} (case-insensitive) — frozen to {@link TransformContext#getReplayStartedAt()},
 *       i.e. the wall-clock time the replay stream began.</li>
 * </ul>
 *
 * <p>When {@code target} is {@code "ALL_TIMESTAMPS"}, freezes {@link ReplayMessage#timestamp},
 * {@link ReplayMessage#recordedAt}, and any header value that parses as ISO-8601.
 *
 * <p>Example:
 * <pre>{@code {"type": "time_freeze", "target": "$.timestamp", "frozenAt": "2024-01-01T00:00:00Z"}}</pre>
 */
public record TimeFreezeStep(String target, String frozenAt) implements TransformStep {

    /**
     * Applies this step using the supplied replay context.
     *
     * <p>Use this overload from the streaming pipeline so that {@code "NOW"} resolves
     * consistently to the single instant the replay session began.
     *
     * @param msg the mutable message carrier
     * @param ctx per-stream context providing {@link TransformContext#getReplayStartedAt()}
     */
    public void apply(ReplayMessage msg, TransformContext ctx) {
        Instant frozen = "NOW".equalsIgnoreCase(frozenAt)
                ? ctx.getReplayStartedAt()
                : Instant.parse(frozenAt);
        TimeStepHelper.applyToTimestampTarget(msg, target, __ -> frozen);
    }

    /**
     * Applies this step without an explicit context.
     *
     * <p>{@code "NOW"} resolves to {@link Instant#now()} at the moment of this call —
     * consistent within a single message but not across a stream (use
     * {@link #apply(ReplayMessage, TransformContext)} for streaming paths).
     *
     * @param msg the mutable message carrier
     */
    @Override
    public void apply(ReplayMessage msg) {
        Instant frozen = "NOW".equalsIgnoreCase(frozenAt)
                ? Instant.now()
                : Instant.parse(frozenAt);
        TimeStepHelper.applyToTimestampTarget(msg, target, __ -> frozen);
    }
}
