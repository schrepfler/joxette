package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformContext;
import com.joxette.replay.transform.TransformStep;

import java.time.Duration;
import java.time.Instant;

/**
 * Compresses inter-message gaps for streaming replay (SSE / NDJSON) by {@code factor}.
 *
 * <p>A {@code factor} of 6.0 replays 1 hour of events in 10 minutes. The step does
 * <em>not</em> modify message timestamps — it controls pacing only, by computing how
 * long the streaming layer should sleep before emitting each message.
 *
 * <h3>Algorithm</h3>
 * <ol>
 *   <li><b>First message</b> — records {@code (msgTs, Instant.now())} as the anchor in
 *       {@link TransformContext}; sets {@code pendingSleep} to zero.</li>
 *   <li><b>Subsequent messages</b> — computes the raw gap from the anchor timestamp,
 *       scales it: {@code scaledGap = rawGap / factor}, then sets {@code pendingSleep}
 *       to {@code max(0, anchorWall + scaledGap − now)}.</li>
 * </ol>
 *
 * <p>For paginated (non-streaming) responses {@code pendingSleep} is always zero — the
 * caller creates a throwaway {@link TransformContext} per message.
 *
 * <p>Example:
 * <pre>{@code {"type": "time_compress", "target": "$.timestamp", "factor": 6.0}}</pre>
 */
public record TimeCompressStep(String target, double factor) implements TransformStep {

    /**
     * Computes the sleep duration the streaming layer should observe before emitting
     * this message, storing it in {@code ctx}.  Does <em>not</em> modify the message
     * timestamp.
     *
     * @param msg the mutable message carrier
     * @param ctx per-stream context; holds the anchor and receives the pending sleep
     */
    public void apply(ReplayMessage msg, TransformContext ctx) {
        Instant msgTs = TimeStepHelper.resolveTimestamp(msg, target);
        if (msgTs == null) {
            ctx.setPendingSleep(Duration.ZERO);
            return;
        }

        if (ctx.getCompressAnchorMsgTs() == null) {
            // First message — establish the anchor; no sleep required
            ctx.setCompressAnchor(msgTs, Instant.now());
            ctx.setPendingSleep(Duration.ZERO);
        } else {
            long rawGapMs = Duration.between(ctx.getCompressAnchorMsgTs(), msgTs).toMillis();
            if (rawGapMs <= 0) {
                ctx.setPendingSleep(Duration.ZERO);
            } else {
                long scaledMs = Math.round(rawGapMs / factor);
                Instant targetWall = ctx.getCompressAnchorWallTs().plusMillis(scaledMs);
                ctx.setPendingSleep(Duration.between(Instant.now(), targetWall));
            }
        }
    }
}
