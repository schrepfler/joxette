package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformContext;
import com.joxette.replay.transform.TransformStep;
import com.joxette.replay.transform.ReplayMessage;

/**
 * Sets the Kafka message key to {@code null} (keyless message).
 *
 * <p>Useful when replaying to a topic that should use round-robin partition
 * assignment — a null key means Kafka's default partitioner distributes messages
 * evenly rather than pinning them to a partition by key hash.
 *
 * <p>Example:
 * <pre>{@code {"type": "null_key"}}</pre>
 */
public record NullKeyStep() implements TransformStep {

    /**
     * Sets {@code msg.key} to {@code null}.
     *
     * @param msg mutable message carrier
     * @param ctx per-session context (unused by this step)
     */
    public void apply(ReplayMessage msg, TransformContext ctx) {
        msg.key = null;
    }
}
