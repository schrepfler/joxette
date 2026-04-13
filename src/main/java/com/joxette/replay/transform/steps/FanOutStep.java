package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

import java.util.List;

/**
 * Produces the message to each topic in {@code targetTopics}.
 *
 * <p><b>Stub note</b>: multi-output semantics ({@code Stream<ReplayMessage>})
 * are deferred. In this phase the step is a pass-through identity for the
 * browse/stream path.
 *
 * <p>Example:
 * <pre>{@code {"type": "fan_out", "targetTopics": ["orders-a", "orders-b"]}}</pre>
 */
public record FanOutStep(List<String> targetTopics) implements TransformStep {}
