package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

import java.util.List;

/**
 * Emits the message to each topic in {@code topics}, producing one
 * {@link com.joxette.replay.transform.ReplayMessage} copy per target topic.
 *
 * <p>{@link com.joxette.replay.transform.TransformPipeline#apply} expands a
 * single message into {@code topics.size()} copies at this step. Any steps
 * declared after {@code fan_out} in the pipeline apply independently to each copy.
 *
 * <p>Example:
 * <pre>{@code {"type": "fan_out", "topics": ["orders-a", "orders-b"]}}</pre>
 */
public record FanOutStep(List<String> topics) implements TransformStep {}
