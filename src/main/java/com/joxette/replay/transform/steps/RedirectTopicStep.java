package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

/**
 * Changes the topic field of the replayed message to {@code targetTopic}.
 * This affects where the message is produced when replaying to Kafka.
 *
 * <p>Example:
 * <pre>{@code {"type": "redirect_topic", "targetTopic": "orders-staging"}}</pre>
 */
public record RedirectTopicStep(String targetTopic) implements TransformStep {}
