package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

/**
 * Overrides the topic that the message is emitted to.
 *
 * <p>{@code topic} may be a literal string or a {@code ${path}} template resolved
 * against the message at apply time (e.g. {@code ${$.value.target_topic}}).
 * This affects the {@code topic} field in the replay response and, when the
 * replay-to-Kafka feature is present, the actual produce target.
 *
 * <p>Example:
 * <pre>{@code {"type": "redirect_topic", "topic": "orders-staging"}}</pre>
 */
public record RedirectTopicStep(String topic) implements TransformStep {}
