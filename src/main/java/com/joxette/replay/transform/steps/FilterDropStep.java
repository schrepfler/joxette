package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

/**
 * Drops the message from the replay stream when {@code condition} evaluates to
 * {@code true}. The condition is an expression evaluated against the message.
 *
 * <p><b>Stub note</b>: condition evaluation is not yet implemented. In this phase
 * the step is a pass-through (never drops) to avoid breaking pipelines that
 * include this step before the evaluator is built.
 *
 * <p>Example:
 * <pre>{@code {"type": "filter_drop", "condition": "$.status == 'cancelled'"}}</pre>
 */
public record FilterDropStep(String condition) implements TransformStep {}
