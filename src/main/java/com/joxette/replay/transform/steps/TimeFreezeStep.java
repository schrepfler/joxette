package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

/**
 * Replaces the timestamp at {@code target} (JSONPath) with the fixed ISO-8601
 * instant {@code frozenAt} for every message in the replay.
 *
 * <p>Example:
 * <pre>{@code {"type": "time_freeze", "target": "$.timestamp", "frozenAt": "2024-01-01T00:00:00Z"}}</pre>
 */
public record TimeFreezeStep(String target, String frozenAt) implements TransformStep {}
