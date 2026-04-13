package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

import java.util.List;

/**
 * Writes the first non-null value from {@code sources} (JSONPath list) to {@code target}.
 * If all sources are null or absent, {@code target} is left unchanged.
 *
 * <p>Example:
 * <pre>{@code {"type": "coalesce", "sources": ["$.order_id", "$.legacy_order_id"], "target": "$.id"}}</pre>
 */
public record CoalesceStep(List<String> sources, String target) implements TransformStep {
    public CoalesceStep {
        if (sources == null) sources = List.of();
    }
}
