package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.joxette.replay.transform.MessageJsonPath;
import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformStep;

import java.util.List;

/**
 * Writes the first non-null value from {@code sources} (a JSONPath list) to
 * {@code target}. If all sources resolve to {@code null} and a {@code fallback}
 * literal is provided, the fallback is written instead. If all sources are null
 * and no fallback is given, {@code target} is left unchanged.
 *
 * <p>Example — use {@code legacy_order_id} when {@code order_id} is absent,
 * defaulting to {@code "unknown"} if both are missing:
 * <pre>{@code
 * {
 *   "type": "coalesce",
 *   "sources": ["$.value.order_id", "$.value.legacy_order_id"],
 *   "target": "$.value.id",
 *   "fallback": "unknown"
 * }
 * }</pre>
 */
public record CoalesceStep(List<String> sources, String target, JsonNode fallback)
        implements TransformStep {

    public CoalesceStep {
        if (sources == null) {
            sources = List.of();
        }
    }

    @Override
    public void apply(ReplayMessage msg) {
        for (String source : sources) {
            Object val = MessageJsonPath.read(msg, source);
            if (val != null) {
                MessageJsonPath.write(msg, target, val);
                return;
            }
        }
        if (fallback != null) {
            MessageJsonPath.write(msg, target, MessageJsonPath.toNativeValue(fallback));
        }
    }
}
