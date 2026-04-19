package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformStep;
import com.joxette.replay.transform.gap.GapOperation;
import com.joxette.replay.transform.gap.GapSelector;

/**
 * Selects one or more inter-message gaps matching a {@link GapSelector} and rewrites
 * their duration using a {@link GapOperation} (cut, hold, trim, pad, scale).
 *
 * <p>The {@link #apply(ReplayMessage)} method is intentionally a no-op in Phase 1.
 * Stateful evaluation is wired in Phase 2 via {@code GapEvaluator}, which tracks
 * anchor matches across messages and writes the computed sleep duration into
 * {@code TransformContext}.
 *
 * <p>Example — hold all gaps after the first OrderCreated at 500 ms:
 * <pre>{@code
 * { "type": "gap_transform",
 *   "select": { "after": { "predicate": { "field": "$.value.type", "operator": "EQ",
 *                                          "value": "OrderCreated" },
 *                           "quantifier": "first" },
 *               "min_duration_ms": 3000 },
 *   "operation": { "op": "hold", "target_ms": 500 } }
 * }</pre>
 *
 * <p>Example — cut all gaps above 10 s:
 * <pre>{@code
 * { "type": "gap_transform",
 *   "select": { "min_duration_ms": 10000 },
 *   "operation": { "op": "cut" } }
 * }</pre>
 *
 * @param select    selects which gaps this step acts on
 * @param operation the rewrite to apply to each selected gap
 */
public record GapTransformStep(
        @JsonProperty("select")    GapSelector  select,
        @JsonProperty("operation") GapOperation operation
) implements TransformStep {

    // apply() intentionally left as the TransformStep default no-op.
    // GapEvaluator (Phase 2) will drive this step via TransformContext.
}
