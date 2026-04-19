package com.joxette.replay.transform.gap;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Selects one or more gaps in the message stream for a {@link GapOperation} to act on.
 *
 * <p>At least one of {@code after}, {@code before}, or {@code withinFragment} must be
 * non-null; a selector with none of these set is vacuously unbounded and would match
 * every gap, which requires an explicit intent — use {@code minDurationMs} or
 * {@code maxDurationMs} to scope it in that case.
 *
 * <p>When multiple fields are set they narrow the selection — they are combined with AND.
 *
 * <p>Examples:
 * <pre>{@code
 * // Gap after the first OrderCreated that is at least 3 seconds long
 * { "after": { "predicate": { "field": "$.value.type", "operator": "EQ",
 *                              "value": "OrderCreated" }, "quantifier": "first" },
 *   "min_duration_ms": 3000 }
 *
 * // All gaps within the checkout fragment
 * { "within_fragment": "checkout" }
 *
 * // Every gap above 10 seconds
 * { "min_duration_ms": 10000 }
 * }</pre>
 *
 * @param after           lower bound — the gap starts immediately after a message matching
 *                        this pattern
 * @param before          upper bound — the gap ends immediately before a message matching
 *                        this pattern
 * @param withinFragment  name of a {@link FragmentDefinition} whose resolved span must
 *                        contain the gap
 * @param minDurationMs   only select gaps at least this long (milliseconds, inclusive)
 * @param maxDurationMs   only select gaps at most this long (milliseconds, inclusive)
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record GapSelector(

        @JsonProperty("after")
        MessagePattern after,

        @JsonProperty("before")
        MessagePattern before,

        @JsonProperty("within_fragment")
        String withinFragment,

        @JsonProperty("min_duration_ms")
        Long minDurationMs,

        @JsonProperty("max_duration_ms")
        Long maxDurationMs
) {

    public GapSelector {
        if (after == null && before == null && withinFragment == null) {
            throw new IllegalArgumentException(
                "GapSelector must specify at least one of: after, before, within_fragment");
        }
    }
}
