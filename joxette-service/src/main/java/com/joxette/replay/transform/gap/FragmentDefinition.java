package com.joxette.replay.transform.gap;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A named phase-capture definition that identifies a region of the cassette
 * bounded by two {@link MessagePattern} anchors.
 *
 * <p>Fragments are defined at preset level and resolved per-cassette during
 * replay. Once both {@code from} and {@code to} anchors match, the fragment
 * span is cached in {@code TransformContext} and available to {@link GapSelector}
 * via {@code within_fragment}.
 *
 * <p>Example:
 * <pre>{@code
 * {
 *   "name": "checkout",
 *   "label": "Checkout Phase",
 *   "color": "#4f8ef7",
 *   "from": { "predicate": { "field": "$.value.type", "operator": "EQ", "value": "OrderCreated" },
 *              "quantifier": "first" },
 *   "to":   { "predicate": { "field": "$.value.type", "operator": "EQ", "value": "PaymentCompleted" },
 *              "quantifier": "first" },
 *   "if":   { "max_duration_ms": 30000 }
 * }
 * }</pre>
 *
 * @param name      unique identifier used by {@link GapSelector#withinFragment()} references
 * @param label     human-readable display name for UI rendering
 * @param color     hex colour string for timeline band (e.g. {@code "#4f8ef7"})
 * @param from      pattern identifying the start anchor of this fragment
 * @param to        pattern identifying the end anchor of this fragment
 * @param ifClause  optional timing constraint; fragments that violate it are not resolved
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record FragmentDefinition(

        @JsonProperty("name")
        String name,

        @JsonProperty("label")
        String label,

        @JsonProperty("color")
        String color,

        @JsonProperty("from")
        MessagePattern from,

        @JsonProperty("to")
        MessagePattern to,

        @JsonProperty("if")
        IfClause ifClause
) {

    /**
     * Optional timing constraint applied after both anchors are resolved.
     * A fragment that does not satisfy the clause is treated as unresolved.
     *
     * @param minDurationMs minimum span duration in milliseconds (inclusive), or {@code null}
     * @param maxDurationMs maximum span duration in milliseconds (inclusive), or {@code null}
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record IfClause(
            @JsonProperty("min_duration_ms") Long minDurationMs,
            @JsonProperty("max_duration_ms") Long maxDurationMs
    ) {}
}
