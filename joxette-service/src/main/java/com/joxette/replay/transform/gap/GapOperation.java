package com.joxette.replay.transform.gap;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Sealed hierarchy of gap-editing operations, dispatched by the {@code "op"} discriminator.
 *
 * <p>Available operations:
 * <ul>
 *   <li>{@link Cut}  — eliminate the gap entirely</li>
 *   <li>{@link Hold} — set gap to an exact target duration</li>
 *   <li>{@link Trim} — shorten gap by a fixed amount or factor (exactly one must be set)</li>
 *   <li>{@link Pad}  — extend gap by a fixed amount</li>
 *   <li>{@link Scale}— multiply gap duration by a factor</li>
 * </ul>
 *
 * <p>Examples:
 * <pre>{@code
 * { "op": "cut" }
 * { "op": "hold",  "target_ms": 500 }
 * { "op": "trim",  "by_ms": 2000 }
 * { "op": "trim",  "by_factor": 0.5 }
 * { "op": "pad",   "by_ms": 1000 }
 * { "op": "scale", "factor": 0.1 }
 * }</pre>
 */
@JsonTypeInfo(
    use     = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "op"
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = GapOperation.Cut.class,   name = "cut"),
    @JsonSubTypes.Type(value = GapOperation.Hold.class,  name = "hold"),
    @JsonSubTypes.Type(value = GapOperation.Trim.class,  name = "trim"),
    @JsonSubTypes.Type(value = GapOperation.Pad.class,   name = "pad"),
    @JsonSubTypes.Type(value = GapOperation.Scale.class, name = "scale")
})
public sealed interface GapOperation
        permits GapOperation.Cut, GapOperation.Hold, GapOperation.Trim,
                GapOperation.Pad, GapOperation.Scale {

    /** Removes the gap entirely — the following message plays immediately. */
    record Cut() implements GapOperation {}

    /**
     * Sets the gap to an exact duration.
     *
     * @param targetMs target gap duration in milliseconds
     */
    record Hold(@JsonProperty("target_ms") long targetMs) implements GapOperation {}

    /**
     * Shortens the gap. Exactly one of {@code byMs} or {@code byFactor} must be non-null.
     *
     * @param byMs     fixed number of milliseconds to subtract (must not make gap negative)
     * @param byFactor fractional multiplier to subtract from 1 — e.g. {@code 0.5} halves the gap
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record Trim(
            @JsonProperty("by_ms")     Long   byMs,
            @JsonProperty("by_factor") Double byFactor
    ) implements GapOperation {

        public Trim {
            if ((byMs == null) == (byFactor == null)) {
                throw new IllegalArgumentException(
                    "Trim requires exactly one of by_ms or by_factor to be set");
            }
        }
    }

    /**
     * Extends the gap by a fixed amount.
     *
     * @param byMs milliseconds to add
     */
    record Pad(@JsonProperty("by_ms") long byMs) implements GapOperation {}

    /**
     * Multiplies the gap duration by a factor.
     *
     * @param factor multiplier (e.g. {@code 0.1} compresses to 10%, {@code 2.0} doubles)
     */
    record Scale(@JsonProperty("factor") double factor) implements GapOperation {}
}
