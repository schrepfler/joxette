package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Controls what the SOL engine produces when wired into a replay endpoint.
 *
 * <dl>
 *   <dt>{@code events}</dt>
 *   <dd>Return only the events that survive the SOL pipeline (matched / filtered subset).</dd>
 *   <dt>{@code annotated}</dt>
 *   <dd>Return all events with SOL match tags injected as headers
 *       ({@code __sol_match}, {@code __sol_tag:<name>}, {@code __sol_elapsed_ms:<name>}).
 *       Events that did not participate in any tag carry none of these headers.</dd>
 *   <dt>{@code summary}</dt>
 *   <dd>Return only the {@link SolMatchService.SolMatchResult} metadata — no events.
 *       For SSE this is a terminal {@code sol_summary} event; for JSON a single summary object.</dd>
 * </dl>
 */
public enum SolOutput {
    EVENTS("events"),
    ANNOTATED("annotated"),
    SUMMARY("summary");

    private final String value;

    SolOutput(String value) { this.value = value; }

    @JsonValue
    public String getValue() { return value; }

    @JsonCreator
    public static SolOutput parse(String raw) {
        if (raw == null) return EVENTS;
        for (SolOutput v : values()) if (v.value.equalsIgnoreCase(raw.trim())) return v;
        throw new IllegalArgumentException("Invalid sol_output '" + raw + "'");
    }
}
