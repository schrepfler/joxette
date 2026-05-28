package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Controls the overall shape of the entity replay JSON response.
 *
 * <dl>
 *   <dt>{@code events} (default)</dt>
 *   <dd>Cursor-paginated array of {@link EntityRecord} objects — the current behaviour.</dd>
 *   <dt>{@code timeline}</dt>
 *   <dd>Events grouped into time buckets. Granularity is auto-selected from the
 *       time span (minutes &lt;1h, hours &lt;7d, days ≥7d) or overridden via
 *       {@code timeline_bucket=minute|hour|day}.</dd>
 *   <dt>{@code portrait}</dt>
 *   <dd>Compact entity summary: event count, topic breakdown, first/last seen,
 *       and a preview of the 3 most recent events. Optionally includes
 *       {@code currentState} when {@code output=state} is also set.</dd>
 * </dl>
 *
 * <p>Only meaningful for JSON ({@code application/json}) responses.
 * SSE and NDJSON streaming endpoints always use the {@code events} format.
 */
public enum ResponseFormat {
    EVENTS("events"),
    TIMELINE("timeline"),
    PORTRAIT("portrait");

    private final String value;

    ResponseFormat(String value) { this.value = value; }

    @JsonValue
    public String getValue() { return value; }

    @JsonCreator
    public static ResponseFormat parse(String raw) {
        if (raw == null) return EVENTS;
        for (ResponseFormat v : values()) if (v.value.equalsIgnoreCase(raw.trim())) return v;
        throw new IllegalArgumentException("Invalid response_format '" + raw + "'");
    }
}
