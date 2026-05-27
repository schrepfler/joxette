package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Controls the shape of the entity replay response.
 *
 * <dl>
 *   <dt>{@code events} (default)</dt>
 *   <dd>Stream of {@link EntityRecord} objects — the current behaviour.</dd>
 *   <dt>{@code state}</dt>
 *   <dd>Folds the event stream into a single JSON object representing the
 *       entity's current (or point-in-time) state. The fold strategy is
 *       controlled by {@link StateFoldStrategy}.</dd>
 *   <dt>{@code diff}</dt>
 *   <dd>Returns each event annotated with the fields that changed relative
 *       to the accumulated state, plus their prior values.</dd>
 * </dl>
 */
public enum ReplayOutputMode {
    EVENTS("events"),
    STATE("state"),
    DIFF("diff");

    private final String value;

    ReplayOutputMode(String value) { this.value = value; }

    @JsonValue
    public String getValue() { return value; }
}
