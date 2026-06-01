package com.joxette.compaction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/** What initiated a compaction or retention run. */
public enum TriggerSource {
    SCHEDULED("scheduled"),
    MANUAL("manual");

    private final String value;

    TriggerSource(String value) { this.value = value; }

    @JsonValue
    public String getValue() { return value; }

    @JsonCreator
    public static TriggerSource fromValue(String value) {
        for (TriggerSource t : values()) {
            if (t.value.equalsIgnoreCase(value)) return t;
        }
        throw new IllegalArgumentException("Unknown TriggerSource: " + value);
    }

    @Override
    public String toString() { return value; }
}
