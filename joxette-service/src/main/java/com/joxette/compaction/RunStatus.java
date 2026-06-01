package com.joxette.compaction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/** Terminal and in-progress status for a compaction or retention run. */
public enum RunStatus {
    RUNNING("running"),
    COMPLETED("completed"),
    FAILED("failed");

    private final String value;

    RunStatus(String value) { this.value = value; }

    @JsonValue
    public String getValue() { return value; }

    @JsonCreator
    public static RunStatus fromValue(String value) {
        for (RunStatus s : values()) {
            if (s.value.equalsIgnoreCase(value)) return s;
        }
        throw new IllegalArgumentException("Unknown RunStatus: " + value);
    }

    @Override
    public String toString() { return value; }
}
