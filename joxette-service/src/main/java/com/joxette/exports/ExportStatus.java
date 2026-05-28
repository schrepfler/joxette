package com.joxette.exports;

import com.fasterxml.jackson.annotation.JsonValue;

public enum ExportStatus {
    PENDING("pending"),
    RUNNING("running"),
    COMPLETED("completed"),
    FAILED("failed");

    private final String value;

    ExportStatus(String value) { this.value = value; }

    @JsonValue
    public String getValue() { return value; }
}
