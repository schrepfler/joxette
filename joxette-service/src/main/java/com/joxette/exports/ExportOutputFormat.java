package com.joxette.exports;

import com.fasterxml.jackson.annotation.JsonValue;

/** Output format for an async export job. */
public enum ExportOutputFormat {
    PARQUET("parquet"),
    NDJSON("ndjson");

    private final String value;

    ExportOutputFormat(String value) { this.value = value; }

    @JsonValue
    public String getValue() { return value; }
}
