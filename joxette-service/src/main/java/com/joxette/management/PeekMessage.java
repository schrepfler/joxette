package com.joxette.management;

import java.util.List;

public record PeekMessage(
    int partition,
    long offset,
    String timestamp,          // ISO-8601
    String key,                // null if no key
    String value,              // UTF-8 string or base64 if binary
    String valueEncoding,      // "utf8" or "base64"
    List<HeaderEntry> headers
) {
    public record HeaderEntry(String key, String value) {}
}
