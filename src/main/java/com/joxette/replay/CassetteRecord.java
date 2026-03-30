package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.List;

/**
 * A single row from the general cassette ({@code lake.cassette}).
 *
 * <p>{@code value} and each header {@code value} are base64url-encoded (no
 * padding) because they are raw bytes from the Kafka wire format. A {@code null}
 * field means the column was NULL in DuckDB.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CassetteRecord(
        String topic,
        int partition,
        long offset,
        Instant timestamp,
        Instant recordedAt,
        String key,
        String value,
        List<Header> headers
) {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Header(String key, String value) {}
}
