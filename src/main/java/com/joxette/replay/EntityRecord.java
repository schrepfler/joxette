package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.List;

/**
 * A single deduplicated row from an entity cassette ({@code lake.entity_{type}}).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EntityRecord(
        String entityId,
        int entityBucket,
        String topic,
        int partition,
        long offset,
        Instant timestamp,
        Instant recordedAt,
        String key,
        String value,
        List<CassetteRecord.Header> headers
) {}
