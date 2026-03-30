package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.Map;

/** Aggregate statistics for a single entity derived from its cassette. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EntityStats(
        String entityType,
        String entityId,
        long messageCount,
        Instant firstMessage,
        Instant lastMessage,
        Instant firstSeen,
        Instant lastSeen,
        Map<String, Long> countByTopic
) {}
