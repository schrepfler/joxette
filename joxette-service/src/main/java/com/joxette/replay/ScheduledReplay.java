package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

/**
 * Snapshot of a pending or active scheduled replay.
 * Returned by {@code GET /cassettes/scheduled} and included in the 202 response
 * when a replay is submitted with {@code start_at} or {@code start_delay_ms}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ScheduledReplay(
        String id,
        /** {@code "topic"} or {@code "entity"} */
        String kind,
        /** Non-null when {@code kind = "topic"}. */
        String topic,
        /** Non-null when {@code kind = "entity"}. */
        String entityType,
        /** Non-null when {@code kind = "entity"}. */
        String entityId,
        Instant from,
        Instant to,
        /** Topic replays only. */
        Integer partition,
        /** Topic replays only. */
        Long offsetFrom,
        /** Topic replays only. */
        Long offsetTo,
        Instant scheduledAt,
        Instant createdAt,
        ScheduledReplayStatus status
) {}
