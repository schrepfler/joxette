package com.joxette.replay;

import java.time.Instant;

/** A row from {@code lake.known_entities}. */
public record EntityInfo(
        String entityType,
        String entityId,
        int entityBucket,
        Instant firstSeen,
        Instant lastSeen
) {}
