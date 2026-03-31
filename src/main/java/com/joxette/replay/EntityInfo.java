package com.joxette.replay;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

/** A row from {@code lake.known_entities}. */
@Schema(description = "A known entity registered in the entity registry (`lake.known_entities`).",
        example = """
            {
              "entityType": "customer",
              "entityId": "cust-042",
              "entityBucket": 5,
              "firstSeen": "2024-01-10T08:00:00Z",
              "lastSeen": "2024-06-01T12:00:00Z"
            }""")
public record EntityInfo(
        @Schema(description = "Entity type name", example = "customer")
        String entityType,

        @Schema(description = "Entity identifier", example = "cust-042")
        String entityId,

        @Schema(description = "Consistent hash bucket for the entity ID", example = "5")
        int entityBucket,

        @Schema(description = "Timestamp of the earliest recorded event for this entity",
                example = "2024-01-10T08:00:00Z")
        Instant firstSeen,

        @Schema(description = "Timestamp of the most recent recorded event for this entity",
                example = "2024-06-01T12:00:00Z")
        Instant lastSeen
) {}
