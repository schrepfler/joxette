package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.List;

/** A row from {@code known_entities}. */
@Schema(description = "A known entity registered in the entity registry (`known_entities`).",
        example = """
            {
              "entityType": "customer",
              "entityId": "cust-042",
              "firstSeen": "2024-01-10T08:00:00Z",
              "lastSeen": "2024-06-01T12:00:00Z",
              "messageCount": 142,
              "sourceTopics": ["customer-events"],
              "lastMessageType": "account_updated"
            }""")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EntityInfo(
        @Schema(description = "Entity type name", example = "customer")
        String entityType,

        @Schema(description = "Entity identifier", example = "cust-042")
        String entityId,

        @Schema(description = "Timestamp of the earliest recorded event for this entity",
                example = "2024-01-10T08:00:00Z")
        Instant firstSeen,

        @Schema(description = "Timestamp of the most recent recorded event for this entity",
                example = "2024-06-01T12:00:00Z")
        Instant lastSeen,

        @Schema(description = "Total number of messages recorded for this entity", example = "142")
        long messageCount,

        @Schema(description = "Kafka topics that have produced events for this entity",
                example = "[\"customer-events\"]")
        List<String> sourceTopics,

        @Schema(description = "Message type of the most recently recorded event",
                example = "account_updated")
        String lastMessageType
) {}
