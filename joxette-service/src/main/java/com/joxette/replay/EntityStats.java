package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.Map;

/** Aggregate statistics for a single entity derived from its cassette. */
@Schema(description = "Aggregate statistics for a single entity derived from its entity cassette. " +
                       "Message counts are based on deduplicated records only.",
        example = """
            {
              "entityType": "customer",
              "entityId": "cust-042",
              "messageCount": 17,
              "firstMessage": "2024-01-15T09:00:00Z",
              "lastMessage": "2024-06-01T10:00:00Z",
              "firstSeen": "2024-01-15T09:00:01Z",
              "lastSeen": "2024-06-01T10:00:01Z",
              "countByTopic": {
                "customer-events": 12,
                "customer-orders": 5
              },
              "fileCount": 3,
              "objectStoreDirectory": "s3://joxette-data/main/entity_customer/",
              "storageConsoleUrl": "http://localhost:9001/rustfs/console/browser/?bucket=joxette-data&key=main%2Fentity_customer%2F"
            }""")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EntityStats(
        @Schema(description = "Entity type name", example = "customer")
        String entityType,

        @Schema(description = "Entity identifier", example = "cust-042")
        String entityId,

        @Schema(description = "Total number of deduplicated messages recorded for this entity", example = "17")
        long messageCount,

        @Schema(description = "Timestamp of the earliest deduplicated message for this entity",
                example = "2024-01-15T09:00:00Z")
        Instant firstMessage,

        @Schema(description = "Timestamp of the most recent deduplicated message for this entity",
                example = "2024-06-01T10:00:00Z")
        Instant lastMessage,

        @Schema(description = "Timestamp when this entity was first registered in the entity registry",
                example = "2024-01-15T09:00:01Z")
        Instant firstSeen,

        @Schema(description = "Timestamp when this entity was last seen in the entity registry",
                example = "2024-06-01T10:00:01Z")
        Instant lastSeen,

        @Schema(description = "Deduplicated message count broken down by source Kafka topic",
                example = "{\"customer-events\": 12, \"customer-orders\": 5}")
        Map<String, Long> countByTopic,

        @Schema(description = "Exact number of physical Parquet files that contain at least one row " +
                               "for this entity. 0 if the entity's data is still fully inlined in the " +
                               "catalog (not yet flushed) or if object storage isn't configured.",
                example = "3")
        int fileCount,

        @Schema(description = "Object-store directory this entity type's Parquet files live in " +
                               "(shared with every other entity of the same type — storage is not " +
                               "physically partitioned per entity). Null if object storage isn't configured.",
                example = "s3://joxette-data/main/entity_customer/")
        String objectStoreDirectory,

        @Schema(description = "Deep link into a storage console's file browser for objectStoreDirectory, " +
                               "if joxette.storage-console.url-template is configured. Null otherwise.",
                example = "http://localhost:9001/rustfs/console/browser/?bucket=joxette-data&key=main%2Fentity_customer%2F")
        String storageConsoleUrl
) {}
