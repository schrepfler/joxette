package com.joxette.streams;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.joxette.replay.DedupPolicy;
import com.joxette.replay.ReplayOutputMode;
import com.joxette.replay.SolOutput;
import com.joxette.replay.StateFoldStrategy;
import com.joxette.replay.transform.TransformStep;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.List;

/**
 * A named derived stream — a stored, lazy query definition over an entity's
 * event history. Can be consumed as a bounded pull query ({@code GET /streams/{id}})
 * or a live push subscription ({@code GET /streams/{id}/events}).
 *
 * <p>A stream with a null {@code entityId} is an <em>entity-type stream</em>:
 * the entity ID is supplied at consumption time as a query parameter.
 */
@Schema(description = "Named derived stream definition. Stores all query, filter, " +
                       "transform, and output options under a reusable slug.")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record StreamDefinition(

        @Schema(description = "Unique slug (URL-safe, e.g. 'order-lifecycle')",
                example = "order-lifecycle")
        String id,

        @Schema(description = "Human-readable display name",
                example = "Order Lifecycle")
        String name,

        @Schema(description = "Entity type this stream operates on",
                example = "order")
        String entityType,

        @Schema(description = "Entity ID to bind at definition time. Null = entity-type stream " +
                              "(entity ID supplied at consumption time via ?entity_id=...)",
                example = "order-789")
        String entityId,

        @Schema(description = "Source filter options")
        SourceOptions source,

        @Schema(description = "SOL sequence-level expression. Null = no sequence processing.",
                example = "MATCH order_created THEN payment WITHIN 24h THEN shipped")
        String sol,

        @Schema(description = "Controls SOL output when `sol` is non-null",
                defaultValue = "events")
        SolOutput solOutput,

        @Schema(description = "Per-event transform pipeline steps applied after SOL")
        List<TransformStep> transform,

        @Schema(description = "Output mode: events | state",
                defaultValue = "events")
        ReplayOutputMode output,

        @Schema(description = "State fold strategy, only relevant when output=state",
                name = "state_fold")
        StateFoldStrategy stateFold,

        @Schema(description = "Timestamp at which this stream definition was created")
        Instant createdAt,

        @Schema(description = "Timestamp at which this stream definition was last updated")
        Instant updatedAt
) {

    /**
     * Source filter options embedded in a {@link StreamDefinition}.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record SourceOptions(

            @Schema(description = "Restrict to events with one of these message types. " +
                                  "Null or empty = all message types.",
                    name = "message_types",
                    example = "[\"OrderCreated\", \"OrderPaid\"]")
            List<String> messageTypes,

            @Schema(description = "Include only events with timestamp >= this value (ISO-8601)",
                    example = "2025-01-01T00:00:00Z")
            Instant from,

            @Schema(description = "Include only events with timestamp <= this value (ISO-8601)",
                    example = "2025-12-31T23:59:59Z")
            Instant to,

            @Schema(description = "Return only the last N events regardless of time window. " +
                                  "Mutually exclusive with from/to.",
                    name = "last_n",
                    example = "50")
            Integer lastN,

            @Schema(description = "Deduplication policy: offset | value | none",
                    defaultValue = "offset")
            DedupPolicy dedup
    ) {}
}
