package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.joxette.replay.transform.TransformStep;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.List;

/**
 * Request body for POST-style entity cassette replay endpoints.
 *
 * <p>POST variants exist alongside the GET endpoints to support large transform
 * pipelines that would be impractical to URL-encode as a query parameter.
 *
 * <p>Exactly one of {@code transform} or {@code transformPreset} may be specified.
 * If neither is present the replay runs with the default metadata-only pipeline.
 */
@Schema(description = "Request body for POST-style entity cassette replay. " +
                       "Supports all GET replay filters plus an inline transform pipeline or preset reference.")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EntityReplayBody(

        @Schema(description = "Include only events with timestamp >= this value (ISO-8601 instant)")
        Instant from,

        @Schema(description = "Include only events with timestamp <= this value (ISO-8601 instant)")
        Instant to,

        @Schema(description = "Maximum number of events to return per page (default 100)", example = "100")
        Integer limit,

        @Schema(description = "Opaque cursor from a previous response's `nextCursor` field")
        String cursor,

        @Schema(description = "Inline transform pipeline steps. Mutually exclusive with `transformPreset`.")
        List<TransformStep> transform,

        @Schema(description = "Name of a saved transform preset. Mutually exclusive with `transform`.",
                example = "staging-sanitize")
        String transformPreset,

        @Schema(description = "ISO-8601 absolute timestamp at which streaming begins " +
                              "(mutually exclusive with `startDelayMs`)",
                name = "start_at")
        Instant startAt,

        @Schema(description = "Relative delay in milliseconds before streaming begins " +
                              "(mutually exclusive with `startAt`)",
                name = "start_delay_ms")
        Long startDelayMs,

        @Schema(description = "Restrict to events with one of the given message type labels. " +
                              "Maps directly to a SQL `WHERE message_type IN (...)` filter with no " +
                              "post-materialisation overhead. Empty or null means no restriction.",
                name = "message_types",
                example = "[\"OrderCreated\", \"OrderPaid\"]")
        List<String> messageTypes,

        @Schema(description = "SOL (Sequence Operations Language) query to run over the entity's full event " +
                              "sequence before returning results. When present the entire sequence is loaded, " +
                              "processed by the SOL engine, then `solOutput` controls what is returned.",
                example = "match OrderCreated(created) >> * >> OrderPaid(paid)\nif duration(created, paid) < 24h")
        String sol,

        @Schema(description = "Controls what is returned when `sol` is present. " +
                              "`events` (default) = only the events surviving the SOL pipeline; " +
                              "`annotated` = all events with SOL match tags injected as headers; " +
                              "`summary` = only the match metadata, no events.",
                name = "sol_output",
                defaultValue = "events")
        SolOutput solOutput,

        @Schema(description = "Return only the last N events regardless of time window. " +
                              "Mutually exclusive with `from`, `to`, and `cursor`. " +
                              "Result is in chronological order.",
                name = "last_n",
                example = "50")
        Integer lastN,

        @Schema(description = "Deduplication policy: " +
                              "`offset` (default) = one copy per (topic, partition, offset); " +
                              "`value` = one copy per (topic, value) — for idempotent producers; " +
                              "`none` = no deduplication, all rows including re-deliveries.",
                defaultValue = "offset")
        DedupPolicy dedup
) {
    /** Normalises null {@code limit} to the default page size and {@code solOutput} to EVENTS. */
    public EntityReplayBody {
        if (limit == null) limit = 100;
        if (solOutput == null) solOutput = SolOutput.EVENTS;
    }
}
