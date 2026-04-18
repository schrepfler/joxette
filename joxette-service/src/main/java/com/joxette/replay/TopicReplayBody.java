package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.joxette.replay.transform.TransformStep;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.List;

/**
 * Request body for POST-style topic cassette replay endpoints.
 *
 * <p>POST variants exist alongside the GET endpoints to support large transform
 * pipelines that would be impractical to URL-encode as a query parameter.
 *
 * <p>Exactly one of {@code transform} or {@code transformPreset} may be specified.
 * If neither is present the replay runs with the default metadata-only pipeline.
 */
@Schema(description = "Request body for POST-style topic cassette replay. " +
                       "Supports all GET replay filters plus an inline transform pipeline or preset reference.")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record TopicReplayBody(

        @Schema(description = "Include only records with timestamp >= this value (ISO-8601 instant)")
        Instant from,

        @Schema(description = "Include only records with timestamp <= this value (ISO-8601 instant)")
        Instant to,

        @Schema(description = "Filter to a single Kafka partition number")
        Integer partition,

        @Schema(description = "Include only records with Kafka offset >= this value", name = "offset_from")
        Long offsetFrom,

        @Schema(description = "Include only records with Kafka offset <= this value", name = "offset_to")
        Long offsetTo,

        @Schema(description = "Maximum number of records to return per page (default 100)", example = "100")
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
        Long startDelayMs
) {
    /** Normalises null {@code limit} to the default page size. */
    public TopicReplayBody {
        if (limit == null) limit = 100;
    }
}
