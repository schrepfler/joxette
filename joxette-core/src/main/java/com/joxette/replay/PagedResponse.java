package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;
import java.util.Map;

/**
 * Cursor-paginated response wrapper.
 *
 * <p>{@code nextCursor} is {@code null} (and omitted from JSON) when there are
 * no further pages. {@code hasMore} is a convenience boolean mirroring whether
 * {@code nextCursor} is present.
 *
 * <p>{@code transformApplied} is present (and {@code true}) when the response was
 * produced with a user-supplied transform pipeline.  Omitted when no pipeline was active.
 */
@Schema(description = "Cursor-paginated response wrapper. " +
                       "`nextCursor` is omitted when `hasMore` is false (last page). " +
                       "`transformApplied` is present only when a user transform pipeline was active.")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record PagedResponse<T>(
        @Schema(description = "Records on this page")
        List<T> data,

        @Schema(description = "Opaque cursor to pass as the `cursor` query parameter to retrieve the next page. " +
                              "Absent on the last page.",
                example = "eyJ0cyI6IjIwMjQtMDYtMDFUMTI6MDA6MDAuMTIzWiIsIm8iOjEwMjR9")
        String nextCursor,

        @Schema(description = "True when more pages are available after this one.", example = "true")
        boolean hasMore,

        @Schema(description = "Present and true when a user-supplied transform pipeline was applied to the records " +
                              "on this page. Omitted when no transform was active.",
                example = "true")
        Boolean transformApplied,

        @Schema(description = "SOL sequence match summary. Present only when a SOL query was supplied and " +
                              "`sol_output` is `annotated` or `events`. Contains `matched`, `tags` (name → " +
                              "{from,to} spans), `sequenceLength`, and any `unexpectedNulls`.")
        SolSummary solSummary
) {
    /**
     * Metadata returned alongside event records when a SOL query was applied.
     *
     * @param matched        whether the last MATCH found at least one occurrence
     * @param tags           tag name → half-open index range in the original sequence
     * @param sequenceLength total events in the pre-SOL sequence (denominator for coverage)
     * @param unexpectedNulls expressions that were null-cast during evaluation
     */
    public record SolSummary(
            boolean matched,
            Map<String, TagSpan> tags,
            int sequenceLength,
            List<String> unexpectedNulls
    ) {}

    /** A tag's half-open index range within the original sequence. */
    public record TagSpan(int from, int to) {}
}
