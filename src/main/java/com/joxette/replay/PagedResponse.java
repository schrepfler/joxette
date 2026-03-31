package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

/**
 * Cursor-paginated response wrapper.
 *
 * <p>{@code nextCursor} is {@code null} (and omitted from JSON) when there are
 * no further pages. {@code hasMore} is a convenience boolean mirroring whether
 * {@code nextCursor} is present.
 */
@Schema(description = "Cursor-paginated response wrapper. " +
                       "`nextCursor` is omitted when `hasMore` is false (last page).")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record PagedResponse<T>(
        @Schema(description = "Records on this page")
        List<T> data,

        @Schema(description = "Opaque cursor to pass as the `cursor` query parameter to retrieve the next page. " +
                              "Absent on the last page.",
                example = "eyJ0cyI6IjIwMjQtMDYtMDFUMTI6MDA6MDAuMTIzWiIsIm8iOjEwMjR9")
        String nextCursor,

        @Schema(description = "True when more pages are available after this one.", example = "true")
        boolean hasMore
) {}
