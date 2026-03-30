package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * Cursor-paginated response wrapper.
 *
 * <p>{@code nextCursor} is {@code null} (and omitted from JSON) when there are
 * no further pages. {@code hasMore} is a convenience boolean mirroring whether
 * {@code nextCursor} is present.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record PagedResponse<T>(
        List<T> data,
        String nextCursor,
        boolean hasMore
) {}
