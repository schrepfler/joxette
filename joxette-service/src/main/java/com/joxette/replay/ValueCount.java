package com.joxette.replay;

import io.swagger.v3.oas.annotations.media.Schema;

/** One (value, count) pair in a {@link CassetteSummary} dimension breakdown. */
public record ValueCount(
    @Schema(description = "The dimension value, null if the underlying field was absent on matching records, "
                         + "or the literal \"__other__\" for the folded remainder beyond the top 20 values.",
            example = "3")
    String value,
    @Schema(description = "Number of deduplicated records with this value", example = "40213")
    long count
) {}
