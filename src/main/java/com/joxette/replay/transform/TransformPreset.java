package com.joxette.replay.transform;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.List;

/**
 * A named transform pipeline preset stored in the {@code transform_presets} plain-DuckDB table.
 *
 * <p>Presets let users save a reusable pipeline once and reference it by name in replay
 * requests via the {@code transform_preset} query parameter, avoiding repetition of large
 * JSON arrays.
 */
@Schema(description = "A named transform pipeline preset that can be referenced by name in replay requests.")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record TransformPreset(

        @Schema(description = "Unique preset name", example = "staging-sanitize")
        String name,

        @Schema(description = "Optional human-readable description",
                example = "Redact PII fields and redirect to staging topic")
        String description,

        @Schema(description = "Ordered list of transform pipeline steps")
        List<TransformStep> steps,

        @Schema(description = "When this preset was created (UTC)")
        Instant createdAt,

        @Schema(description = "When this preset was last updated (UTC)")
        Instant updatedAt
) {}
