package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

/**
 * An {@link EntityRecord} annotated with the fields that changed relative to the
 * prior event and the state those fields held before this event.
 *
 * <p>Produced by {@link DiffService} when {@code output=diff} is requested.
 * All fields from the underlying {@code EntityRecord} are present; {@code changedFields}
 * and {@code before} are only populated when the state actually changed.
 */
@Schema(description = "An entity event annotated with the fields that changed relative to the prior event.")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DiffRecord(
        EntityRecord event,

        @Schema(description = "JSON Pointer paths of top-level fields that changed in this event. " +
                              "Null for the first event (no prior state to compare against).",
                example = "[\"/status\", \"/amount\"]")
        List<String> changedFields,

        @Schema(description = "The values of the changed fields before this event was applied. " +
                              "Null for the first event.",
                example = "{\"status\": \"pending\", \"amount\": 10}")
        ObjectNode before
) {}
