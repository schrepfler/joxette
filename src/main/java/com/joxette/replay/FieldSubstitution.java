package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

/**
 * One field-substitution rule applied during replay-to-topic.
 *
 * <p>Exactly one of {@code value} (literal string) or {@code generate} (auto-generation
 * strategy) must be set.  Substitutions are applied to the message body JSON via the
 * given JSONPath expression.  If the path is absent in a particular message the
 * substitution is silently skipped; if the value cannot be decoded as JSON the whole
 * message is passed through unchanged.
 *
 * <p>Example — literal replacement:
 * <pre>{@code {"path": "$.order_id", "value": "test-order-001"}}</pre>
 *
 * <p>Example — auto-generated UUID per message:
 * <pre>{@code {"path": "$.trace_id", "generate": "uuid"}}</pre>
 */
@Schema(description = """
        One field-substitution rule for replay-to-topic. Exactly one of `value` (literal) or
        `generate` (auto-generated) must be provided. The substitution is applied to the
        message JSON body at the given JSONPath location.
        """)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record FieldSubstitution(

        @Schema(description = "JSONPath expression identifying the field(s) to replace " +
                              "(e.g. `$.order_id`, `$.metadata.trace_id`).",
                requiredMode = Schema.RequiredMode.REQUIRED,
                example = "$.order_id")
        String path,

        @Schema(description = "Literal replacement value. Must be null when `generate` is set.",
                example = "test-order-001")
        String value,

        @Schema(description = "Auto-generation strategy. Currently only `uuid` is supported — " +
                              "a fresh UUID4 is generated per message so downstream systems " +
                              "create new entities rather than colliding with originals. " +
                              "Must be null when `value` is set.",
                example = "uuid")
        String generate

) {
    public FieldSubstitution {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("FieldSubstitution.path must not be blank");
        }
        boolean hasValue    = value    != null;
        boolean hasGenerate = generate != null;
        if (!hasValue && !hasGenerate) {
            throw new IllegalArgumentException(
                    "FieldSubstitution requires either 'value' or 'generate' (path=" + path + ")");
        }
        if (hasValue && hasGenerate) {
            throw new IllegalArgumentException(
                    "FieldSubstitution cannot have both 'value' and 'generate' (path=" + path + ")");
        }
        if (hasGenerate && !"uuid".equals(generate)) {
            throw new IllegalArgumentException(
                    "Unsupported generate strategy '" + generate + "'; only 'uuid' is supported");
        }
    }
}
