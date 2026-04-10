package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

/**
 * Optional message transformation configuration for replay-to-topic operations.
 *
 * <h2>Restamp</h2>
 * <p>When {@code restamp: true}, all Kafka timestamps are shifted so the first message
 * in the replay stream has {@code timestamp = now} (the moment the replay begins), while
 * relative inter-message timing is preserved exactly.  This is useful when replaying
 * into a fresh environment where consumers use event-time windows anchored to recent time.
 *
 * <h2>Field substitutions</h2>
 * <p>Each entry in {@code fieldSubstitutions} replaces the value at a JSONPath location
 * in the message body.  Rules are applied in order; later rules may overwrite earlier ones
 * if their paths overlap.  The key field is never mutated by substitution rules.
 *
 * <p>If no transforms are needed, omit this object entirely from the request body.
 */
@Schema(description = """
        Optional transforms applied to each message before it is produced to the target topic.
        Omit the entire object (or leave it null) to replay messages verbatim.
        """,
        example = """
            {
              "restamp": true,
              "fieldSubstitutions": [
                {"path": "$.order_id",    "generate": "uuid"},
                {"path": "$.environment", "value":    "staging"}
              ]
            }""")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ReplayTransformConfig(

        @Schema(description = """
                Shift all Kafka timestamps so the first message timestamp = now, preserving
                relative inter-message timing. Defaults to false.
                """,
                defaultValue = "false")
        boolean restamp,

        @Schema(description = """
                Replace field values at JSONPath locations in the message body.
                Rules are applied in declaration order. Null or absent means no substitutions.
                """)
        List<FieldSubstitution> fieldSubstitutions

) {
    /** Compact constructor: normalise null list to empty so callers never see null. */
    public ReplayTransformConfig {
        if (fieldSubstitutions == null) {
            fieldSubstitutions = List.of();
        }
    }

    /** Sentinel representing "no transforms" — identity pass-through. */
    public static final ReplayTransformConfig NONE = new ReplayTransformConfig(false, List.of());

    /** Returns {@code true} when no transforms are configured, making this a no-op. */
    public boolean isIdentity() {
        return !restamp && fieldSubstitutions.isEmpty();
    }
}
