package com.joxette.replay;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

/**
 * Applies optional message transformations to cassette records during replay-to-topic.
 *
 * <p>Each instance is <em>stateful and single-use</em>: create one per replay invocation
 * (never share across replays), because the restamp logic captures the current time
 * and lazily computes the timestamp offset from the first record seen.
 *
 * <h2>Restamp</h2>
 * <p>Shifts every record's {@code kafka_timestamp} by a fixed offset so the first
 * message in the stream arrives with {@code timestamp = startNow} (captured at
 * construction), while all inter-message gaps remain identical to the original.
 * The offset is computed lazily on the first record:
 * {@code offset = startNow − firstRecordTimestamp}.
 *
 * <h2>Field substitution</h2>
 * <p>For each {@link FieldSubstitution} rule, the value at the given JSONPath in
 * the message body is replaced with either a literal string or a freshly generated
 * UUID4 (per message, per rule).  If the path is absent in a message the rule is
 * silently skipped.  If the body is null or not valid JSON the entire substitution
 * step is silently bypassed and the original value is preserved.
 *
 * <p>The {@code key} field is never mutated by substitution rules.
 */
public class MessageTransformer {

    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();

    private final boolean restamp;
    private final List<FieldSubstitution> substitutions;
    private final Instant startNow;

    /** Lazily initialised on first record when restamp is enabled. */
    private Duration restampOffset;

    /**
     * Creates a transformer for a single replay invocation.
     *
     * @param config transform configuration; must not be null
     */
    public MessageTransformer(ReplayTransformConfig config) {
        this.restamp       = config.restamp();
        this.substitutions = config.fieldSubstitutions();
        this.startNow      = Instant.now();
    }

    // =========================================================================
    // Public API
    // =========================================================================

    /**
     * Applies all configured transforms to a {@link CassetteRecord}.
     * Returns the original instance if no fields changed, otherwise a new record.
     */
    public CassetteRecord transform(CassetteRecord record) {
        Instant newTs  = restamp ? shifted(record.timestamp()) : record.timestamp();
        String  newVal = applySubstitutions(record.value());

        if (newTs == record.timestamp() && newVal == record.value()) {
            return record;
        }
        return new CassetteRecord(
                record.topic(), record.partition(), record.offset(),
                newTs, record.recordedAt(),
                record.key(), newVal, record.headers(), record.messageType());
    }

    /**
     * Applies all configured transforms to an {@link EntityRecord}.
     * Returns the original instance if no fields changed, otherwise a new record.
     */
    public EntityRecord transform(EntityRecord record) {
        Instant newTs  = restamp ? shifted(record.timestamp()) : record.timestamp();
        String  newVal = applySubstitutions(record.value());

        if (newTs == record.timestamp() && newVal == record.value()) {
            return record;
        }
        return new EntityRecord(
                record.entityId(), record.messageType(),
                record.topic(), record.partition(), record.offset(),
                newTs, record.recordedAt(),
                record.key(), newVal, record.headers());
    }

    // =========================================================================
    // Restamp
    // =========================================================================

    private Instant shifted(Instant original) {
        if (restampOffset == null) {
            restampOffset = Duration.between(original, startNow);
        }
        return original.plus(restampOffset);
    }

    // =========================================================================
    // Field substitution
    // =========================================================================

    /**
     * Applies all field-substitution rules to a base64url-encoded JSON value.
     *
     * @param base64Value base64url-encoded message value (may be null)
     * @return transformed base64url string, or the original if nothing changed or
     *         the value is not parseable JSON
     */
    private String applySubstitutions(String base64Value) {
        if (substitutions.isEmpty() || base64Value == null) {
            return base64Value;
        }
        try {
            String json = new String(DECODER.decode(base64Value), StandardCharsets.UTF_8);
            DocumentContext ctx = JsonPath.parse(json);

            for (FieldSubstitution sub : substitutions) {
                String replacement = sub.value() != null
                        ? sub.value()
                        : UUID.randomUUID().toString();
                try {
                    ctx.set(sub.path(), replacement);
                } catch (PathNotFoundException ignored) {
                    // path absent in this message — skip silently
                }
            }

            return ENCODER.encodeToString(ctx.jsonString().getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            // non-JSON body or decoding failure — return original unchanged
            return base64Value;
        }
    }
}
