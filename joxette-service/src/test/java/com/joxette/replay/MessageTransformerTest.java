package com.joxette.replay;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

class MessageTransformerTest {

    private static final Base64.Encoder ENC = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DEC = Base64.getUrlDecoder();

    private static String b64(String json) {
        return ENC.encodeToString(json.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String b64) {
        return new String(DEC.decode(b64), StandardCharsets.UTF_8);
    }

    private static CassetteRecord record(Instant ts, String value) {
        return new CassetteRecord("orders", 0, 1L, ts, ts, "key-1", value, null, null);
    }

    private static EntityRecord entityRecord(Instant ts, String value) {
        return new EntityRecord("entity-1", "orderEvent", "orders", 0, 1L, ts, ts,
                "key-1", value, null);
    }

    // =========================================================================
    // ReplayTransformConfig validation
    // =========================================================================

    @Test
    void config_identityWhenNoTransforms() {
        assertThat(ReplayTransformConfig.NONE.isIdentity()).isTrue();
        assertThat(new ReplayTransformConfig(false, List.of()).isIdentity()).isTrue();
    }

    @Test
    void config_notIdentityWhenRestamp() {
        assertThat(new ReplayTransformConfig(true, List.of()).isIdentity()).isFalse();
    }

    @Test
    void config_notIdentityWhenSubstitutions() {
        var sub = new FieldSubstitution("$.x", "v", null);
        assertThat(new ReplayTransformConfig(false, List.of(sub)).isIdentity()).isFalse();
    }

    // =========================================================================
    // FieldSubstitution validation
    // =========================================================================

    @Test
    void fieldSubstitution_rejectsBlankPath() {
        assertThatThrownBy(() -> new FieldSubstitution("", "v", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("path");
    }

    @Test
    void fieldSubstitution_rejectsMissingValueAndGenerate() {
        assertThatThrownBy(() -> new FieldSubstitution("$.x", null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fieldSubstitution_rejectsBothValueAndGenerate() {
        assertThatThrownBy(() -> new FieldSubstitution("$.x", "v", "uuid"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fieldSubstitution_rejectsUnknownGenerateStrategy() {
        assertThatThrownBy(() -> new FieldSubstitution("$.x", null, "snowflake"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("snowflake");
    }

    // =========================================================================
    // Identity transformer (no transforms configured)
    // =========================================================================

    @Test
    void noTransforms_returnsSameInstance() {
        var transformer = new MessageTransformer(ReplayTransformConfig.NONE);
        var r = record(Instant.now(), b64("{\"id\":\"1\"}"));
        assertThat(transformer.transform(r)).isSameAs(r);
    }

    @Test
    void noTransforms_entityReturnsSameInstance() {
        var transformer = new MessageTransformer(ReplayTransformConfig.NONE);
        var r = entityRecord(Instant.now(), b64("{\"id\":\"1\"}"));
        assertThat(transformer.transform(r)).isSameAs(r);
    }

    // =========================================================================
    // Restamp
    // =========================================================================

    @Test
    void restamp_firstRecordGetsCurrentTime() {
        var cfg = new ReplayTransformConfig(true, List.of());
        var transformer = new MessageTransformer(cfg);
        Instant before = Instant.now();

        Instant original = Instant.parse("2020-01-01T00:00:00Z");
        var r = record(original, null);
        CassetteRecord out = transformer.transform(r);

        Instant after = Instant.now();
        assertThat(out.timestamp()).isBetween(before.minusMillis(1), after);
    }

    @Test
    void restamp_preservesRelativeTiming() {
        var cfg = new ReplayTransformConfig(true, List.of());
        var transformer = new MessageTransformer(cfg);

        Instant t0 = Instant.parse("2020-01-01T00:00:00Z");
        Instant t1 = t0.plus(Duration.ofMinutes(5));
        Instant t2 = t0.plus(Duration.ofMinutes(10));

        CassetteRecord out0 = transformer.transform(record(t0, null));
        CassetteRecord out1 = transformer.transform(record(t1, null));
        CassetteRecord out2 = transformer.transform(record(t2, null));

        Duration gap01 = Duration.between(out0.timestamp(), out1.timestamp());
        Duration gap12 = Duration.between(out1.timestamp(), out2.timestamp());

        assertThat(gap01).isEqualTo(Duration.ofMinutes(5));
        assertThat(gap12).isEqualTo(Duration.ofMinutes(5));
    }

    @Test
    void restamp_entityPreservesRelativeTiming() {
        var cfg = new ReplayTransformConfig(true, List.of());
        var transformer = new MessageTransformer(cfg);

        Instant t0 = Instant.parse("2019-06-15T12:00:00Z");
        Instant t1 = t0.plus(Duration.ofSeconds(30));

        EntityRecord out0 = transformer.transform(entityRecord(t0, null));
        EntityRecord out1 = transformer.transform(entityRecord(t1, null));

        Duration gap = Duration.between(out0.timestamp(), out1.timestamp());
        assertThat(gap).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void restamp_doesNotMutateOtherFields() {
        var cfg = new ReplayTransformConfig(true, List.of());
        var transformer = new MessageTransformer(cfg);

        String val = b64("{\"x\":1}");
        var r = record(Instant.parse("2020-01-01T00:00:00Z"), val);
        CassetteRecord out = transformer.transform(r);

        assertThat(out.topic()).isEqualTo(r.topic());
        assertThat(out.partition()).isEqualTo(r.partition());
        assertThat(out.offset()).isEqualTo(r.offset());
        assertThat(out.key()).isEqualTo(r.key());
        assertThat(out.value()).isEqualTo(r.value());
    }

    // =========================================================================
    // Field substitution — literal value
    // =========================================================================

    @Test
    void substitution_literalReplacesField() {
        var sub = new FieldSubstitution("$.order_id", "test-order", null);
        var cfg = new ReplayTransformConfig(false, List.of(sub));
        var transformer = new MessageTransformer(cfg);

        String val = b64("{\"order_id\":\"orig-1\",\"status\":\"pending\"}");
        CassetteRecord out = transformer.transform(record(Instant.now(), val));

        String json = decode(out.value());
        assertThat(json).contains("\"test-order\"");
        assertThat(json).contains("\"status\":\"pending\"");
        assertThat(json).doesNotContain("\"orig-1\"");
    }

    @Test
    void substitution_absentPathIsSilentlySkipped() {
        var sub = new FieldSubstitution("$.nonexistent", "x", null);
        var cfg = new ReplayTransformConfig(false, List.of(sub));
        var transformer = new MessageTransformer(cfg);

        String val = b64("{\"order_id\":\"42\"}");
        CassetteRecord out = transformer.transform(record(Instant.now(), val));

        // value should be re-encoded but logically unchanged
        String json = decode(out.value());
        assertThat(json).contains("\"order_id\"");
    }

    @Test
    void substitution_nonJsonValuePassedThrough() {
        var sub = new FieldSubstitution("$.x", "y", null);
        var cfg = new ReplayTransformConfig(false, List.of(sub));
        var transformer = new MessageTransformer(cfg);

        // value is not valid JSON
        String val = b64("not-json-at-all");
        CassetteRecord out = transformer.transform(record(Instant.now(), val));

        assertThat(out.value()).isEqualTo(val);   // original preserved
    }

    @Test
    void substitution_nullValuePassedThrough() {
        var sub = new FieldSubstitution("$.x", "y", null);
        var cfg = new ReplayTransformConfig(false, List.of(sub));
        var transformer = new MessageTransformer(cfg);

        CassetteRecord out = transformer.transform(record(Instant.now(), null));
        assertThat(out.value()).isNull();
    }

    @Test
    void substitution_multipleRulesAppliedInOrder() {
        var sub1 = new FieldSubstitution("$.a", "first", null);
        var sub2 = new FieldSubstitution("$.b", "second", null);
        var cfg  = new ReplayTransformConfig(false, List.of(sub1, sub2));
        var transformer = new MessageTransformer(cfg);

        String val = b64("{\"a\":\"old-a\",\"b\":\"old-b\"}");
        CassetteRecord out = transformer.transform(record(Instant.now(), val));

        String json = decode(out.value());
        assertThat(json).contains("\"first\"");
        assertThat(json).contains("\"second\"");
        assertThat(json).doesNotContain("\"old-a\"").doesNotContain("\"old-b\"");
    }

    // =========================================================================
    // Field substitution — UUID generation
    // =========================================================================

    @Test
    void substitution_generateUuidIsValidUuid() {
        var sub = new FieldSubstitution("$.trace_id", null, "uuid");
        var cfg = new ReplayTransformConfig(false, List.of(sub));
        var transformer = new MessageTransformer(cfg);

        String val = b64("{\"trace_id\":\"old-trace\"}");
        CassetteRecord out = transformer.transform(record(Instant.now(), val));

        String json = decode(out.value());
        // Extract the value after "trace_id": strip surrounding JSON structure
        String uuidStr = json.replaceAll(".*\"trace_id\":\"([^\"]+)\".*", "$1");
        assertThat(UUID.fromString(uuidStr)).isNotNull();  // valid UUID — does not throw
        assertThat(uuidStr).isNotEqualTo("old-trace");
    }

    @Test
    void substitution_generateUuidDiffersPerMessage() {
        var sub = new FieldSubstitution("$.id", null, "uuid");
        var cfg = new ReplayTransformConfig(false, List.of(sub));
        var transformer = new MessageTransformer(cfg);

        String val = b64("{\"id\":\"orig\"}");
        CassetteRecord out1 = transformer.transform(record(Instant.now(), val));
        CassetteRecord out2 = transformer.transform(record(Instant.now(), val));

        String id1 = decode(out1.value()).replaceAll(".*\"id\":\"([^\"]+)\".*", "$1");
        String id2 = decode(out2.value()).replaceAll(".*\"id\":\"([^\"]+)\".*", "$1");
        assertThat(id1).isNotEqualTo(id2);
    }

    // =========================================================================
    // Restamp + substitution combined
    // =========================================================================

    @Test
    void restampAndSubstitution_bothApplied() {
        var sub = new FieldSubstitution("$.env", "test", null);
        var cfg = new ReplayTransformConfig(true, List.of(sub));
        var transformer = new MessageTransformer(cfg);

        Instant before = Instant.now();
        Instant original = Instant.parse("2018-03-01T00:00:00Z");
        String val = b64("{\"env\":\"prod\"}");
        CassetteRecord out = transformer.transform(record(original, val));

        // Timestamp should be near now (subtract 1 ms to tolerate sub-millisecond clock jitter)
        assertThat(out.timestamp()).isAfterOrEqualTo(before.minusMillis(1));
        // Field should be substituted
        assertThat(decode(out.value())).contains("\"test\"");
        assertThat(decode(out.value())).doesNotContain("\"prod\"");
    }
}
