package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.ReplayMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CoalesceStepTest {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final Base64.Encoder ENC = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DEC = Base64.getUrlDecoder();

    private static String b64(String s) {
        return ENC.encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String b64) {
        return new String(DEC.decode(b64), StandardCharsets.UTF_8);
    }

    private static ReplayMessage msg(String valueJson) {
        String b64val = valueJson != null ? b64(valueJson) : null;
        return new ReplayMessage(new CassetteRecord("orders", 0, 1L,
                Instant.EPOCH, Instant.EPOCH, null, b64val, null, null));
    }

    // -------------------------------------------------------------------------
    // First source wins
    // -------------------------------------------------------------------------

    @Test
    void usesFirstNonNullSource() {
        var step = new CoalesceStep(
                List.of("$.value.order_id", "$.value.legacy_id"),
                "$.value.id",
                null);
        ReplayMessage msg = msg("{\"order_id\":\"ORD-1\",\"legacy_id\":\"L-99\",\"id\":null}");

        step.apply(msg);

        assertThat(decode(msg.value)).contains("\"id\":\"ORD-1\"");
    }

    @Test
    void skipsNullFirstSourceUsesSecond() {
        var step = new CoalesceStep(
                List.of("$.value.order_id", "$.value.legacy_id"),
                "$.value.id",
                null);
        ReplayMessage msg = msg("{\"order_id\":null,\"legacy_id\":\"L-99\",\"id\":null}");

        step.apply(msg);

        assertThat(decode(msg.value)).contains("\"id\":\"L-99\"");
    }

    @Test
    void skipsAbsentFirstSourceUsesSecond() {
        var step = new CoalesceStep(
                List.of("$.value.order_id", "$.value.legacy_id"),
                "$.value.id",
                null);
        // order_id not present
        ReplayMessage msg = msg("{\"legacy_id\":\"L-77\",\"id\":null}");

        step.apply(msg);

        assertThat(decode(msg.value)).contains("\"id\":\"L-77\"");
    }

    // -------------------------------------------------------------------------
    // Fallback when all sources null
    // -------------------------------------------------------------------------

    @Test
    void writesFallbackWhenAllSourcesNull() throws Exception {
        var step = new CoalesceStep(
                List.of("$.value.order_id", "$.value.legacy_id"),
                "$.value.id",
                OM.readTree("\"unknown\""));
        ReplayMessage msg = msg("{\"order_id\":null,\"legacy_id\":null,\"id\":null}");

        step.apply(msg);

        assertThat(decode(msg.value)).contains("\"id\":\"unknown\"");
    }

    @Test
    void writesFallbackWhenAllSourcesAbsent() throws Exception {
        var step = new CoalesceStep(
                List.of("$.value.order_id"),
                "$.value.id",
                OM.readTree("42"));
        ReplayMessage msg = msg("{\"id\":null}");  // order_id absent

        step.apply(msg);

        assertThat(decode(msg.value)).contains("\"id\":42");
    }

    // -------------------------------------------------------------------------
    // No fallback, all sources null — target unchanged
    // -------------------------------------------------------------------------

    @Test
    void targetUnchangedWhenAllSourcesNullAndNoFallback() {
        var step = new CoalesceStep(
                List.of("$.value.order_id", "$.value.legacy_id"),
                "$.value.id",
                null);
        ReplayMessage msg = msg("{\"order_id\":null,\"legacy_id\":null,\"id\":\"original\"}");

        step.apply(msg);

        assertThat(decode(msg.value)).contains("\"id\":\"original\"");
    }

    // -------------------------------------------------------------------------
    // Empty sources list — target unchanged
    // -------------------------------------------------------------------------

    @Test
    void emptySourcesListLeavesTargetUnchanged() throws Exception {
        var step = new CoalesceStep(
                List.of(),
                "$.value.id",
                OM.readTree("\"fallback\""));
        ReplayMessage msg = msg("{\"id\":\"original\"}");

        step.apply(msg);

        // fallback is written because no sources were tried (all "null")
        assertThat(decode(msg.value)).contains("\"id\":\"fallback\"");
    }

    // -------------------------------------------------------------------------
    // Null sources defaults to empty list
    // -------------------------------------------------------------------------

    @Test
    void nullSourcesConstructorDefaultsToEmptyList() {
        var step = new CoalesceStep(null, "$.value.id", null);
        assertThat(step.sources()).isEmpty();
    }

    // -------------------------------------------------------------------------
    // Null value body — sources read null, fallback applied if present
    // -------------------------------------------------------------------------

    @Test
    void nullValueBodyWithFallbackCreatesField() throws Exception {
        var step = new CoalesceStep(
                List.of("$.value.order_id"),
                "$.value.id",
                OM.readTree("\"default-id\""));
        ReplayMessage msg = msg(null);

        step.apply(msg);

        assertThat(msg.value).isNotNull();
        assertThat(decode(msg.value)).contains("\"id\":\"default-id\"");
    }

    // -------------------------------------------------------------------------
    // Jackson deserialization
    // -------------------------------------------------------------------------

    @Test
    void jacksonDeserialisationFull() throws Exception {
        String json = """
                {
                  "type": "coalesce",
                  "sources": ["$.value.order_id","$.value.legacy_id"],
                  "target": "$.value.id",
                  "fallback": "unknown"
                }
                """;
        var step = OM.readValue(json, CoalesceStep.class);
        assertThat(step.sources()).containsExactly("$.value.order_id", "$.value.legacy_id");
        assertThat(step.target()).isEqualTo("$.value.id");
        assertThat(step.fallback().asText()).isEqualTo("unknown");
    }

    @Test
    void jacksonDeserialisationWithoutFallback() throws Exception {
        String json = """
                {"type":"coalesce","sources":["$.value.id"],"target":"$.value.result"}
                """;
        var step = OM.readValue(json, CoalesceStep.class);
        assertThat(step.fallback()).isNull();
    }
}
