package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.ReplayMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;

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
    // Group 1: Source prioritization
    // -------------------------------------------------------------------------

    static Stream<Arguments> sourcePrioritizationCases() {
        return Stream.of(
                Arguments.of("{\"order_id\":\"ORD-1\",\"legacy_id\":\"L-99\",\"id\":null}", "ORD-1"),
                Arguments.of("{\"order_id\":null,\"legacy_id\":\"L-99\",\"id\":null}", "L-99"),
                Arguments.of("{\"legacy_id\":\"L-77\",\"id\":null}", "L-77")
        );
    }

    @ParameterizedTest
    @MethodSource("sourcePrioritizationCases")
    void sourcePrioritization(String valueJson, String expectedSelected) {
        var step = new CoalesceStep(
                List.of("$.value.order_id", "$.value.legacy_id"),
                "$.value.id",
                null);
        ReplayMessage msg = msg(valueJson);

        step.apply(msg);

        assertThat(decode(msg.value)).contains("\"id\":\"" + expectedSelected + "\"");
    }

    // -------------------------------------------------------------------------
    // Group 2: Fallback behavior
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @CsvSource({
            "true,  true,  fallback-val",
            "true,  false, fallback-val",
            "false, true,  original",
    })
    void fallbackBehavior(boolean hasFallback, boolean allSourcesNull, String expectedResult) throws Exception {
        JsonNode fallback = hasFallback ? OM.readTree("\"fallback-val\"") : null;
        var step = new CoalesceStep(List.of("$.value.order_id"), "$.value.id", fallback);
        // allSourcesNull=true → field present but null; false → field absent entirely
        String valueJson = allSourcesNull
                ? "{\"order_id\":null,\"id\":\"original\"}"
                : "{\"id\":\"original\"}";
        ReplayMessage msg = msg(valueJson);

        step.apply(msg);

        assertThat(decode(msg.value)).contains("\"id\":\"" + expectedResult + "\"");
    }

    // -------------------------------------------------------------------------
    // Empty sources list — fallback applied
    // -------------------------------------------------------------------------

    @Test
    void emptySourcesListLeavesTargetUnchanged() throws Exception {
        var step = new CoalesceStep(
                List.of(),
                "$.value.id",
                OM.readTree("\"fallback\""));
        ReplayMessage msg = msg("{\"id\":\"original\"}");

        step.apply(msg);

        assertThat(decode(msg.value)).contains("\"id\":\"fallback\"");
    }

    // -------------------------------------------------------------------------
    // Constructor: null sources defaults to empty list
    // -------------------------------------------------------------------------

    @Test
    void nullSourcesConstructorDefaultsToEmptyList() {
        var step = new CoalesceStep(null, "$.value.id", null);
        assertThat(step.sources()).isEmpty();
    }

    // -------------------------------------------------------------------------
    // Null value body — fallback creates field
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
