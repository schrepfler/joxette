package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.ReplayMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class SetConstantStepTest {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final Base64.Encoder ENC = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DEC = Base64.getUrlDecoder();

    private static String b64(String json) {
        return ENC.encodeToString(json.getBytes(StandardCharsets.UTF_8));
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
    // Parameterized: constant value types (string, numeric, boolean, null, object)
    // -------------------------------------------------------------------------

    static Stream<Arguments> constantValueCases() throws Exception {
        return Stream.of(
                Arguments.of("string",  OM.readTree("\"staging\""),   "\"testField\":\"staging\""),
                Arguments.of("numeric", OM.readTree("42"),             "\"testField\":42"),
                Arguments.of("boolean", OM.readTree("false"),          "\"testField\":false"),
                Arguments.of("null",    OM.readTree("null"),           "\"testField\":null"),
                Arguments.of("object",  OM.readTree("{\"k\":\"v\"}"),  "\"k\":\"v\"")
        );
    }

    @ParameterizedTest(name = "sets {0} constant value")
    @MethodSource("constantValueCases")
    void setsConstantValueByType(String typeName, JsonNode constantValue, String expectedFragment) throws Exception {
        var step = new SetConstantStep("$.value.testField", constantValue);
        ReplayMessage msg = msg("{\"testField\":\"old\"}");

        step.apply(msg);

        assertThat(decode(msg.value)).contains(expectedFragment);
    }

    // -------------------------------------------------------------------------
    // Non-existent field (create)
    // -------------------------------------------------------------------------

    @Test
    void createsAbsentTopLevelFieldInValueBody() throws Exception {
        var step = new SetConstantStep("$.value.env", OM.readTree("\"staging\""));
        ReplayMessage msg = msg("{\"id\":\"1\"}");  // no 'env' field

        step.apply(msg);

        assertThat(decode(msg.value)).contains("\"env\":\"staging\"");
        assertThat(decode(msg.value)).contains("\"id\":\"1\"");
    }

    // -------------------------------------------------------------------------
    // null value body
    // -------------------------------------------------------------------------

    @Test
    void nullValueBodyCreatesJsonObject() throws Exception {
        var step = new SetConstantStep("$.value.env", OM.readTree("\"staging\""));
        ReplayMessage msg = msg(null);  // no value

        step.apply(msg);

        assertThat(msg.value).isNotNull();
        assertThat(decode(msg.value)).contains("\"env\":\"staging\"");
    }

    // -------------------------------------------------------------------------
    // Top-level message fields
    // -------------------------------------------------------------------------

    @Test
    void setsTopic() throws Exception {
        var step = new SetConstantStep("$.topic", OM.readTree("\"orders-staging\""));
        ReplayMessage msg = msg("{}");

        step.apply(msg);

        assertThat(msg.topic).isEqualTo("orders-staging");
    }

    // -------------------------------------------------------------------------
    // Non-JSON body is silently skipped (value unchanged)
    // -------------------------------------------------------------------------

    @Test
    void nonJsonBodyIsSkipped() throws Exception {
        var step = new SetConstantStep("$.value.env", OM.readTree("\"staging\""));
        ReplayMessage msg = msg(null);
        msg.value = b64("not-json");  // override with non-JSON

        step.apply(msg);

        assertThat(decode(msg.value)).isEqualTo("not-json");
    }

    // -------------------------------------------------------------------------
    // Jackson deserialization round-trip
    // -------------------------------------------------------------------------

    @Test
    void jacksonDeserialisation() throws Exception {
        String json = """
                {"type":"set_constant","target":"$.value.env","value":"staging"}
                """;
        var step = OM.readValue(json, SetConstantStep.class);

        assertThat(step.target()).isEqualTo("$.value.env");
        assertThat(step.value().asText()).isEqualTo("staging");
    }
}
