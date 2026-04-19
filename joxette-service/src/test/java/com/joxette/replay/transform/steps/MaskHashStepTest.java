package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.ReplayMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class MaskHashStepTest {

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
    // Group 1: Hash output variations — no-prefix, with-prefix, no-salt, with-salt
    // -------------------------------------------------------------------------

    /**
     * @param prefix           prefix arg (empty string means null)
     * @param salt             salt arg (empty string means null)
     * @param inputValue       the raw field value in JSON
     * @param expectedHashInput the string fed into SHA-256 (salt + inputValue)
     */
    @ParameterizedTest(name = "[{index}] prefix=''{0}'' salt=''{1}'' input=''{2}''")
    @CsvSource(delimiterString = "|", value = {
            // prefix | salt       | inputValue         | expectedHashInput
            "         |            | alice@example.com  | alice@example.com",
            "anon-    |            | alice@example.com  | alice@example.com",
            "         | secret-salt| alice@example.com  | secret-saltalice@example.com",
            "anon-    | fixed-salt | bob@example.com    | fixed-saltbob@example.com",
    })
    void hashOutputVariations(String prefix, String salt, String inputValue, String expectedHashInput) {
        String prefixArg = (prefix == null || prefix.isBlank()) ? null : prefix.trim();
        String saltArg   = (salt   == null || salt.isBlank())   ? null : salt.trim();

        var step = new MaskHashStep("$.value.email", prefixArg, saltArg);
        ReplayMessage message = msg("{\"email\":\"" + inputValue.trim() + "\"}");

        step.apply(message);

        String json = decode(message.value);
        String expectedHash = sha256Hex(expectedHashInput.trim());
        String expectedField = (prefixArg != null ? prefixArg : "") + expectedHash;
        assertThat(json).contains("\"email\":\"" + expectedField + "\"");
        assertThat(json).doesNotContain(inputValue.trim());
    }

    @Test
    void hashOutputIsFull64CharHex() {
        var step = new MaskHashStep("$.value.field", null, null);
        ReplayMessage message = msg("{\"field\":\"value\"}");

        step.apply(message);

        String json = decode(message.value);
        int start = json.indexOf("\"field\":\"") + 9;
        int end   = json.indexOf("\"", start);
        assertThat(json.substring(start, end)).hasSize(64).matches("[0-9a-f]+");
    }

    @Test
    void saltedHashIsReproducible() {
        var step = new MaskHashStep("$.value.email", "anon-", "fixed-salt");
        ReplayMessage msg1 = msg("{\"email\":\"bob@example.com\"}");
        ReplayMessage msg2 = msg("{\"email\":\"bob@example.com\"}");

        step.apply(msg1);
        step.apply(msg2);

        assertThat(decode(msg1.value)).isEqualTo(decode(msg2.value));
    }

    // -------------------------------------------------------------------------
    // Group 2: Edge case inputs — field is skipped when absent/null/no body
    // -------------------------------------------------------------------------

    static Stream<ReplayMessage> edgeCaseMessages() {
        return Stream.of(
                msg("{\"email\":null,\"id\":\"1\"}"),  // null field value
                msg("{\"id\":\"1\"}"),                  // absent field
                msg(null)                               // null value body
        );
    }

    @ParameterizedTest(name = "[{index}] edge case message")
    @MethodSource("edgeCaseMessages")
    void edgeCaseInputsAreSkippedWithoutThrow(ReplayMessage message) {
        var step = new MaskHashStep("$.value.email", "anon-", null);
        String originalValue = message.value;

        step.apply(message);

        assertThat(message.value).isEqualTo(originalValue);
    }

    // -------------------------------------------------------------------------
    // Jackson deserialisation
    // -------------------------------------------------------------------------

    @Test
    void jacksonDeserialisationWithAllFields() throws Exception {
        String json = """
                {"type":"mask_hash","target":"$.value.email","prefix":"anon-","salt":"my-salt"}
                """;
        var step = OM.readValue(json, MaskHashStep.class);
        assertThat(step.target()).isEqualTo("$.value.email");
        assertThat(step.prefix()).isEqualTo("anon-");
        assertThat(step.salt()).isEqualTo("my-salt");
    }

    @Test
    void jacksonDeserialisationWithTargetOnly() throws Exception {
        String json = "{\"type\":\"mask_hash\",\"target\":\"$.value.email\"}";
        var step = OM.readValue(json, MaskHashStep.class);
        assertThat(step.target()).isEqualTo("$.value.email");
        assertThat(step.prefix()).isNull();
        assertThat(step.salt()).isNull();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static String sha256Hex(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                sb.append(String.format("%02x", b & 0xff));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
