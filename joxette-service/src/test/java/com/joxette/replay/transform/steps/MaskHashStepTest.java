package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.ReplayMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;

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
    // Basic hash — no prefix, no salt
    // -------------------------------------------------------------------------

    @Test
    void hashesFieldValueToSha256Hex() {
        var step = new MaskHashStep("$.value.email", null, null);
        ReplayMessage msg = msg("{\"email\":\"alice@example.com\"}");

        step.apply(msg);

        String json = decode(msg.value);
        // exact expected hash of "alice@example.com"
        String expected = sha256Hex("alice@example.com");
        assertThat(json).contains("\"email\":\"" + expected + "\"");
        assertThat(json).doesNotContain("alice@example.com");
    }

    // -------------------------------------------------------------------------
    // Prefix prepended to hash output
    // -------------------------------------------------------------------------

    @Test
    void prependsPrefixToHash() {
        var step = new MaskHashStep("$.value.email", "anon-", null);
        ReplayMessage msg = msg("{\"email\":\"alice@example.com\"}");

        step.apply(msg);

        String json = decode(msg.value);
        String expected = "anon-" + sha256Hex("alice@example.com");
        assertThat(json).contains("\"email\":\"" + expected + "\"");
    }

    // -------------------------------------------------------------------------
    // Salt mixed into hash input
    // -------------------------------------------------------------------------

    @Test
    void saltChangesHashOutput() {
        String email = "alice@example.com";
        var stepNoSalt   = new MaskHashStep("$.value.email", null, null);
        var stepWithSalt = new MaskHashStep("$.value.email", null, "secret-salt");

        ReplayMessage msg1 = msg("{\"email\":\"" + email + "\"}");
        ReplayMessage msg2 = msg("{\"email\":\"" + email + "\"}");

        stepNoSalt.apply(msg1);
        stepWithSalt.apply(msg2);

        String hash1 = extractEmailValue(decode(msg1.value));
        String hash2 = extractEmailValue(decode(msg2.value));

        assertThat(hash1).isNotEqualTo(hash2);
        assertThat(hash2).isEqualTo(sha256Hex("secret-salt" + email));
    }

    @Test
    void saltedHashIsReproducible() {
        String email = "bob@example.com";
        var step = new MaskHashStep("$.value.email", "anon-", "fixed-salt");

        ReplayMessage msg1 = msg("{\"email\":\"" + email + "\"}");
        ReplayMessage msg2 = msg("{\"email\":\"" + email + "\"}");

        step.apply(msg1);
        step.apply(msg2);

        assertThat(decode(msg1.value)).isEqualTo(decode(msg2.value));
    }

    // -------------------------------------------------------------------------
    // Null / absent field — skipped
    // -------------------------------------------------------------------------

    @Test
    void nullFieldValueIsSkipped() {
        var step = new MaskHashStep("$.value.email", "anon-", null);
        ReplayMessage msg = msg("{\"email\":null,\"id\":\"1\"}");

        step.apply(msg);

        // email should remain null (not hashed)
        assertThat(decode(msg.value)).contains("\"email\":null");
    }

    @Test
    void absentFieldIsSkipped() {
        var step = new MaskHashStep("$.value.email", "anon-", null);
        ReplayMessage msg = msg("{\"id\":\"1\"}");

        step.apply(msg);  // must not throw, email path absent

        assertThat(decode(msg.value)).doesNotContain("\"email\"");
    }

    @Test
    void nullValueBodyIsSkipped() {
        var step = new MaskHashStep("$.value.email", "anon-", null);
        ReplayMessage msg = msg(null);

        step.apply(msg);  // must not throw

        assertThat(msg.value).isNull();
    }

    // -------------------------------------------------------------------------
    // Hash output is full 64-character hex string
    // -------------------------------------------------------------------------

    @Test
    void hashOutputIsFull64CharHex() {
        var step = new MaskHashStep("$.value.field", null, null);
        ReplayMessage msg = msg("{\"field\":\"value\"}");

        step.apply(msg);

        String json = decode(msg.value);
        // extract value between "field":" and closing "
        int start = json.indexOf("\"field\":\"") + 9;
        int end   = json.indexOf("\"", start);
        String hash = json.substring(start, end);
        assertThat(hash).hasSize(64).matches("[0-9a-f]+");
    }

    // -------------------------------------------------------------------------
    // Jackson deserialization
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

    private String extractEmailValue(String json) {
        int start = json.indexOf("\"email\":\"") + 9;
        int end   = json.indexOf("\"", start);
        return json.substring(start, end);
    }

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
