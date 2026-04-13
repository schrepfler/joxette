package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.ReplayMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class RedactStepTest {

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
    // Field is nulled out
    // -------------------------------------------------------------------------

    @Test
    void nullsOutFieldInValueBody() {
        var step = new RedactStep("$.value.ssn");
        ReplayMessage msg = msg("{\"ssn\":\"123-45-6789\",\"name\":\"Alice\"}");

        step.apply(msg);

        String json = decode(msg.value);
        assertThat(json).contains("\"ssn\":null");
        assertThat(json).doesNotContain("123-45-6789");
        assertThat(json).contains("\"name\":\"Alice\"");
    }

    @Test
    void nullsOutNestedField() {
        var step = new RedactStep("$.value.user.email");
        ReplayMessage msg = msg("{\"user\":{\"email\":\"alice@example.com\",\"id\":1}}");

        step.apply(msg);

        String json = decode(msg.value);
        assertThat(json).contains("\"email\":null");
        assertThat(json).doesNotContain("alice@example.com");
        assertThat(json).contains("\"id\":1");
    }

    // -------------------------------------------------------------------------
    // Absent field — silently skipped
    // -------------------------------------------------------------------------

    @Test
    void absentFieldIsSkippedSilently() {
        var step = new RedactStep("$.value.nonexistent");
        ReplayMessage msg = msg("{\"id\":\"1\"}");
        String originalValue = msg.value;

        step.apply(msg);  // must not throw

        // value should be unchanged (or re-encoded but semantically same)
        assertThat(decode(msg.value)).contains("\"id\":\"1\"");
    }

    // -------------------------------------------------------------------------
    // Already null field
    // -------------------------------------------------------------------------

    @Test
    void alreadyNullFieldRemainsNull() {
        var step = new RedactStep("$.value.ssn");
        ReplayMessage msg = msg("{\"ssn\":null,\"name\":\"Bob\"}");

        step.apply(msg);

        assertThat(decode(msg.value)).contains("\"ssn\":null");
    }

    // -------------------------------------------------------------------------
    // Null value body — silently skipped
    // -------------------------------------------------------------------------

    @Test
    void nullValueBodyIsSkipped() {
        var step = new RedactStep("$.value.ssn");
        ReplayMessage msg = msg(null);

        step.apply(msg);  // must not throw

        assertThat(msg.value).isNull();
    }

    // -------------------------------------------------------------------------
    // Non-JSON value body — silently skipped
    // -------------------------------------------------------------------------

    @Test
    void nonJsonValueBodyIsSkipped() {
        var step = new RedactStep("$.value.ssn");
        ReplayMessage msg = msg(null);
        msg.value = b64("not-json");

        step.apply(msg);

        assertThat(decode(msg.value)).isEqualTo("not-json");
    }

    // -------------------------------------------------------------------------
    // Topic field redaction
    // -------------------------------------------------------------------------

    @Test
    void nullsOutTopicField() {
        var step = new RedactStep("$.topic");
        ReplayMessage msg = msg("{}");

        step.apply(msg);

        assertThat(msg.topic).isEqualTo("null");
    }

    // -------------------------------------------------------------------------
    // Jackson deserialization
    // -------------------------------------------------------------------------

    @Test
    void jacksonDeserialisation() throws Exception {
        String json = "{\"type\":\"redact\",\"target\":\"$.value.ssn\"}";
        var step = OM.readValue(json, RedactStep.class);
        assertThat(step.target()).isEqualTo("$.value.ssn");
    }
}
