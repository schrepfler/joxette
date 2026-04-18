package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.ReplayMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class TemplateStepTest {

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
        return new ReplayMessage(new CassetteRecord("orders", 3, 999L,
                Instant.EPOCH, Instant.EPOCH, null, b64val, null, null));
    }

    // -------------------------------------------------------------------------
    // Basic substitution
    // -------------------------------------------------------------------------

    @Test
    void substitutesValueFieldPlaceholder() {
        var step = new TemplateStep("$.value.routing_key",
                "replay-${value.order_id}");
        ReplayMessage msg = msg("{\"order_id\":\"ORD-42\",\"routing_key\":\"old\"}");

        step.apply(msg);

        assertThat(decode(msg.value)).contains("\"routing_key\":\"replay-ORD-42\"");
    }

    @Test
    void substitutesMetadataPlaceholder() {
        var step = new TemplateStep("$.value.routing_key",
                "part-${partition}-off-${offset}");
        ReplayMessage msg = msg("{\"routing_key\":\"\"}");
        msg.partition = 3;
        msg.offset    = 999L;

        step.apply(msg);

        assertThat(decode(msg.value)).contains("\"routing_key\":\"part-3-off-999\"");
    }

    @Test
    void substitutesMixedPlaceholders() {
        var step = new TemplateStep("$.value.label",
                "${topic}::${value.order_id}::${partition}");
        ReplayMessage msg = msg("{\"order_id\":\"ORD-1\",\"label\":\"\"}");
        msg.topic     = "orders";
        msg.partition = 0;

        step.apply(msg);

        assertThat(decode(msg.value)).contains("\"label\":\"orders::ORD-1::0\"");
    }

    // -------------------------------------------------------------------------
    // No placeholders — literal string written
    // -------------------------------------------------------------------------

    @Test
    void noPlaceholdersWritesLiteralString() {
        var step = new TemplateStep("$.value.env", "staging");
        ReplayMessage msg = msg("{\"env\":\"prod\"}");

        step.apply(msg);

        assertThat(decode(msg.value)).contains("\"env\":\"staging\"");
    }

    // -------------------------------------------------------------------------
    // Absent placeholder — replaced with empty string
    // -------------------------------------------------------------------------

    @Test
    void absentPlaceholderReplacedWithEmptyString() {
        var step = new TemplateStep("$.value.label",
                "prefix-${value.nonexistent}-suffix");
        ReplayMessage msg = msg("{\"label\":\"\"}");

        step.apply(msg);

        assertThat(decode(msg.value)).contains("\"label\":\"prefix--suffix\"");
    }

    // -------------------------------------------------------------------------
    // Null value body
    // -------------------------------------------------------------------------

    @Test
    void nullValueBodyCreatesTargetField() {
        var step = new TemplateStep("$.value.routing_key",
                "literal-${partition}");
        ReplayMessage msg = msg(null);
        msg.partition = 5;

        step.apply(msg);

        assertThat(msg.value).isNotNull();
        assertThat(decode(msg.value)).contains("\"routing_key\":\"literal-5\"");
    }

    // -------------------------------------------------------------------------
    // Jackson deserialization
    // -------------------------------------------------------------------------

    @Test
    void jacksonDeserialisation() throws Exception {
        String json = """
                {"type":"template","target":"$.value.label","template":"${value.id}-replay"}
                """;
        var step = OM.readValue(json, TemplateStep.class);
        assertThat(step.target()).isEqualTo("$.value.label");
        assertThat(step.template()).isEqualTo("${value.id}-replay");
    }
}
