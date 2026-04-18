package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.ReplayMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class CopyFieldStepTest {

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
        return new ReplayMessage(new CassetteRecord("orders", 2, 100L,
                Instant.EPOCH, Instant.EPOCH, null, b64val, null, null));
    }

    // -------------------------------------------------------------------------
    // Value-to-value copy
    // -------------------------------------------------------------------------

    @Test
    void copiesFieldWithinValueBody() {
        var step = new CopyFieldStep("$.value.order_id", "$.value.legacy_id");
        ReplayMessage msg = msg("{\"order_id\":\"ORD-42\",\"legacy_id\":\"old\"}");

        step.apply(msg);

        String json = decode(msg.value);
        assertThat(json).contains("\"legacy_id\":\"ORD-42\"");
        assertThat(json).contains("\"order_id\":\"ORD-42\"");  // source unchanged
    }

    // -------------------------------------------------------------------------
    // Value-to-key copy
    // -------------------------------------------------------------------------

    @Test
    void copiesValueFieldIntoMessageKey() {
        var step = new CopyFieldStep("$.value.order_id", "$.key");
        ReplayMessage msg = msg("{\"order_id\":\"ORD-99\"}");

        step.apply(msg);

        // key should now be base64url("ORD-99")
        assertThat(decode(msg.key)).isEqualTo("ORD-99");
    }

    // -------------------------------------------------------------------------
    // Missing source — target unchanged
    // -------------------------------------------------------------------------

    @Test
    void absentSourceLeavesTargetUnchanged() {
        var step = new CopyFieldStep("$.value.nonexistent", "$.value.target");
        ReplayMessage msg = msg("{\"target\":\"original\"}");

        step.apply(msg);

        assertThat(decode(msg.value)).contains("\"target\":\"original\"");
    }

    // -------------------------------------------------------------------------
    // Null source value — target unchanged
    // -------------------------------------------------------------------------

    @Test
    void nullSourceValueLeavesTargetUnchanged() {
        var step = new CopyFieldStep("$.value.src", "$.value.dst");
        ReplayMessage msg = msg("{\"src\":null,\"dst\":\"keep\"}");

        step.apply(msg);

        // null read → skip
        assertThat(decode(msg.value)).contains("\"dst\":\"keep\"");
    }

    // -------------------------------------------------------------------------
    // Null value body — silently skipped
    // -------------------------------------------------------------------------

    @Test
    void nullValueBodyIsSkipped() {
        var step = new CopyFieldStep("$.value.order_id", "$.value.id");
        ReplayMessage msg = msg(null);

        step.apply(msg);  // must not throw

        assertThat(msg.value).isNull();
    }

    // -------------------------------------------------------------------------
    // Metadata-to-value copy
    // -------------------------------------------------------------------------

    @Test
    void copiesPartitionIntoValueBody() {
        var step = new CopyFieldStep("$.partition", "$.value.source_partition");
        ReplayMessage msg = msg("{\"a\":1}");
        msg.partition = 7;

        step.apply(msg);

        assertThat(decode(msg.value)).contains("\"source_partition\":7");
    }

    // -------------------------------------------------------------------------
    // Jackson deserialization
    // -------------------------------------------------------------------------

    @Test
    void jacksonDeserialisation() throws Exception {
        String json = """
                {"type":"copy_field","from":"$.value.order_id","to":"$.key"}
                """;
        var step = OM.readValue(json, CopyFieldStep.class);
        assertThat(step.from()).isEqualTo("$.value.order_id");
        assertThat(step.to()).isEqualTo("$.key");
    }
}
