package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.TransformContext;
import com.joxette.replay.transform.ReplayMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the structural / JSON and key transformation steps:
 * {@link RenameFieldStep}, {@link DeleteFieldStep}, {@link FlattenFieldStep},
 * {@link AddComputedFieldStep}, {@link MergePatchStep},
 * {@link RemapKeyStep}, {@link NullKeyStep}, {@link KeyFromValueStep}.
 */
class StructuralKeyStepTest {

    private static final Base64.Encoder ENC = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DEC = Base64.getUrlDecoder();

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static String b64(String s) {
        return ENC.encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String b64) {
        return new String(DEC.decode(b64), StandardCharsets.UTF_8);
    }

    private static ReplayMessage msg(String jsonValue) {
        Instant now = Instant.now();
        CassetteRecord r = new CassetteRecord(
                "test-topic", 0, 1L, now, now,
                b64("original-key"),
                jsonValue != null ? b64(jsonValue) : null,
                null, null);
        return new ReplayMessage(r);
    }

    private static ReplayMessage msgNoKey(String jsonValue) {
        Instant now = Instant.now();
        CassetteRecord r = new CassetteRecord(
                "test-topic", 0, 1L, now, now,
                null,
                jsonValue != null ? b64(jsonValue) : null,
                null, null);
        return new ReplayMessage(r);
    }

    private static TransformContext ctx(long seq) {
        return new TransformContext("test-replay-id", seq);
    }

    private static JsonNode readValue(ReplayMessage m) throws Exception {
        return JsonStepHelper.MAPPER.readTree(DEC.decode(m.value));
    }

    // =========================================================================
    // RenameFieldStep
    // =========================================================================

    @Test
    void renameField_topLevelKey() throws Exception {
        var step = new RenameFieldStep("$.value.orderId", "order_id");
        var m = msg("{\"orderId\":\"42\",\"status\":\"ok\"}");

        step.apply(m, ctx(0));

        JsonNode v = readValue(m);
        assertThat(v.has("orderId")).isFalse();
        assertThat(v.get("order_id").asText()).isEqualTo("42");
        assertThat(v.get("status").asText()).isEqualTo("ok");
    }

    @Test
    void renameField_preservesValueType() throws Exception {
        var step = new RenameFieldStep("$.value.count", "total");
        var m = msg("{\"count\":7}");

        step.apply(m, ctx(0));

        JsonNode v = readValue(m);
        assertThat(v.has("count")).isFalse();
        assertThat(v.get("total").isNumber()).isTrue();
        assertThat(v.get("total").asInt()).isEqualTo(7);
    }

    @Test
    void renameField_absentSourceIsNoOp() {
        var step = new RenameFieldStep("$.value.missing", "new_name");
        var m = msg("{\"a\":1}");
        String originalValue = m.value;

        step.apply(m, ctx(0));

        assertThat(m.value).isEqualTo(originalValue);
    }

    @Test
    void renameField_nonJsonValueIsNoOp() {
        var step = new RenameFieldStep("$.value.x", "y");
        var m = msg(null);
        m.value = b64("not-json");
        String originalValue = m.value;

        step.apply(m, ctx(0));

        assertThat(m.value).isEqualTo(originalValue);
    }

    @Test
    void renameField_nullValueIsNoOp() {
        var step = new RenameFieldStep("$.value.x", "y");
        var m = msg(null);

        step.apply(m, ctx(0));

        assertThat(m.value).isNull();
    }

    // =========================================================================
    // DeleteFieldStep
    // =========================================================================

    @Test
    void deleteField_removesExistingKey() throws Exception {
        var step = new DeleteFieldStep("$.value.secret");
        var m = msg("{\"id\":\"1\",\"secret\":\"s3cr3t\"}");

        step.apply(m, ctx(0));

        JsonNode v = readValue(m);
        assertThat(v.has("secret")).isFalse();
        assertThat(v.get("id").asText()).isEqualTo("1");
    }

    @Test
    void deleteField_absentKeyIsNoOp() {
        var step = new DeleteFieldStep("$.value.nonexistent");
        var m = msg("{\"a\":1}");
        String originalValue = m.value;

        step.apply(m, ctx(0));

        assertThat(m.value).isEqualTo(originalValue);
    }

    @Test
    void deleteField_nonJsonValueIsNoOp() {
        var step = new DeleteFieldStep("$.value.x");
        var m = msg(null);
        m.value = b64("raw-bytes");
        String originalValue = m.value;

        step.apply(m, ctx(0));

        assertThat(m.value).isEqualTo(originalValue);
    }

    @Test
    void deleteField_nullValueIsNoOp() {
        var step = new DeleteFieldStep("$.value.x");
        var m = msg(null);

        step.apply(m, ctx(0));

        assertThat(m.value).isNull();
    }

    // =========================================================================
    // FlattenFieldStep
    // =========================================================================

    @Test
    void flattenField_hoistWithPrefix() throws Exception {
        var step = new FlattenFieldStep("$.value.metadata", "meta_");
        var m = msg("{\"id\":\"1\",\"metadata\":{\"env\":\"prod\",\"version\":\"2\"}}");

        step.apply(m, ctx(0));

        JsonNode v = readValue(m);
        assertThat(v.has("metadata")).isFalse();
        assertThat(v.get("id").asText()).isEqualTo("1");
        assertThat(v.get("meta_env").asText()).isEqualTo("prod");
        assertThat(v.get("meta_version").asText()).isEqualTo("2");
    }

    @Test
    void flattenField_hoistWithoutPrefix() throws Exception {
        var step = new FlattenFieldStep("$.value.meta", null);
        var m = msg("{\"x\":1,\"meta\":{\"a\":\"aVal\",\"b\":\"bVal\"}}");

        step.apply(m, ctx(0));

        JsonNode v = readValue(m);
        assertThat(v.has("meta")).isFalse();
        assertThat(v.get("a").asText()).isEqualTo("aVal");
        assertThat(v.get("b").asText()).isEqualTo("bVal");
        assertThat(v.get("x").asInt()).isEqualTo(1);
    }

    @Test
    void flattenField_collisionLastWriteWins() throws Exception {
        var step = new FlattenFieldStep("$.value.extra", "");
        // "status" exists at root AND in "extra" — inner wins (last written)
        var m = msg("{\"status\":\"old\",\"extra\":{\"status\":\"new\"}}");

        step.apply(m, ctx(0));

        JsonNode v = readValue(m);
        assertThat(v.has("extra")).isFalse();
        assertThat(v.get("status").asText()).isEqualTo("new");
    }

    @Test
    void flattenField_sourceNotObjectIsNoOp() {
        var step = new FlattenFieldStep("$.value.tags", "t_");
        var m = msg("{\"tags\":[\"a\",\"b\"]}");
        String originalValue = m.value;

        step.apply(m, ctx(0));

        assertThat(m.value).isEqualTo(originalValue);
    }

    @Test
    void flattenField_absentSourceIsNoOp() {
        var step = new FlattenFieldStep("$.value.missing", "");
        var m = msg("{\"a\":1}");
        String originalValue = m.value;

        step.apply(m, ctx(0));

        assertThat(m.value).isEqualTo(originalValue);
    }

    // =========================================================================
    // AddComputedFieldStep
    // =========================================================================

    @Test
    void addComputed_replaySequence() throws Exception {
        var step = new AddComputedFieldStep("$.value.seq", "REPLAY_SEQUENCE");
        var m = msg("{\"id\":\"1\"}");

        step.apply(m, ctx(42));

        JsonNode v = readValue(m);
        assertThat(v.get("seq").isNumber()).isTrue();
        assertThat(v.get("seq").asLong()).isEqualTo(42L);
    }

    @Test
    void addComputed_nowEpochMs() throws Exception {
        var step = new AddComputedFieldStep("$.value.ts", "NOW_EPOCH_MS");
        var m = msg("{\"id\":\"1\"}");
        long before = System.currentTimeMillis();

        step.apply(m, ctx(0));

        long after = System.currentTimeMillis();
        JsonNode v = readValue(m);
        long ts = v.get("ts").asLong();
        assertThat(ts).isBetween(before, after);
    }

    @Test
    void addComputed_nowIso() throws Exception {
        var step = new AddComputedFieldStep("$.value.ts", "NOW_ISO");
        var m = msg("{\"id\":\"1\"}");

        step.apply(m, ctx(0));

        JsonNode v = readValue(m);
        String ts = v.get("ts").asText();
        // Should parse as an Instant without throwing
        assertThat(Instant.parse(ts)).isNotNull();
    }

    @Test
    void addComputed_jsonPathExpression() throws Exception {
        var step = new AddComputedFieldStep("$.value.order_ref", "$.value.order_id");
        var m = msg("{\"order_id\":\"ORD-99\"}");

        step.apply(m, ctx(0));

        JsonNode v = readValue(m);
        assertThat(v.get("order_ref").asText()).isEqualTo("ORD-99");
        assertThat(v.get("order_id").asText()).isEqualTo("ORD-99");  // original still present
    }

    @Test
    void addComputed_unresolvedExpressionWritesNull() throws Exception {
        var step = new AddComputedFieldStep("$.value.derived", "$.value.nonexistent");
        var m = msg("{\"x\":1}");

        step.apply(m, ctx(0));

        JsonNode v = readValue(m);
        assertThat(v.has("derived")).isTrue();
        assertThat(v.get("derived").isNull()).isTrue();
    }

    // =========================================================================
    // MergePatchStep
    // =========================================================================

    @Test
    void mergePatch_addsNewKey() throws Exception {
        ObjectNode patch = JsonStepHelper.MAPPER.createObjectNode();
        patch.put("country", "US");
        var step = new MergePatchStep("$.value", patch);
        var m = msg("{\"city\":\"NY\"}");

        step.apply(m, ctx(0));

        JsonNode v = readValue(m);
        assertThat(v.get("city").asText()).isEqualTo("NY");
        assertThat(v.get("country").asText()).isEqualTo("US");
    }

    @Test
    void mergePatch_overwritesExistingKey() throws Exception {
        ObjectNode patch = JsonStepHelper.MAPPER.createObjectNode();
        patch.put("status", "REPLAYED");
        var step = new MergePatchStep("$.value", patch);
        var m = msg("{\"status\":\"ORIGINAL\",\"id\":\"1\"}");

        step.apply(m, ctx(0));

        JsonNode v = readValue(m);
        assertThat(v.get("status").asText()).isEqualTo("REPLAYED");
        assertThat(v.get("id").asText()).isEqualTo("1");
    }

    @Test
    void mergePatch_nullValueRemovesKey() throws Exception {
        ObjectNode patch = JsonStepHelper.MAPPER.createObjectNode();
        patch.putNull("secret");
        patch.put("env", "test");
        var step = new MergePatchStep("$.value", patch);
        var m = msg("{\"secret\":\"s3cr3t\",\"env\":\"prod\"}");

        step.apply(m, ctx(0));

        JsonNode v = readValue(m);
        assertThat(v.has("secret")).isFalse();
        assertThat(v.get("env").asText()).isEqualTo("test");
    }

    @Test
    void mergePatch_recursiveObjectMerge() throws Exception {
        ObjectNode innerPatch = JsonStepHelper.MAPPER.createObjectNode();
        innerPatch.put("zip", "10001");
        ObjectNode patch = JsonStepHelper.MAPPER.createObjectNode();
        patch.set("address", innerPatch);
        var step = new MergePatchStep("$.value", patch);
        var m = msg("{\"address\":{\"city\":\"NY\",\"zip\":\"old\"},\"name\":\"Alice\"}");

        step.apply(m, ctx(0));

        JsonNode v = readValue(m);
        assertThat(v.get("address").get("city").asText()).isEqualTo("NY");
        assertThat(v.get("address").get("zip").asText()).isEqualTo("10001");
        assertThat(v.get("name").asText()).isEqualTo("Alice");
    }

    @Test
    void mergePatch_nestedTargetPath() throws Exception {
        ObjectNode patch = JsonStepHelper.MAPPER.createObjectNode();
        patch.put("b", "patched");
        var step = new MergePatchStep("$.value.nested", patch);
        var m = msg("{\"nested\":{\"a\":\"orig\",\"b\":\"orig\"}}");

        step.apply(m, ctx(0));

        JsonNode v = readValue(m);
        assertThat(v.get("nested").get("a").asText()).isEqualTo("orig");
        assertThat(v.get("nested").get("b").asText()).isEqualTo("patched");
    }

    @Test
    void mergePatch_absentTargetIsNoOp() {
        ObjectNode patch = JsonStepHelper.MAPPER.createObjectNode();
        patch.put("x", "y");
        var step = new MergePatchStep("$.value.missing", patch);
        var m = msg("{\"a\":1}");
        String originalValue = m.value;

        step.apply(m, ctx(0));

        assertThat(m.value).isEqualTo(originalValue);
    }

    // =========================================================================
    // RemapKeyStep
    // =========================================================================

    static Stream<Arguments> remapKeyCases() {
        return Stream.of(
            // template,                             prefix,    messageValue,                         expectedKey
            Arguments.of("new-key",                 null,      "{\"id\":\"1\"}",                     "new-key"),
            Arguments.of("order",                   "replay-", "{\"id\":\"1\"}",                     "replay-order"),
            Arguments.of("${$.value.order_id}",     null,      "{\"order_id\":\"ORD-42\"}",          "ORD-42"),
            Arguments.of("${$.value.id}",           "ord-",    "{\"id\":\"99\"}",                    "ord-99"),
            Arguments.of("${$.value.missing}",      "k-",      "{\"id\":\"1\"}",                     "k-"),
            Arguments.of("${$.value.type}-${$.value.id}", null, "{\"type\":\"order\",\"id\":\"7\"}", "order-7")
        );
    }

    @ParameterizedTest
    @MethodSource("remapKeyCases")
    void remapKey(String template, String prefix, String messageValue, String expectedKey) {
        var step = new RemapKeyStep(template, prefix);
        var m = msg(messageValue);

        step.apply(m, ctx(0));

        assertThat(decode(m.key)).isEqualTo(expectedKey);
    }

    // =========================================================================
    // NullKeyStep
    // =========================================================================

    @Test
    void nullKey_setsKeyToNull() {
        var step = new NullKeyStep();
        var m = msg("{\"id\":\"1\"}");
        assertThat(m.key).isNotNull();

        step.apply(m, ctx(0));

        assertThat(m.key).isNull();
    }

    @Test
    void nullKey_alreadyNullKeyIsNoOp() {
        var step = new NullKeyStep();
        var m = msgNoKey("{\"id\":\"1\"}");

        step.apply(m, ctx(0));

        assertThat(m.key).isNull();
    }

    // =========================================================================
    // KeyFromValueStep
    // =========================================================================

    // jsonPath, valueJson, expectedKey  (null sentinel = expect null key)
    @ParameterizedTest
    @CsvSource({
        "$.value.order_id, {\"order_id\":\"ORD-99\"COMMA\"status\":\"ok\"}, ORD-99",
        "$.value.id,       {\"id\":42},                                      42",
        "$.value.id,       {\"id\":null},                                    NULL",
        "$.value.nonexistent, {\"x\":1},                                     NULL"
    })
    void keyFromValue_extraction(String jsonPath, String valueJson, String expectedKey) {
        String json = valueJson.replace("COMMA", ",");
        var step = new KeyFromValueStep(jsonPath);
        var m = msg(json);

        step.apply(m, ctx(0));

        if ("NULL".equals(expectedKey)) {
            assertThat(m.key).isNull();
        } else {
            assertThat(decode(m.key)).isEqualTo(expectedKey);
        }
    }

    @Test
    void keyFromValue_nonJsonValueIsNoOp() {
        var step = new KeyFromValueStep("$.value.id");
        var m = msg(null);
        m.value = b64("not-json");
        String originalKey = m.key;

        step.apply(m, ctx(0));

        assertThat(m.key).isEqualTo(originalKey);
    }

    @Test
    void keyFromValue_nestedPath() {
        var step = new KeyFromValueStep("$.value.payment.order_id");
        var m = msg("{\"payment\":{\"order_id\":\"PAY-5\"}}");

        step.apply(m, ctx(0));

        assertThat(decode(m.key)).isEqualTo("PAY-5");
    }
}
