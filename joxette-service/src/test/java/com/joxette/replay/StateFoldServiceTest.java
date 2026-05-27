package com.joxette.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link StateFoldService}.
 *
 * <p>Tests cover the three fold strategies against hand-crafted entity record sequences
 * without any database dependency.
 */
class StateFoldServiceTest {

    private StateFoldService service;
    private ObjectMapper     objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        service = new StateFoldService(objectMapper);
    }

    // -------------------------------------------------------------------------
    // Empty sequence
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @CsvSource({"MERGE_PATCH", "LAST_VALUE", "LAST_PER_TOPIC"})
    void fold_emptySequence_returnsEmptyState(StateFoldStrategy strategy) {
        StateFoldService.StateResult result = service.fold(List.of(), strategy);

        assertThat(result.state().isEmpty()).isTrue();
        assertThat(result.asOf()).isNull();
        assertThat(result.eventCount()).isEqualTo(0);
    }

    // -------------------------------------------------------------------------
    // MERGE_PATCH
    // -------------------------------------------------------------------------

    @Test
    void mergePatch_singleEvent_returnsItsValue() {
        List<EntityRecord> records = List.of(
                record("orders.events", json("{\"status\":\"pending\",\"amount\":10}")));

        StateFoldService.StateResult result = service.fold(records, StateFoldStrategy.MERGE_PATCH);

        assertThat(result.state().get("status").asText()).isEqualTo("pending");
        assertThat(result.state().get("amount").asInt()).isEqualTo(10);
        assertThat(result.eventCount()).isEqualTo(1);
    }

    @Test
    void mergePatch_multipleEvents_latestFieldsWin() {
        List<EntityRecord> records = List.of(
                record("orders.events", json("{\"status\":\"pending\",\"amount\":10}")),
                record("orders.events", json("{\"status\":\"paid\"}")));

        StateFoldService.StateResult result = service.fold(records, StateFoldStrategy.MERGE_PATCH);

        assertThat(result.state().get("status").asText()).isEqualTo("paid");
        assertThat(result.state().get("amount").asInt()).isEqualTo(10); // retained
        assertThat(result.eventCount()).isEqualTo(2);
    }

    @Test
    void mergePatch_nullValueInPatch_removesField() {
        List<EntityRecord> records = List.of(
                record("orders.events", json("{\"status\":\"pending\",\"note\":\"hello\"}")),
                record("orders.events", json("{\"note\":null}")));

        StateFoldService.StateResult result = service.fold(records, StateFoldStrategy.MERGE_PATCH);

        assertThat(result.state().has("note")).isFalse();
        assertThat(result.state().get("status").asText()).isEqualTo("pending");
    }

    @Test
    void mergePatch_nestedObjects_deepMerged() {
        List<EntityRecord> records = List.of(
                record("orders.events", json("{\"address\":{\"city\":\"Berlin\",\"zip\":\"10115\"}}")),
                record("orders.events", json("{\"address\":{\"city\":\"Munich\"}}")));

        StateFoldService.StateResult result = service.fold(records, StateFoldStrategy.MERGE_PATCH);

        assertThat(result.state().at("/address/city").asText()).isEqualTo("Munich");
        assertThat(result.state().at("/address/zip").asText()).isEqualTo("10115"); // retained
    }

    @Test
    void mergePatch_nonObjectValue_ignored() {
        List<EntityRecord> records = List.of(
                record("orders.events", json("{\"status\":\"pending\"}")),
                record("orders.events", "not-valid-json-base64"));

        StateFoldService.StateResult result = service.fold(records, StateFoldStrategy.MERGE_PATCH);

        // Second record should be ignored, first still applied
        assertThat(result.state().get("status").asText()).isEqualTo("pending");
        assertThat(result.eventCount()).isEqualTo(2);
    }

    // -------------------------------------------------------------------------
    // LAST_VALUE
    // -------------------------------------------------------------------------

    @Test
    void lastValue_returnsOnlyFinalEvent() {
        List<EntityRecord> records = List.of(
                record("orders.events", json("{\"status\":\"pending\"}")),
                record("orders.events", json("{\"status\":\"paid\"}")),
                record("orders.events", json("{\"status\":\"shipped\"}")));

        StateFoldService.StateResult result = service.fold(records, StateFoldStrategy.LAST_VALUE);

        assertThat(result.state().get("status").asText()).isEqualTo("shipped");
        assertThat(result.eventCount()).isEqualTo(3);
    }

    @Test
    void lastValue_skipsNullValues_fallsBackToLastValid() {
        List<EntityRecord> records = List.of(
                record("orders.events", json("{\"status\":\"pending\"}")),
                record("orders.events", null)); // null value

        StateFoldService.StateResult result = service.fold(records, StateFoldStrategy.LAST_VALUE);

        assertThat(result.state().get("status").asText()).isEqualTo("pending");
    }

    // -------------------------------------------------------------------------
    // LAST_PER_TOPIC
    // -------------------------------------------------------------------------

    @Test
    void lastPerTopic_mergesLastValuePerTopic() {
        List<EntityRecord> records = List.of(
                record("orders.events",   json("{\"status\":\"pending\",\"orderId\":\"42\"}")),
                record("payments.events", json("{\"paymentId\":\"P1\",\"amount\":99}")),
                record("orders.events",   json("{\"status\":\"paid\"}")),
                record("payments.events", json("{\"paymentId\":\"P2\"}")));

        StateFoldService.StateResult result = service.fold(records, StateFoldStrategy.LAST_PER_TOPIC);

        // orders last value: {status: paid}  (no orderId — last_per_topic, not merge)
        // payments last value: {paymentId: P2}
        // merged: status + paymentId
        assertThat(result.state().get("status").asText()).isEqualTo("paid");
        assertThat(result.state().get("paymentId").asText()).isEqualTo("P2");
        assertThat(result.eventCount()).isEqualTo(4);
    }

    @Test
    void lastPerTopic_singleTopic_behavesLikeLastValue() {
        List<EntityRecord> records = List.of(
                record("orders.events", json("{\"status\":\"pending\"}")),
                record("orders.events", json("{\"status\":\"shipped\"}")));

        StateFoldService.StateResult result = service.fold(records, StateFoldStrategy.LAST_PER_TOPIC);

        assertThat(result.state().get("status").asText()).isEqualTo("shipped");
    }

    // -------------------------------------------------------------------------
    // asOf and eventCount
    // -------------------------------------------------------------------------

    @Test
    void fold_asOf_isTimestampOfLastRecord() {
        Instant t1 = Instant.parse("2024-01-01T10:00:00Z");
        Instant t2 = Instant.parse("2024-01-02T10:00:00Z");
        List<EntityRecord> records = List.of(
                recordAt("orders.events", json("{\"a\":1}"), t1),
                recordAt("orders.events", json("{\"b\":2}"), t2));

        StateFoldService.StateResult result = service.fold(records, StateFoldStrategy.MERGE_PATCH);

        assertThat(result.asOf()).isEqualTo(t2);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private EntityRecord record(String topic, String base64Value) {
        return recordAt(topic, base64Value, Instant.parse("2024-01-01T10:00:00Z"));
    }

    private EntityRecord recordAt(String topic, String base64Value, Instant timestamp) {
        return new EntityRecord("ORD-1", "testEvent", topic, 0, 0L,
                timestamp, timestamp, null, base64Value, null);
    }

    /** Encodes a JSON string as base64url so it matches EntityRecord.value() format. */
    private String json(String jsonString) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(jsonString.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
