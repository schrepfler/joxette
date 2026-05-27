package com.joxette.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PortraitService}.
 */
class PortraitServiceTest {

    private PortraitService service;
    private StateFoldService stateFoldService;

    @BeforeEach
    void setUp() {
        service = new PortraitService();
        stateFoldService = new StateFoldService(new ObjectMapper());
    }

    // -------------------------------------------------------------------------
    // Empty sequence
    // -------------------------------------------------------------------------

    @Test
    void portrait_emptySequence_zeroCountNullTimestamps() {
        PortraitService.PortraitResult result =
                service.portrait("order", "ORD-1", List.of(), null);

        assertThat(result.eventCount()).isEqualTo(0);
        assertThat(result.firstSeen()).isNull();
        assertThat(result.lastSeen()).isNull();
        assertThat(result.topicBreakdown()).isEmpty();
        assertThat(result.recentEvents()).isEmpty();
        assertThat(result.currentState()).isNull();
    }

    // -------------------------------------------------------------------------
    // Event count and timestamps
    // -------------------------------------------------------------------------

    @Test
    void portrait_multipleEvents_correctCountAndTimestamps() {
        Instant t1 = Instant.parse("2025-01-01T10:00:00Z");
        Instant t2 = Instant.parse("2025-01-15T14:00:00Z");
        Instant t3 = Instant.parse("2025-01-20T09:00:00Z");

        List<EntityRecord> records = List.of(
                record("OrderCreated",  t1, "orders.events"),
                record("OrderPaid",     t2, "payments.events"),
                record("OrderShipped",  t3, "orders.events"));

        PortraitService.PortraitResult result =
                service.portrait("order", "ORD-1", records, null);

        assertThat(result.eventCount()).isEqualTo(3);
        assertThat(result.firstSeen()).isEqualTo(t1);
        assertThat(result.lastSeen()).isEqualTo(t3);
    }

    // -------------------------------------------------------------------------
    // Topic breakdown
    // -------------------------------------------------------------------------

    @Test
    void portrait_topicBreakdown_countsPerTopic() {
        List<EntityRecord> records = List.of(
                record("e1", Instant.parse("2025-01-01T00:00:00Z"), "orders.events"),
                record("e2", Instant.parse("2025-01-02T00:00:00Z"), "payments.events"),
                record("e3", Instant.parse("2025-01-03T00:00:00Z"), "orders.events"),
                record("e4", Instant.parse("2025-01-04T00:00:00Z"), "orders.events"));

        PortraitService.PortraitResult result =
                service.portrait("order", "ORD-1", records, null);

        assertThat(result.topicBreakdown()).containsEntry("orders.events", 3);
        assertThat(result.topicBreakdown()).containsEntry("payments.events", 1);
    }

    // -------------------------------------------------------------------------
    // Recent events preview
    // -------------------------------------------------------------------------

    @Test
    void portrait_moreThanThreeEvents_showsLastThreeInReverseOrder() {
        Instant base = Instant.parse("2025-01-01T00:00:00Z");
        List<EntityRecord> records = List.of(
                record("e1", base,                "orders.events"),
                record("e2", base.plusSeconds(1), "orders.events"),
                record("e3", base.plusSeconds(2), "orders.events"),
                record("e4", base.plusSeconds(3), "orders.events"),
                record("e5", base.plusSeconds(4), "orders.events"));

        PortraitService.PortraitResult result =
                service.portrait("order", "ORD-1", records, null);

        assertThat(result.recentEvents()).hasSize(3);
        // Most recent first
        assertThat(result.recentEvents().get(0).messageType()).isEqualTo("e5");
        assertThat(result.recentEvents().get(1).messageType()).isEqualTo("e4");
        assertThat(result.recentEvents().get(2).messageType()).isEqualTo("e3");
    }

    @Test
    void portrait_fewerThanThreeEvents_showsAllInReverseOrder() {
        List<EntityRecord> records = List.of(
                record("e1", Instant.parse("2025-01-01T00:00:00Z"), "orders.events"),
                record("e2", Instant.parse("2025-01-02T00:00:00Z"), "orders.events"));

        PortraitService.PortraitResult result =
                service.portrait("order", "ORD-1", records, null);

        assertThat(result.recentEvents()).hasSize(2);
        assertThat(result.recentEvents().get(0).messageType()).isEqualTo("e2");
        assertThat(result.recentEvents().get(1).messageType()).isEqualTo("e1");
    }

    // -------------------------------------------------------------------------
    // currentState embedding
    // -------------------------------------------------------------------------

    @Test
    void portrait_withStateResult_embedsCurrentState() {
        List<EntityRecord> records = List.of(
                recordWithValue("OrderCreated", Instant.parse("2025-01-01T00:00:00Z"), "orders.events",
                        json("{\"status\":\"pending\"}")),
                recordWithValue("OrderPaid",    Instant.parse("2025-01-02T00:00:00Z"), "orders.events",
                        json("{\"status\":\"paid\"}")));

        StateFoldService.StateResult state = stateFoldService.fold(records, StateFoldStrategy.MERGE_PATCH);
        PortraitService.PortraitResult result =
                service.portrait("order", "ORD-1", records, state);

        assertThat(result.currentState()).isNotNull();
        assertThat(result.currentState().get("status").asText()).isEqualTo("paid");
    }

    @Test
    void portrait_withoutStateResult_currentStateNull() {
        List<EntityRecord> records = List.of(
                record("e1", Instant.parse("2025-01-01T00:00:00Z"), "orders.events"));

        PortraitService.PortraitResult result =
                service.portrait("order", "ORD-1", records, null);

        assertThat(result.currentState()).isNull();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private EntityRecord record(String messageType, Instant timestamp, String topic) {
        return new EntityRecord("ORD-1", messageType, topic, 0, 0L,
                timestamp, timestamp, null, null, null);
    }

    private EntityRecord recordWithValue(String messageType, Instant timestamp, String topic, String value) {
        return new EntityRecord("ORD-1", messageType, topic, 0, 0L,
                timestamp, timestamp, null, value, null);
    }

    private String json(String jsonString) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(jsonString.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
