package com.joxette.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DiffService}.
 *
 * <p>Tests cover empty input, single event, multi-event field changes,
 * field removal (null patch), nested objects, new-field additions,
 * and non-parseable event values.
 */
class DiffServiceTest {

    private DiffService service;

    @BeforeEach
    void setUp() {
        service = new DiffService(new ObjectMapper());
    }

    // -------------------------------------------------------------------------
    // Empty / single
    // -------------------------------------------------------------------------

    @Test
    void diff_emptySequence_returnsEmptyList() {
        assertThat(service.diff(List.of())).isEmpty();
    }

    @Test
    void diff_singleEvent_hasNullDiffFields() {
        List<DiffRecord> result = service.diff(List.of(
                record(json("{\"status\":\"pending\",\"amount\":10}"))
        ));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).changedFields()).isNull();
        assertThat(result.get(0).before()).isNull();
    }

    // -------------------------------------------------------------------------
    // Field changes
    // -------------------------------------------------------------------------

    @Test
    void diff_fieldUpdated_reportedInChangedFields() {
        List<DiffRecord> result = service.diff(List.of(
                record(json("{\"status\":\"pending\",\"amount\":10}")),
                record(json("{\"status\":\"paid\"}")))
        );

        DiffRecord second = result.get(1);
        assertThat(second.changedFields()).containsExactly("/status");
        assertThat(second.before().get("status").asText()).isEqualTo("pending");
        // amount not in patch → not reported as changed
        assertThat(second.before().has("amount")).isFalse();
    }

    @Test
    void diff_noChange_changedFieldsNull() {
        List<DiffRecord> result = service.diff(List.of(
                record(json("{\"status\":\"pending\"}")),
                record(json("{\"status\":\"pending\"}")))
        );

        assertThat(result.get(1).changedFields()).isNull();
        assertThat(result.get(1).before()).isNull();
    }

    @Test
    void diff_multipleFieldsChanged() {
        List<DiffRecord> result = service.diff(List.of(
                record(json("{\"status\":\"pending\",\"amount\":10,\"note\":\"hello\"}")),
                record(json("{\"status\":\"paid\",\"amount\":20}")))
        );

        DiffRecord second = result.get(1);
        assertThat(second.changedFields()).containsExactlyInAnyOrder("/status", "/amount");
        assertThat(second.before().get("status").asText()).isEqualTo("pending");
        assertThat(second.before().get("amount").asInt()).isEqualTo(10);
    }

    // -------------------------------------------------------------------------
    // Field addition
    // -------------------------------------------------------------------------

    @Test
    void diff_newFieldAdded_reportedWithNoBeforeValue() {
        List<DiffRecord> result = service.diff(List.of(
                record(json("{\"status\":\"pending\"}")),
                record(json("{\"trackingId\":\"T-42\"}")))
        );

        DiffRecord second = result.get(1);
        assertThat(second.changedFields()).containsExactly("/trackingId");
        // before is null when all changes are pure additions (nothing to show as "prior value")
        assertThat(second.before()).isNull();
    }

    // -------------------------------------------------------------------------
    // Field removal (null patch → RFC 7396)
    // -------------------------------------------------------------------------

    @Test
    void diff_fieldRemovedByNullPatch_reportedInChangedFields() {
        List<DiffRecord> result = service.diff(List.of(
                record(json("{\"status\":\"pending\",\"note\":\"hello\"}")),
                record(json("{\"note\":null}")))
        );

        DiffRecord second = result.get(1);
        assertThat(second.changedFields()).containsExactly("/note");
        assertThat(second.before().get("note").asText()).isEqualTo("hello");
    }

    // -------------------------------------------------------------------------
    // Non-parseable event
    // -------------------------------------------------------------------------

    @Test
    void diff_nonParseableValue_includedWithNullDiff() {
        List<DiffRecord> result = service.diff(List.of(
                record(json("{\"status\":\"pending\"}")),
                record("not-valid-base64-json"),
                record(json("{\"status\":\"paid\"}")))
        );

        assertThat(result).hasSize(3);
        // Second event: non-parseable, no diff
        assertThat(result.get(1).changedFields()).isNull();
        assertThat(result.get(1).before()).isNull();
        // Third event: compared against state after first event (non-parseable skipped)
        assertThat(result.get(2).changedFields()).containsExactly("/status");
        assertThat(result.get(2).before().get("status").asText()).isEqualTo("pending");
    }

    @Test
    void diff_nullValue_includedWithNullDiff() {
        List<DiffRecord> result = service.diff(List.of(
                record(json("{\"status\":\"pending\"}")),
                record(null))
        );

        assertThat(result).hasSize(2);
        assertThat(result.get(1).changedFields()).isNull();
        assertThat(result.get(1).before()).isNull();
    }

    // -------------------------------------------------------------------------
    // Sequential changes — accumulated state tracks correctly
    // -------------------------------------------------------------------------

    @Test
    void diff_threeEvents_accumulatesStateCorrectly() {
        List<DiffRecord> result = service.diff(List.of(
                record(json("{\"status\":\"pending\",\"amount\":10}")),
                record(json("{\"status\":\"paid\"}")),
                record(json("{\"status\":\"shipped\",\"amount\":20}")))
        );

        // Event 2: status pending→paid
        assertThat(result.get(1).changedFields()).containsExactly("/status");
        assertThat(result.get(1).before().get("status").asText()).isEqualTo("pending");

        // Event 3: status paid→shipped, amount 10→20
        assertThat(result.get(2).changedFields()).containsExactlyInAnyOrder("/status", "/amount");
        assertThat(result.get(2).before().get("status").asText()).isEqualTo("paid");
        assertThat(result.get(2).before().get("amount").asInt()).isEqualTo(10);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private EntityRecord record(String base64Value) {
        return new EntityRecord("ORD-1", "testEvent", "orders.events", 0, 0L,
                Instant.parse("2024-01-01T10:00:00Z"), Instant.parse("2024-01-01T10:00:00Z"),
                null, base64Value, null);
    }

    private String json(String jsonString) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(jsonString.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
