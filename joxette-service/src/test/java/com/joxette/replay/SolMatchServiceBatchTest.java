package com.joxette.replay;

import com.joxette.replay.SolMatchService.SolBatchResult;
import com.joxette.replay.SolMatchService.SolSequenceExample;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SolMatchService#matchAcrossEntities} — the batch
 * "examples" matcher behind {@code POST /cassettes/entities/{type}/sol-examples}.
 *
 * <p>Entity listing and event streaming are stubbed so the aggregation logic
 * (totals, pattern-ordered model rows incl. Prefix/gap/Suffix, example shaping,
 * caps and truncation) is exercised without a database.
 */
class SolMatchServiceBatchTest {

    private static final Instant T0 = Instant.EPOCH;

    private EntityReplayService replay;
    private SolMatchService service;

    /** entityId → ordered event-type names served by the stubbed stream. */
    private final Map<String, List<String>> sequences = new LinkedHashMap<>();

    @BeforeEach
    void setUp() throws SQLException {
        replay = mock(EntityReplayService.class);
        service = new SolMatchService(replay);

        doAnswer(inv -> {
            String entityId = inv.getArgument(1);
            Consumer<EntityRecord> sink = inv.getArgument(4);
            List<String> names = sequences.getOrDefault(entityId, List.of());
            for (int i = 0; i < names.size(); i++) {
                sink.accept(record(entityId, names.get(i), i));
            }
            return null;
        }).when(replay).streamEntityEvents(eq("order"), anyString(), isNull(), isNull(), any());
    }

    private void stubEntities(String... ids) throws SQLException {
        List<EntityInfo> infos = new ArrayList<>();
        for (String id : ids) {
            infos.add(new EntityInfo("order", id, T0, T0, 1, List.of("t"), null));
        }
        when(replay.listEntities(eq("order"), anyInt(), isNull(), any()))
                .thenReturn(new PagedResponse<>(infos, null, false, null, null));
    }

    private static EntityRecord record(String entityId, String messageType, int i) {
        return new EntityRecord(entityId, messageType, "t", 0, i,
                T0.plusSeconds(i), T0.plusSeconds(i), null, null, List.of());
    }

    @Test
    void aggregatesModelRowsInPatternOrderWithPrefixGapAndSuffix() throws SQLException {
        sequences.put("e1", List.of("home", "click", "search", "tail"));   // gap + suffix
        sequences.put("e2", List.of("home", "search"));                    // adjacent, no gap/suffix
        sequences.put("e3", List.of("intro", "home", "search"));           // prefix
        sequences.put("e4", List.of("click", "click"));                    // unmatched
        stubEntities("e1", "e2", "e3", "e4");

        SolBatchResult result = service.matchAcrossEntities(
                "order", "match home >> * >> search", null, null, 100, 25);

        assertThat(result.totalSequences()).isEqualTo(4);
        assertThat(result.matchedSequences()).isEqualTo(3);

        // Prefix → home → (gap) → search → Suffix, with per-row sequence counts
        assertThat(result.model()).extracting(r -> r.gap() ? "·gap·" : r.label())
                .containsExactly("Prefix", "home", "·gap·", "search", "Suffix");
        assertThat(result.model()).extracting(SolMatchService.ModelRow::count)
                .containsExactly(1, 3, 1, 3, 1);
    }

    @Test
    void examplesCarryEventsSpansAndMatchedFlag() throws SQLException {
        sequences.put("e1", List.of("home", "click", "search"));
        sequences.put("e4", List.of("click", "click"));
        stubEntities("e1", "e4");

        SolBatchResult result = service.matchAcrossEntities(
                "order", "match home >> * >> search", null, null, 100, 25);

        assertThat(result.examples()).hasSize(2);
        SolSequenceExample matched = result.examples().get(0);
        assertThat(matched.entityId()).isEqualTo("e1");
        assertThat(matched.matched()).isTrue();
        assertThat(matched.events()).containsExactly("home", "click", "search");
        assertThat(matched.tags()).containsKeys("home", "search");
        assertThat(matched.tags().get("home").from()).isZero();
        assertThat(matched.tags().get("search").from()).isEqualTo(2);
        // SEQ / MATCHED noise excluded from example spans
        assertThat(matched.tags()).doesNotContainKeys("SEQ", "MATCHED");

        SolSequenceExample unmatched = result.examples().get(1);
        assertThat(unmatched.matched()).isFalse();
        assertThat(unmatched.tags()).isEmpty();
    }

    @Test
    void respectsMaxSequencesAndExampleLimit() throws SQLException {
        sequences.put("e1", List.of("home", "search"));
        sequences.put("e2", List.of("home", "search"));
        sequences.put("e3", List.of("home", "search"));
        stubEntities("e1", "e2", "e3");

        SolBatchResult result = service.matchAcrossEntities(
                "order", "match home", null, null, 2, 1);

        assertThat(result.totalSequences()).isEqualTo(2);   // maxSequences cap
        assertThat(result.examples()).hasSize(1);           // exampleLimit cap
    }

    @Test
    void longSequencesAreTruncatedAndOutOfRangeSpansDropped() throws SQLException {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < 100; i++) names.add("step");
        names.add("finale");   // index 100 — beyond the 80-event example cap
        sequences.put("e1", names);
        stubEntities("e1");

        SolBatchResult result = service.matchAcrossEntities(
                "order", "match finale", null, null, 100, 25);

        SolSequenceExample ex = result.examples().get(0);
        assertThat(ex.truncated()).isTrue();
        assertThat(ex.events()).hasSize(SolMatchService.MAX_EXAMPLE_EVENTS);
        // The finale span starts beyond the cap → dropped from the example
        assertThat(ex.tags()).doesNotContainKey("finale");
        // …but it still counts toward the aggregate model
        assertThat(result.model()).anySatisfy(r -> {
            if (!r.gap() && "finale".equals(r.label())) assertThat(r.count()).isEqualTo(1);
        });
    }
}
