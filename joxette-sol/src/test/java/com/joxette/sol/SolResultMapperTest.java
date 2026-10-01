package com.joxette.sol;

import com.joxette.replay.EntityRecord;
import com.sol.engine.SolEngine;
import com.sol.engine.SolResult;
import com.sol.model.Tag;
import com.sol.parser.SolParser;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The UI draws tag spans over the returned records by index, so the spans must be
 * expressed in the coordinates of {@link SolResultMapper.Mapped#records()}, not of
 * the engine's final sequence (which also holds synthetic events with no record).
 */
class SolResultMapperTest {

    private static List<EntityRecord> records(String... types) {
        List<EntityRecord> out = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            out.add(new EntityRecord("e1", types[i], "t", 0, i,
                    Instant.ofEpochSecond(i), Instant.ofEpochSecond(i), null, "{}", List.of()));
        }
        return out;
    }

    private static SolResultMapper.Mapped run(String query, List<EntityRecord> records) {
        SolResult result = SolEngine.execute(SolParser.parse(query),
                EntityRecordAdapter.toSequence("e1", records));
        return SolResultMapper.map(result, records);
    }

    private static List<String> typesIn(SolResultMapper.Mapped mapped, String tag) {
        Tag t = mapped.tags().get(tag);
        return mapped.records().subList(t.from(), t.to()).stream().map(EntityRecord::messageType).toList();
    }

    @Test
    void plainMatch_spansIndexTheRecordsUnchanged() {
        SolResultMapper.Mapped mapped = run("match A(a) >> B(b)", records("x", "a", "b", "y"));

        assertEquals(4, mapped.records().size());
        assertEquals(List.of("a"), typesIn(mapped, "A"));
        assertEquals(List.of("b"), typesIn(mapped, "B"));
    }

    @Test
    void replaceWithSyntheticEvent_laterTagsStillCoverTheirOwnRecords() {
        // b_new is synthetic: it is in the engine's sequence but has no record, so it is
        // dropped from the response — C must still land on "c", not on the event after it.
        SolResultMapper.Mapped mapped = run("match A(a) >> B(b)+ >> C(c)\nreplace B with X(b_new)",
                records("a", "b", "b", "c", "z"));

        assertEquals(List.of("a", "c", "z"),
                mapped.records().stream().map(EntityRecord::messageType).toList());
        assertEquals(List.of("a"), typesIn(mapped, "A"));
        assertEquals(List.of(), typesIn(mapped, "B"), "B now holds only the synthetic event");
        assertEquals(List.of("c"), typesIn(mapped, "C"));
    }

    @Test
    void filterDroppingTheSequence_leavesNoRecordsAndOnlyEmptySpans() {
        SolResultMapper.Mapped mapped = run("match A(a) >> B(b)\nfilter false", records("a", "b"));

        assertTrue(mapped.records().isEmpty());
        for (Map.Entry<String, Tag> e : mapped.tags().entrySet()) {
            assertEquals(0, e.getValue().length(), e.getKey() + " must be empty");
        }
    }
}
