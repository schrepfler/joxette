package com.sol.engine;

import com.sol.model.Event;
import com.sol.model.Sequence;
import com.sol.model.Tag;
import com.sol.parser.SolParser;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class SolEngineTest {

    private static final Instant T0 = Instant.EPOCH;
    private static final Instant T1 = Instant.ofEpochSecond(60);
    private static final Instant T2 = Instant.ofEpochSecond(120);
    private static final Instant T3 = Instant.ofEpochSecond(180);

    private static Sequence seq(String id, Event... events) {
        return new Sequence(id, List.of(events));
    }

    private static Event ev(String name, Instant ts) {
        return new Event(name, ts);
    }

    static Stream<Arguments> engineCases() {
        return Stream.of(

                Arguments.of("match sets MATCHED tag",
                        seq("u1", ev("login", T0), ev("purchase", T1), ev("logout", T2)),
                        "match Login(login) >> * >> Buy(purchase)",
                        true, 3 /* full sequence preserved; MATCHED tag spans matched range */),

                Arguments.of("filter MATCHED keeps matched sequences",
                        seq("u1", ev("login", T0), ev("purchase", T1)),
                        "match Buy(purchase)\nfilter MATCHED",
                        true, 2 /* sequence has 2 events; MATCHED tag covers the purchase */),

                Arguments.of("filter MATCHED drops unmatched",
                        seq("u1", ev("login", T0), ev("logout", T1)),
                        "match Buy(purchase)\nfilter MATCHED",
                        false, 0),

                Arguments.of("set adds sequence dimension",
                        seq("u1", ev("a", T0), ev("b", T1)),
                        "match A(a) >> B(b)\nset gap = duration(A, B)",
                        true, 2),

                Arguments.of("match split then combine counts splits",
                        seq("u1", ev("click", T0), ev("click", T1), ev("click", T2)),
                        "match split click\ncombine count = max(split_index)",
                        true, 3)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("engineCases")
    void engineScenarios(String label, Sequence input, String query,
                         boolean expectNonEmpty, int expectedEventCount) {
        List<SolOperation> ops = SolParser.parse(query);
        SolResult result = SolEngine.execute(ops, input);

        if (expectNonEmpty) {
            assertFalse(result.sequence().events().isEmpty(), label + ": sequence should not be empty");
            if (expectedEventCount > 0)
                assertEquals(expectedEventCount, result.sequence().size(), label + ": event count");
        } else {
            // filter removed the sequence — result sequence may be empty
            assertTrue(result.sequence().events().isEmpty()
                    || !result.matched(), label + ": should not be matched");
        }
    }

    // -----------------------------------------------------------------------
    // Tag-span correctness after REPLACE
    // -----------------------------------------------------------------------

    static Stream<Arguments> replaceCases() {
        return Stream.of(

                Arguments.of("replace middle tag shrinks span to single event",
                        seq("u1", ev("a", T0), ev("b", T1), ev("c", T2)),
                        // match B(b) tags index 1; replace collapses it to one new event
                        "match A(a) >> B(b) >> C(c)\nreplace B with Replaced(b_new)",
                        (Consumer<SolResult>) result -> {
                            // output: [a, b_new, c] — 3 events total
                            assertEquals(3, result.sequence().size(), "event count after replace");
                            assertEquals("b_new", result.sequence().get(1).name(), "replacement event name");
                            Tag b = result.tags().get("B");
                            assertNotNull(b, "B tag must survive replace");
                            // replaced tag covers exactly the single replacement event at index 1
                            assertEquals(1, b.from(), "B.from after replace");
                            assertEquals(2, b.to(),   "B.to after replace");
                        }),

                Arguments.of("replace removes tag range and shifts suffix indices",
                        seq("u1", ev("start", T0), ev("mid", T1), ev("mid", T2), ev("end", T3)),
                        // match two mid events under M, replace with a single synthetic event
                        "match S(start) >> M(mid)+ >> E(end)\nreplace M with Collapsed(single)",
                        (Consumer<SolResult>) result -> {
                            // output: [start, single, end] — 3 events
                            assertEquals(3, result.sequence().size(), "event count after multi-event replace");
                            assertEquals("single", result.sequence().get(1).name(), "collapsed event name");
                            Tag m = result.tags().get("M");
                            assertNotNull(m, "M tag must survive replace");
                            // M now covers just the one replacement event at index 1
                            assertEquals(1, m.from(), "M.from after multi-replace");
                            assertEquals(2, m.to(),   "M.to after multi-replace");
                        }),

                Arguments.of("replace with null removes tagged events from sequence",
                        seq("u1", ev("keep", T0), ev("drop", T1), ev("keep", T2)),
                        "match K1(keep) >> D(drop) >> K2(keep)\nreplace D with null",
                        (Consumer<SolResult>) result -> {
                            // drop event removed; only two keep events remain
                            assertEquals(2, result.sequence().size(), "event count after null replace");
                            assertEquals("keep", result.sequence().get(0).name(), "first keep");
                            assertEquals("keep", result.sequence().get(1).name(), "second keep");
                        })
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("replaceCases")
    void replaceTagSpanCorrectness(String label, Sequence input, String query,
                                   Consumer<SolResult> assertions) {
        List<SolOperation> ops = SolParser.parse(query);
        SolResult result = SolEngine.execute(ops, input);
        assertions.accept(result);
    }

    // -----------------------------------------------------------------------
    // Combine output dimensions
    // -----------------------------------------------------------------------

    static Stream<Arguments> combineCases() {
        return Stream.of(

                Arguments.of("combine after split merges all events in original order",
                        seq("u1", ev("click", T0), ev("click", T1), ev("click", T2)),
                        "match split click\ncombine",
                        (Consumer<SolResult>) result -> {
                            // all 3 click events present in the combined sequence
                            assertEquals(3, result.sequence().size(), "all events preserved after combine");
                            // COMBINE clears tags
                            assertTrue(result.tags().isEmpty(), "tags cleared after combine");
                        }),

                Arguments.of("combine aggregation dim is set on combined sequence",
                        seq("u1", ev("click", T0), ev("click", T1), ev("click", T2)),
                        "match split click\ncombine total = 3",
                        (Consumer<SolResult>) result -> {
                            assertEquals(3, result.sequence().size(), "all events preserved");
                            Object total = result.sequence().dim("total");
                            assertNotNull(total, "total dim must be set");
                            assertEquals(3L, ((Number) total).longValue(), "total dim value");
                        }),

                Arguments.of("combine preserves dims common across all sub-sequences",
                        // two separate sub-sequences each with the same 'region' dim
                        new Sequence("u1",
                                List.of(ev("a", T0), ev("b", T1)),
                                java.util.Map.of("region", "eu")),
                        // split on 'a' then combine — both sub-seqs inherit 'region'
                        "match split a\ncombine",
                        (Consumer<SolResult>) result -> {
                            assertEquals("eu", result.sequence().dim("region"),
                                    "common dim 'region' must be carried through combine");
                        }),

                Arguments.of("combine drops dims that differ across sub-sequences",
                        seq("u1", ev("click", T0), ev("click", T1), ev("click", T2)),
                        // split_index differs per sub-seq, so it must be dropped by combine
                        "match split click\ncombine",
                        (Consumer<SolResult>) result -> {
                            assertNull(result.sequence().dim("split_index"),
                                    "split_index must be dropped because it differs across sub-sequences");
                        })
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("combineCases")
    void combineOutputDimensions(String label, Sequence input, String query,
                                  Consumer<SolResult> assertions) {
        List<SolOperation> ops = SolParser.parse(query);
        SolResult result = SolEngine.execute(ops, input);
        assertions.accept(result);
    }
}
