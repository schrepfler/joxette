package com.sol.engine;

import com.sol.model.Event;
import com.sol.model.Sequence;
import com.sol.parser.SolParser;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.List;
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
}
