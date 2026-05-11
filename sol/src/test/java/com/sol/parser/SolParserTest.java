package com.sol.parser;

import com.sol.engine.SolOperation;
import com.sol.engine.SolOperation.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class SolParserTest {

    static Stream<Arguments> parseCases() {
        return Stream.of(
                Arguments.of("single match",
                        "match purchase",
                        1, MatchOp.class),

                Arguments.of("match split",
                        "match split click",
                        1, MatchSplitOp.class),

                Arguments.of("match with tag and wildcard",
                        "match Search(search) >> * >> Buy(purchase)",
                        1, MatchOp.class),

                Arguments.of("match then filter",
                        "match purchase\nfilter MATCHED",
                        2, MatchOp.class),

                Arguments.of("match with if condition",
                        "match A(login) >> * >> B(logout)\nif duration(A, B) < 1h",
                        1, MatchOp.class),

                Arguments.of("set operation",
                        "match A(a) >> B(b)\nset my_dim = duration(A, B)",
                        2, MatchOp.class),

                Arguments.of("combine",
                        "match split click\ncombine total = max(split_index)",
                        2, MatchSplitOp.class),

                Arguments.of("replace with null",
                        "match Tag(bad_event)\nreplace Tag with null",
                        2, MatchOp.class)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parseCases")
    void parserScenarios(String label, String query, int expectedOps, Class<?> firstOpType) {
        List<SolOperation> ops = SolParser.parse(query);
        assertEquals(expectedOps, ops.size(), label + ": op count");
        assertInstanceOf(firstOpType, ops.getFirst(), label + ": first op type");
    }

    @ParameterizedTest(name = "quantifier: {0}")
    @MethodSource("quantifierCases")
    void quantifierParsing(String query, int min, int max) {
        List<SolOperation> ops = SolParser.parse(query);
        MatchOp match = (MatchOp) ops.getFirst();
        // find the non-anchor element with the quantifier
        PatternElement elem = match.pattern().stream()
                .filter(e -> e.anchor() == null && !e.isWildcard())
                .findFirst().orElseThrow();
        assertEquals(min, elem.quantifier().min(), query + ": min");
        assertEquals(max, elem.quantifier().max(), query + ": max");
    }

    static Stream<Arguments> quantifierCases() {
        return Stream.of(
                Arguments.of("match click+",    1, -1),
                Arguments.of("match click*",    0, -1),
                Arguments.of("match click?",    0,  1),
                Arguments.of("match click{3}",  3,  3),
                Arguments.of("match click{2,5}", 2, 5),
                Arguments.of("match click{2,}",  2, -1),
                Arguments.of("match click{,4}",  0,  4)
        );
    }
}
