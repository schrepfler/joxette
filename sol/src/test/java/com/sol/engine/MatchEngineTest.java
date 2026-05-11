package com.sol.engine;

import com.sol.engine.SolOperation.PatternElement;
import com.sol.engine.SolOperation.Quantifier;
import com.sol.expr.EvalContext;
import com.sol.model.Event;
import com.sol.model.Sequence;
import com.sol.model.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class MatchEngineTest {

    private static final Instant T = Instant.EPOCH;

    private static Sequence seq(String... names) {
        List<Event> events = java.util.Arrays.stream(names)
                .map(n -> new Event(n, T)).toList();
        return new Sequence("test", events);
    }

    private static PatternElement elem(String tagName, String eventName) {
        return new PatternElement(tagName, List.of(eventName), List.of(), Quantifier.ONE, null);
    }

    private static PatternElement wildcard() {
        return new PatternElement(null, List.of(), List.of(), Quantifier.ZERO_MORE, null);
    }

    private static PatternElement elemPlus(String tagName, String eventName) {
        return new PatternElement(tagName, List.of(eventName), List.of(), Quantifier.ONE_MORE, null);
    }

    static Stream<Arguments> matchCases() {
        return Stream.of(
                // simple single-event match
                Arguments.of("simple event match",
                        seq("login", "purchase", "logout"),
                        List.of(elem("A", "purchase")),
                        true, "A", 1, 2),

                // consecutive two events
                Arguments.of("consecutive events",
                        seq("a", "b", "c"),
                        List.of(elem("A", "a"), elem("B", "b")),
                        true, "A", 0, 1),

                // wildcard between
                Arguments.of("wildcard between",
                        seq("search", "browse", "browse", "purchase"),
                        List.of(elem("S", "search"), wildcard(), elem("P", "purchase")),
                        true, "P", 3, 4),

                // no match
                Arguments.of("no match",
                        seq("login", "logout"),
                        List.of(elem("P", "purchase")),
                        false, null, -1, -1),

                // one-or-more quantifier
                Arguments.of("one-or-more match",
                        seq("click", "click", "click", "buy"),
                        List.of(elemPlus("Clicks", "click")),
                        true, "Clicks", 0, 3)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("matchCases")
    void matchScenarios(String label, Sequence seq, List<PatternElement> pattern,
                        boolean expectMatch, String checkTag, int expectFrom, int expectTo) {
        EvalContext ctx = new EvalContext(seq, Map.of());
        Optional<Map<String, Tag>> result = MatchEngine.findFirst(seq, pattern, null, 0, ctx);

        assertEquals(expectMatch, result.isPresent(), label);
        if (expectMatch && checkTag != null) {
            Tag tag = result.get().get(checkTag);
            assertNotNull(tag, label + ": tag '" + checkTag + "' missing");
            assertEquals(expectFrom, tag.from(), label + ": from");
            assertEquals(expectTo,   tag.to(),   label + ": to");
        }
    }
}
