package com.joxette.replay;

import com.joxette.replay.SequenceMatchService.MatchStep;
import com.joxette.replay.SequenceMatchService.MatchedSequence;
import com.joxette.replay.SequenceMatchService.SequenceConstraints;
import com.joxette.replay.SequenceMatchService.SequenceMatchRequest;
import com.joxette.replay.SequenceMatchService.SequenceMatchResponse;
import com.joxette.replay.transform.Predicate;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SequenceMatchService} — no Spring context, no database.
 *
 * <p>All test cases are parameterized via {@code @MethodSource} so that each
 * scenario is a self-contained {@link TestCase} record fed into one assertion
 * method.  This matches the project's preference for {@code @ParameterizedTest}.
 */
class SequenceMatchServiceTest {

    private static final Base64.Encoder ENC = Base64.getUrlEncoder().withoutPadding();
    private static final Instant        T0  = Instant.parse("2025-01-01T00:00:00Z");

    private final SequenceMatchService svc = new SequenceMatchService();

    // =========================================================================
    // Test case record
    // =========================================================================

    record TestCase(
            String          description,
            List<MatchStep> steps,
            SequenceConstraints constraints,
            Integer         limit,
            List<CassetteRecord> records,
            long            expectedMatched,
            int             expectedExamples,
            double[]        expectedReachRates   // null means skip reach-rate check
    ) {}

    // =========================================================================
    // Factories
    // =========================================================================

    private static String b64(String json) {
        return ENC.encodeToString(json.getBytes(StandardCharsets.UTF_8));
    }

    /** Creates a cassette record whose value JSON contains {@code "type":"<type>"}. */
    private static CassetteRecord rec(String type, long offsetMillis) {
        Instant ts = T0.plusMillis(offsetMillis);
        String  v  = b64("{\"type\":\"" + type + "\"}");
        return new CassetteRecord("orders", 0, offsetMillis, ts, ts, null, v, null, null);
    }

    private static MatchStep step(String typeValue, boolean required, boolean repeated, String gap) {
        Predicate p = new Predicate.Leaf("$.value.type", Predicate.Operator.EQ, typeValue);
        return new MatchStep(p, required, repeated, gap);
    }

    private static SequenceMatchRequest req(List<MatchStep> steps, SequenceConstraints c, Integer limit) {
        return new SequenceMatchRequest(steps, c, limit, null, null);
    }

    // =========================================================================
    // Test cases
    // =========================================================================

    static Stream<TestCase> cases() {
        return Stream.of(

            // ── 1. Empty cassette ──────────────────────────────────────────
            new TestCase(
                "empty cassette → 0 matches",
                List.of(step("A", true, false, "any"), step("B", true, false, "any")),
                null, null,
                List.of(),
                0L, 0, null
            ),

            // ── 2. No messages match first step ───────────────────────────
            new TestCase(
                "no messages match first step → 0 matches",
                List.of(step("X", true, false, "any"), step("Y", true, false, "any")),
                null, null,
                List.of(rec("A", 0), rec("B", 100), rec("C", 200)),
                0L, 0, null
            ),

            // ── 3. 2-step required sequence (A → B) ───────────────────────
            new TestCase(
                "2-step required sequence finds one match",
                List.of(step("A", true, false, "any"), step("B", true, false, "any")),
                null, null,
                List.of(rec("A", 0), rec("B", 500)),
                1L, 1, null
            ),

            // ── 4. 2-step required: wildcard between steps (gap=any) ───────
            new TestCase(
                "wildcard message between A and B is allowed with gap=any",
                List.of(step("A", true, false, "any"), step("B", true, false, "any")),
                null, null,
                List.of(rec("A", 0), rec("W", 100), rec("W", 200), rec("B", 300)),
                1L, 1, null
            ),

            // ── 5. Optional middle step ────────────────────────────────────
            // NFA forks when it advances past A: one thread waits for M, another
            // skips M and waits for B.  For [A, M, B] both forks complete
            // (A→M→B and A→skip-M→B).  For the second [A, B] the skip-M fork
            // completes (A→B).  Total = 3.
            new TestCase(
                "optional middle step: sequences with and without it both match",
                List.of(
                    step("A", true,  false, "any"),
                    step("M", false, false, "any"),   // optional
                    step("B", true,  false, "any")
                ),
                null, null,
                List.of(
                    rec("A", 0), rec("M", 100), rec("B", 200),
                    rec("A", 1000), rec("B", 1100)
                ),
                3L, 3, null
            ),

            // ── 6. Repeated step ──────────────────────────────────────────
            // Each M keeps a "stay" thread at step 1 and advances a new thread
            // to step 2.  With M×3 before B, three threads are at step 2 when
            // B arrives → 3 complete sequences.
            new TestCase(
                "repeated step produces a match per repetition-count prefix",
                List.of(
                    step("A", true, false, "any"),
                    step("M", true, true,  "any"),   // required + repeated
                    step("B", true, false, "any")
                ),
                null, null,
                List.of(rec("A", 0), rec("M", 100), rec("M", 200), rec("M", 300), rec("B", 400)),
                3L, 3, null
            ),

            // ── 7. Immediate gap — wildcard kills thread ───────────────────
            new TestCase(
                "gap=immediate: wildcard between steps kills the thread",
                List.of(step("A", true, false, "immediate"), step("B", true, false, "immediate")),
                null, null,
                List.of(rec("A", 0), rec("W", 50), rec("B", 100)),
                0L, 0, null
            ),

            // ── 8. Immediate gap — direct adjacency succeeds ───────────────
            new TestCase(
                "gap=immediate: A immediately followed by B matches",
                List.of(step("A", true, false, "immediate"), step("B", true, false, "immediate")),
                null, null,
                List.of(rec("A", 0), rec("B", 100)),
                1L, 1, null
            ),

            // ── 9. maxDurationMs constraint filters out slow sequences ─────
            new TestCase(
                "maxDurationMs=200 filters out 500ms sequence",
                List.of(step("A", true, false, "any"), step("B", true, false, "any")),
                new SequenceConstraints(200L, null), null,
                List.of(rec("A", 0), rec("B", 500)),
                0L, 0, null
            ),

            // ── 10. maxDurationMs: fast sequence passes ────────────────────
            new TestCase(
                "maxDurationMs=1000 allows 500ms sequence",
                List.of(step("A", true, false, "any"), step("B", true, false, "any")),
                new SequenceConstraints(1000L, null), null,
                List.of(rec("A", 0), rec("B", 500)),
                1L, 1, null
            ),

            // ── 11. minDurationMs constraint ──────────────────────────────
            new TestCase(
                "minDurationMs=600 filters out 500ms sequence",
                List.of(step("A", true, false, "any"), step("B", true, false, "any")),
                new SequenceConstraints(null, 600L), null,
                List.of(rec("A", 0), rec("B", 500)),
                0L, 0, null
            ),

            // ── 12. limit caps examples but not matchedSequences count ─────
            new TestCase(
                "limit=1 caps examples list at 1 while matchedSequences counts all",
                List.of(step("A", true, false, "any"), step("B", true, false, "any")),
                null, 1,
                List.of(
                    rec("A", 0),   rec("B", 100),
                    rec("A", 200), rec("B", 300),
                    rec("A", 400), rec("B", 500)
                ),
                3L, 1, null
            )
        );
    }

    // =========================================================================
    // Parameterized assertion
    // =========================================================================

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void runTestCase(TestCase tc) {
        SequenceMatchRequest req = req(tc.steps(), tc.constraints(), tc.limit());
        SequenceMatchResponse resp = svc.match(req, tc.records());

        assertThat(resp.matchedSequences())
                .as("matchedSequences for: %s", tc.description())
                .isEqualTo(tc.expectedMatched());

        assertThat(resp.examples())
                .as("examples.size() for: %s", tc.description())
                .hasSizeLessThanOrEqualTo(Math.max(tc.expectedExamples(), tc.expectedExamples()));

        if (tc.expectedMatched() == 0) {
            assertThat(resp.examples()).as("examples empty for: %s", tc.description()).isEmpty();
        } else if (tc.expectedExamples() > 0) {
            assertThat(resp.examples())
                    .as("examples non-empty for: %s", tc.description())
                    .isNotEmpty();
        }

        assertThat(resp.totalMessages())
                .as("totalMessages for: %s", tc.description())
                .isEqualTo(tc.records().size());

        if (tc.expectedReachRates() != null) {
            assertThat(resp.reachRates())
                    .as("reachRates for: %s", tc.description())
                    .containsExactly(tc.expectedReachRates());
        }

        // Verify example invariants when examples are present.
        for (MatchedSequence example : resp.examples()) {
            assertThat(example.messages())
                    .as("example messages non-empty for: %s", tc.description())
                    .isNotEmpty();
            assertThat(example.anchorTimestamps())
                    .as("example anchors non-empty for: %s", tc.description())
                    .isNotEmpty();
            assertThat(example.durationMs())
                    .as("example durationMs non-negative for: %s", tc.description())
                    .isGreaterThanOrEqualTo(0L);
        }
    }
}
