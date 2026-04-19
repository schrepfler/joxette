package com.joxette.replay.transform.gap;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.Predicate;
import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformContext;
import com.joxette.replay.transform.TransformContext.ResolvedFragment;
import com.joxette.replay.transform.gap.MessagePattern.Quantifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link GapEvaluator}: anchor tracking, fragment resolution,
 * gap selector matching, and gap operation application.
 */
class GapEvaluatorTest {

    private static final Base64.Encoder ENC = Base64.getUrlEncoder().withoutPadding();

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static String b64(String s) {
        return ENC.encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    /** Creates a message with a given timestamp and JSON value (base64-encoded). */
    private static ReplayMessage msg(Instant ts, String valueJson) {
        String enc = valueJson != null ? b64(valueJson) : null;
        return new ReplayMessage(new CassetteRecord("t", 0, 0L, ts, ts, "k", enc, null, null));
    }

    private static ReplayMessage msg(Instant ts) {
        return msg(ts, "{\"type\":\"Unrelated\"}");
    }

    private static Predicate eqPredicate(String field, String value) {
        return new Predicate.Leaf(field, Predicate.Operator.EQ, value);
    }

    private static MessagePattern pattern(Predicate pred, Quantifier q) {
        return new MessagePattern(pred, q);
    }

    // -------------------------------------------------------------------------
    // Predicate constants
    // -------------------------------------------------------------------------

    private static final Predicate ORDER_CREATED  = eqPredicate("$.value.type", "OrderCreated");
    private static final Predicate PAYMENT_SENT   = eqPredicate("$.value.type", "PaymentSent");
    private static final Predicate PAYMENT_DONE   = eqPredicate("$.value.type", "PaymentCompleted");

    // -------------------------------------------------------------------------
    // applyOperation — all 5 variants
    // -------------------------------------------------------------------------

    static Stream<Arguments> applyOperationCases() {
        return Stream.of(
            Arguments.of("cut",           Duration.ofMillis(5000), new GapOperation.Cut(),              Duration.ZERO),
            Arguments.of("hold",          Duration.ofMillis(5000), new GapOperation.Hold(500),           Duration.ofMillis(500)),
            Arguments.of("trim_by_ms",    Duration.ofMillis(5000), new GapOperation.Trim(2000L, null),   Duration.ofMillis(3000)),
            Arguments.of("trim_clamp",    Duration.ofMillis(1000), new GapOperation.Trim(3000L, null),   Duration.ZERO),
            Arguments.of("trim_by_factor",Duration.ofMillis(5000), new GapOperation.Trim(null, 0.5),     Duration.ofMillis(2500)),
            Arguments.of("pad",           Duration.ofMillis(5000), new GapOperation.Pad(1000),           Duration.ofMillis(6000)),
            Arguments.of("scale",         Duration.ofMillis(5000), new GapOperation.Scale(0.1),          Duration.ofMillis(500)),
            Arguments.of("scale_up",      Duration.ofMillis(5000), new GapOperation.Scale(2.0),          Duration.ofMillis(10000))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("applyOperationCases")
    void applyOperation_returnsExpectedDuration(String label, Duration gap, GapOperation op, Duration expected) {
        assertThat(GapEvaluator.applyOperation(gap, op)).isEqualTo(expected);
    }

    // -------------------------------------------------------------------------
    // resolveGapDuration — first message returns empty
    // -------------------------------------------------------------------------

    @Test
    void resolveGap_firstMessage_returnsEmpty() {
        TransformContext ctx = new TransformContext();
        ctx.setFragmentDefs(List.of());
        ReplayMessage first = msg(Instant.parse("2024-01-01T10:00:00Z"), "{\"type\":\"OrderCreated\"}");
        GapEvaluator.observeMessage(first, ctx);
        // prevMsgTimestamp is still null — no gap possible

        GapSelector sel = new GapSelector(
                pattern(ORDER_CREATED, Quantifier.First.INSTANCE),
                null, null, null, null);
        assertThat(GapEvaluator.resolveGapDuration(sel, first, ctx)).isEmpty();
    }

    // -------------------------------------------------------------------------
    // Quantifier: first
    // -------------------------------------------------------------------------

    @Test
    void quantifier_first_gapAfterFirstMatch() {
        Instant t0 = Instant.parse("2024-01-01T10:00:00Z");
        Instant t1 = t0.plusSeconds(5);
        Instant t2 = t1.plusSeconds(2);

        List<ReplayMessage> msgs = List.of(
                msg(t0, "{\"type\":\"OrderCreated\"}"),
                msg(t1, "{\"type\":\"Unrelated\"}"),   // gap after first OrderCreated = 5s
                msg(t2, "{\"type\":\"OrderCreated\"}") // second OrderCreated — should NOT match for "first"
        );
        MessagePattern pat = pattern(ORDER_CREATED, Quantifier.First.INSTANCE);
        ReplayMessage current = msgs.get(1);
        TransformContext stepCtx = buildCtx(msgs, 1, List.of(), pat);

        GapSelector sel = new GapSelector(
                pattern(ORDER_CREATED, Quantifier.First.INSTANCE),
                null, null, null, null);
        Optional<Duration> result = GapEvaluator.resolveGapDuration(sel, current, stepCtx);
        assertThat(result).isPresent().contains(Duration.ofSeconds(5));
    }

    // -------------------------------------------------------------------------
    // Quantifier: last
    // -------------------------------------------------------------------------

    @Test
    void quantifier_last_gapAfterLastMatch() {
        Instant t0 = Instant.parse("2024-01-01T10:00:00Z");
        Instant t1 = t0.plusSeconds(3);
        Instant t2 = t1.plusSeconds(7);
        Instant t3 = t2.plusSeconds(2);

        List<ReplayMessage> feed = List.of(
                msg(t0, "{\"type\":\"OrderCreated\"}"),
                msg(t1, "{\"type\":\"Unrelated\"}"),
                msg(t2, "{\"type\":\"OrderCreated\"}"),
                msg(t3, "{\"type\":\"Result\"}")
        );
        MessagePattern lastPat = pattern(ORDER_CREATED, Quantifier.Last.INSTANCE);
        TransformContext ctx = buildCtx(feed, 3, List.of(), lastPat);

        GapSelector sel = new GapSelector(
                pattern(ORDER_CREATED, Quantifier.Last.INSTANCE),
                null, null, null, null);
        Optional<Duration> result = GapEvaluator.resolveGapDuration(sel, feed.get(3), ctx);
        assertThat(result).isPresent().contains(Duration.ofSeconds(2));
    }

    // -------------------------------------------------------------------------
    // Quantifier: nth
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "nth={0} → gapMs={1}")
    @MethodSource("nthCases")
    void quantifier_nth_selectsCorrectOccurrence(int n, long expectedGapMs, boolean shouldMatch) {
        Instant t0 = Instant.parse("2024-01-01T10:00:00Z");
        Instant t1 = t0.plusSeconds(2);   // first OrderCreated
        Instant t2 = t1.plusSeconds(4);   // second OrderCreated
        Instant t3 = t2.plusSeconds(6);   // third OrderCreated
        Instant t4 = t3.plusSeconds(1);   // probe message

        List<ReplayMessage> feed = List.of(
                msg(t0, "{\"type\":\"Unrelated\"}"),
                msg(t1, "{\"type\":\"OrderCreated\"}"),
                msg(t2, "{\"type\":\"OrderCreated\"}"),
                msg(t3, "{\"type\":\"OrderCreated\"}"),
                msg(t4, "{\"type\":\"Probe\"}")
        );

        MessagePattern nthPat = pattern(ORDER_CREATED, new Quantifier.Nth(n));
        TransformContext ctx = buildCtx(feed, 4, List.of(), nthPat);

        GapSelector sel = new GapSelector(nthPat, null, null, null, null);
        Optional<Duration> result = GapEvaluator.resolveGapDuration(sel, feed.get(4), ctx);

        if (shouldMatch) {
            assertThat(result).isPresent().contains(Duration.ofMillis(expectedGapMs));
        } else {
            assertThat(result).isEmpty();
        }
    }

    static Stream<Arguments> nthCases() {
        return Stream.of(
            // nth=3 means "gap after the 3rd OrderCreated" — prev at t3, gap = t4-t3 = 1s
            Arguments.of(3, 1000L, true),
            // nth=4 doesn't exist — no match
            Arguments.of(4, 0L, false)
        );
    }

    // -------------------------------------------------------------------------
    // Quantifier: any
    // -------------------------------------------------------------------------

    @Test
    void quantifier_any_matchesEveryOccurrence() {
        Instant t0 = Instant.parse("2024-01-01T10:00:00Z");
        Instant t1 = t0.plusSeconds(3);
        Instant t2 = t1.plusSeconds(5);
        Instant t3 = t2.plusSeconds(2);

        List<ReplayMessage> feed = List.of(
                msg(t0, "{\"type\":\"OrderCreated\"}"),
                msg(t1, "{\"type\":\"Probe\"}"),        // gap after 1st OrderCreated = 3s
                msg(t2, "{\"type\":\"OrderCreated\"}"),
                msg(t3, "{\"type\":\"Probe\"}")         // gap after 2nd OrderCreated = 2s
        );

        MessagePattern anyPat = pattern(ORDER_CREATED, Quantifier.Any.INSTANCE);
        GapSelector sel = new GapSelector(anyPat, null, null, null, null);

        // First probe (index 1)
        TransformContext ctx1 = buildCtx(feed, 1, List.of(), anyPat);
        assertThat(GapEvaluator.resolveGapDuration(sel, feed.get(1), ctx1))
                .isPresent().contains(Duration.ofSeconds(3));

        // Second probe (index 3)
        TransformContext ctx3 = buildCtx(feed, 3, List.of(), anyPat);
        assertThat(GapEvaluator.resolveGapDuration(sel, feed.get(3), ctx3))
                .isPresent().contains(Duration.ofSeconds(2));
    }

    // -------------------------------------------------------------------------
    // Quantifier: first_after
    // -------------------------------------------------------------------------

    @Test
    void quantifier_firstAfter_requiresAnchorResolved() {
        Instant t0 = Instant.parse("2024-01-01T10:00:00Z");
        Instant t1 = t0.plusSeconds(3);
        Instant t2 = t1.plusSeconds(4);
        Instant t3 = t2.plusSeconds(1);

        // t0: OrderCreated, t1: PaymentSent (first after OrderCreated), t2: PaymentSent again, t3: probe
        List<ReplayMessage> feed = List.of(
                msg(t0, "{\"type\":\"OrderCreated\"}"),
                msg(t1, "{\"type\":\"PaymentSent\"}"),
                msg(t2, "{\"type\":\"PaymentSent\"}"),
                msg(t3, "{\"type\":\"Probe\"}")
        );

        MessagePattern anchorPattern = pattern(ORDER_CREATED, Quantifier.First.INSTANCE);
        MessagePattern firstAfterPattern = pattern(
                PAYMENT_SENT,
                new Quantifier.FirstAfter(anchorPattern));

        // selNoMatch uses the same first_after pattern
        GapSelector selNoMatch = new GapSelector(firstAfterPattern, null, null, null, null);

        // At index 3, prevMsgTimestamp = t2 ≠ t1 (first PaymentSent after OrderCreated) → no match
        TransformContext ctx = buildCtx(feed, 3, List.of(), firstAfterPattern);
        assertThat(GapEvaluator.resolveGapDuration(selNoMatch, feed.get(3), ctx)).isEmpty();

        // At index 2 (msg t2), prevMsgTimestamp = t1 = firstAfter resolved → match, gap = 4s
        TransformContext ctx2 = buildCtx(feed, 2, List.of(), firstAfterPattern);
        assertThat(GapEvaluator.resolveGapDuration(selNoMatch, feed.get(2), ctx2))
                .isPresent().contains(Duration.ofSeconds(4));
    }

    // -------------------------------------------------------------------------
    // within_fragment selector
    // -------------------------------------------------------------------------

    @Test
    void withinFragment_resolvedFragment_matchesGapsInsideSpan() {
        Instant t0 = Instant.parse("2024-01-01T10:00:00Z");
        Instant t1 = t0.plusSeconds(2);
        Instant t2 = t1.plusSeconds(3);
        Instant t3 = t2.plusSeconds(5);  // outside fragment

        FragmentDefinition checkout = new FragmentDefinition(
                "checkout", "Checkout Phase", "#4f8ef7",
                pattern(ORDER_CREATED, Quantifier.First.INSTANCE),
                pattern(PAYMENT_DONE,  Quantifier.First.INSTANCE),
                null);

        List<ReplayMessage> feed = List.of(
                msg(t0, "{\"type\":\"OrderCreated\"}"),
                msg(t1, "{\"type\":\"Mid\"}"),
                msg(t2, "{\"type\":\"PaymentCompleted\"}"),
                msg(t3, "{\"type\":\"After\"}")
        );

        GapSelector sel = new GapSelector(null, null, "checkout", null, null);

        // The fragment resolves once the "to" anchor (PaymentCompleted at t2) fires.
        // Build context up to index 2 so the fragment resolves, then check the gap
        // at index 2 (prev=t1, msg=t2, gap=3s) — this gap falls inside [t0, t2].
        TransformContext ctx2 = buildCtx(feed, 2, List.of(checkout));
        assertThat(GapEvaluator.resolveGapDuration(sel, feed.get(2), ctx2))
                .isPresent().contains(Duration.ofSeconds(3));

        // Index 3: outside fragment (prev=t2, msg=t3; t3 > endAt=t2)
        TransformContext ctx3 = buildCtx(feed, 3, List.of(checkout));
        assertThat(GapEvaluator.resolveGapDuration(sel, feed.get(3), ctx3)).isEmpty();
    }

    @Test
    void withinFragment_notYetResolved_returnsEmpty() {
        Instant t0 = Instant.parse("2024-01-01T10:00:00Z");
        Instant t1 = t0.plusSeconds(2);

        // Fragment where "to" anchor never fires
        FragmentDefinition frag = new FragmentDefinition(
                "pending", "Pending", "#ff0000",
                pattern(ORDER_CREATED, Quantifier.First.INSTANCE),
                pattern(PAYMENT_DONE,  Quantifier.First.INSTANCE),
                null);

        List<ReplayMessage> feed = List.of(
                msg(t0, "{\"type\":\"OrderCreated\"}"),
                msg(t1, "{\"type\":\"Unrelated\"}")
                // PaymentCompleted never arrives
        );

        TransformContext ctx = buildCtx(feed, 1, List.of(frag));
        GapSelector sel = new GapSelector(null, null, "pending", null, null);
        assertThat(GapEvaluator.resolveGapDuration(sel, feed.get(1), ctx)).isEmpty();
    }

    // -------------------------------------------------------------------------
    // min/max duration filters
    // -------------------------------------------------------------------------

    @Test
    void minDurationFilter_excludesShortGaps() {
        Instant t0 = Instant.parse("2024-01-01T10:00:00Z");
        Instant t1 = t0.plusMillis(500); // gap = 500ms < 1000ms min
        List<ReplayMessage> feed = List.of(msg(t0), msg(t1));

        // Use a broad "after" anchor that always matches (any quantifier on a predicate that always fires).
        Predicate alwaysTrue = new Predicate.Not(new Predicate.Leaf("$.topic", Predicate.Operator.EQ, "__never__"));
        MessagePattern alwaysPat = pattern(alwaysTrue, Quantifier.Any.INSTANCE);
        TransformContext ctx = buildCtx(feed, 1, List.of(), alwaysPat);
        GapSelector sel = new GapSelector(alwaysPat, null, null, 1000L, null);
        assertThat(GapEvaluator.resolveGapDuration(sel, feed.get(1), ctx)).isEmpty();
    }

    @Test
    void maxDurationFilter_excludesLongGaps() {
        Instant t0 = Instant.parse("2024-01-01T10:00:00Z");
        Instant t1 = t0.plusSeconds(10); // gap = 10s > 5s max
        List<ReplayMessage> feed = List.of(msg(t0), msg(t1));

        Predicate alwaysTrue = new Predicate.Not(new Predicate.Leaf("$.topic", Predicate.Operator.EQ, "__never__"));
        MessagePattern alwaysPat = pattern(alwaysTrue, Quantifier.Any.INSTANCE);
        TransformContext ctx = buildCtx(feed, 1, List.of(), alwaysPat);
        GapSelector sel = new GapSelector(alwaysPat, null, null, null, 5000L);
        assertThat(GapEvaluator.resolveGapDuration(sel, feed.get(1), ctx)).isEmpty();
    }

    // -------------------------------------------------------------------------
    // Fragment IfClause filtering
    // -------------------------------------------------------------------------

    @Test
    void fragmentIfClause_maxDuration_rejectsLongFragment() {
        Instant t0 = Instant.parse("2024-01-01T10:00:00Z");
        Instant t1 = t0.plusSeconds(60); // span = 60s > max 30s

        FragmentDefinition frag = new FragmentDefinition(
                "checkout", "Checkout", "#000",
                pattern(ORDER_CREATED, Quantifier.First.INSTANCE),
                pattern(PAYMENT_DONE,  Quantifier.First.INSTANCE),
                new FragmentDefinition.IfClause(null, 30_000L));

        List<ReplayMessage> feed = List.of(
                msg(t0, "{\"type\":\"OrderCreated\"}"),
                msg(t1, "{\"type\":\"PaymentCompleted\"}")
        );
        TransformContext ctx = buildCtx(feed, 1, List.of(frag));

        // Fragment should NOT resolve because duration (60s) > maxDurationMs (30s)
        assertThat(ctx.getResolvedFragments()).doesNotContainKey("checkout");
    }

    @Test
    void fragmentIfClause_minDuration_rejectsShortFragment() {
        Instant t0 = Instant.parse("2024-01-01T10:00:00Z");
        Instant t1 = t0.plusMillis(100); // span = 100ms < min 1000ms

        FragmentDefinition frag = new FragmentDefinition(
                "checkout", "Checkout", "#000",
                pattern(ORDER_CREATED, Quantifier.First.INSTANCE),
                pattern(PAYMENT_DONE,  Quantifier.First.INSTANCE),
                new FragmentDefinition.IfClause(1000L, null));

        List<ReplayMessage> feed = List.of(
                msg(t0, "{\"type\":\"OrderCreated\"}"),
                msg(t1, "{\"type\":\"PaymentCompleted\"}")
        );
        TransformContext ctx = buildCtx(feed, 1, List.of(frag));
        assertThat(ctx.getResolvedFragments()).doesNotContainKey("checkout");
    }

    @Test
    void fragment_resolves_whenIfClausePasses() {
        Instant t0 = Instant.parse("2024-01-01T10:00:00Z");
        Instant t1 = t0.plusSeconds(10); // span = 10s, within [5s, 30s]

        FragmentDefinition frag = new FragmentDefinition(
                "checkout", "Checkout", "#000",
                pattern(ORDER_CREATED, Quantifier.First.INSTANCE),
                pattern(PAYMENT_DONE,  Quantifier.First.INSTANCE),
                new FragmentDefinition.IfClause(5_000L, 30_000L));

        List<ReplayMessage> feed = List.of(
                msg(t0, "{\"type\":\"OrderCreated\"}"),
                msg(t1, "{\"type\":\"PaymentCompleted\"}")
        );
        TransformContext ctx = buildCtx(feed, 1, List.of(frag));
        assertThat(ctx.getResolvedFragments()).containsKey("checkout");
        ResolvedFragment rf = ctx.getResolvedFragments().get("checkout");
        assertThat(rf.startAt()).isEqualTo(t0);
        assertThat(rf.endAt()).isEqualTo(t1);
    }

    // -------------------------------------------------------------------------
    // Anchor never seen → empty
    // -------------------------------------------------------------------------

    @Test
    void anchorNeverSeen_returnsEmpty() {
        Instant t0 = Instant.parse("2024-01-01T10:00:00Z");
        Instant t1 = t0.plusSeconds(3);

        List<ReplayMessage> feed = List.of(
                msg(t0, "{\"type\":\"Unrelated\"}"),
                msg(t1, "{\"type\":\"Unrelated\"}")
        );
        TransformContext ctx = buildCtx(feed, 1);

        GapSelector sel = new GapSelector(
                pattern(ORDER_CREATED, Quantifier.First.INSTANCE),
                null, null, null, null);
        assertThat(GapEvaluator.resolveGapDuration(sel, feed.get(1), ctx)).isEmpty();
    }

    // -------------------------------------------------------------------------
    // before selector
    // -------------------------------------------------------------------------

    @Test
    void beforeSelector_matchesGapBeforeTargetMessage() {
        Instant t0 = Instant.parse("2024-01-01T10:00:00Z");
        Instant t1 = t0.plusSeconds(5);
        Instant t2 = t1.plusSeconds(2);

        MessagePattern beforePat = pattern(PAYMENT_SENT, Quantifier.First.INSTANCE);
        List<ReplayMessage> feed = List.of(
                msg(t0, "{\"type\":\"Unrelated\"}"),
                msg(t1, "{\"type\":\"Unrelated\"}"),
                msg(t2, "{\"type\":\"PaymentSent\"}")
        );
        TransformContext ctx = buildCtx(feed, 2, List.of(), beforePat);

        GapSelector sel = new GapSelector(null, beforePat, null, null, null);
        Optional<Duration> result = GapEvaluator.resolveGapDuration(sel, feed.get(2), ctx);
        assertThat(result).isPresent().contains(Duration.ofSeconds(2));
    }

    // -------------------------------------------------------------------------
    // Double anchor selector (after + before)
    // -------------------------------------------------------------------------

    @Test
    void doubleAnchor_afterAndBefore_combinedWithAnd() {
        Instant t0 = Instant.parse("2024-01-01T10:00:00Z");
        Instant t1 = t0.plusSeconds(3);
        Instant t2 = t1.plusSeconds(4);

        List<ReplayMessage> feed = List.of(
                msg(t0, "{\"type\":\"OrderCreated\"}"),
                msg(t1, "{\"type\":\"Mid\"}"),
                msg(t2, "{\"type\":\"PaymentSent\"}")
        );

        MessagePattern afterPat  = pattern(ORDER_CREATED, Quantifier.First.INSTANCE);
        MessagePattern beforePat = pattern(PAYMENT_SENT,  Quantifier.First.INSTANCE);
        GapSelector sel = new GapSelector(afterPat, beforePat, null, null, null);

        // At index 2 (PaymentSent), prevMsgTimestamp = t1 (Mid) ≠ t0 (OrderCreated) → no match
        TransformContext ctx = buildCtx(feed, 2, List.of(), afterPat, beforePat);
        assertThat(GapEvaluator.resolveGapDuration(sel, feed.get(2), ctx)).isEmpty();

        // At index 1 (Mid), prevMsgTimestamp = t0 (OrderCreated), but before=PaymentSent doesn't match Mid
        TransformContext ctx1 = buildCtx(feed, 1, List.of(), afterPat, beforePat);
        assertThat(GapEvaluator.resolveGapDuration(sel, feed.get(1), ctx1)).isEmpty();
    }

    // -------------------------------------------------------------------------
    // Helpers for building pre-fed TransformContext up to a given index
    // -------------------------------------------------------------------------

    private static TransformContext buildCtx(List<ReplayMessage> feed, int upToIndexInclusive) {
        return buildCtx(feed, upToIndexInclusive, List.of());
    }

    private static TransformContext buildCtx(List<ReplayMessage> feed, int upToIndexInclusive,
                                              List<FragmentDefinition> frags,
                                              MessagePattern... extraPatterns) {
        TransformContext ctx = new TransformContext();
        ctx.setFragmentDefs(frags);
        // Collect patterns to watch: from fragment from/to anchors + extras
        List<MessagePattern> watched = new ArrayList<>();
        for (FragmentDefinition frag : frags) {
            addPatternTree(frag.from(), watched);
            addPatternTree(frag.to(),   watched);
        }
        for (MessagePattern p : extraPatterns) addPatternTree(p, watched);
        ctx.setWatchedPatterns(watched);

        for (int i = 0; i <= upToIndexInclusive; i++) {
            GapEvaluator.observeMessage(feed.get(i), ctx);
            if (i < upToIndexInclusive) {
                ctx.setPrevMsgTimestamp(feed.get(i).timestamp);
            }
        }
        return ctx;
    }

    /** Adds a pattern and any nested first_after anchors to the list. */
    private static void addPatternTree(MessagePattern p, List<MessagePattern> out) {
        if (p == null) return;
        out.add(p);
        if (p.quantifier() instanceof Quantifier.FirstAfter fa) {
            addPatternTree(fa.after(), out);
        }
    }
}
