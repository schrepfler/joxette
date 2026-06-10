package com.sol.engine;

import com.sol.engine.SolOperation.Anchor;
import com.sol.engine.SolOperation.PatternElement;
import com.sol.engine.SolOperation.Quantifier;
import com.sol.expr.EvalContext;
import com.sol.expr.Expr;
import com.sol.expr.ExpressionEvaluator;
import com.sol.model.Event;
import com.sol.model.Sequence;
import com.sol.model.Tag;

import java.util.*;

/**
 * Left-to-right matching algorithm for SOL MATCH / MATCH SPLIT.
 *
 * <p>Algorithm summary:
 * <ol>
 *   <li>Walk events left-to-right.</li>
 *   <li>For each event, try to advance the current pattern element.</li>
 *   <li>Middle tags are lazy (prefer moving to the next element early);
 *       edge tags are greedy (prefer extending).</li>
 *   <li>Backtrack on mismatch.</li>
 *   <li>After the pattern is exhausted, evaluate the optional IF condition.
 *       If it fails, backtrack and try the next position.</li>
 * </ol>
 */
final class MatchEngine {

    private MatchEngine() {}

    /**
     * Finds the first match of {@code pattern} starting at or after {@code startIndex}.
     *
     * @return a map of tag name → Tag, or empty if no match found
     */
    static Optional<Map<String, Tag>> findFirst(
            Sequence seq,
            List<PatternElement> pattern,
            Expr condition,
            int startIndex,
            EvalContext ctx) {

        int n = seq.size();
        // Try every possible start position
        for (int anchor = startIndex; anchor <= n; anchor++) {
            Optional<Map<String, Tag>> result = tryMatch(seq, pattern, condition, anchor, ctx);
            if (result.isPresent()) return result;
        }
        return Optional.empty();
    }

    /**
     * Finds all non-overlapping matches, returning each as a tag-map + the
     * exclusive end index of the match (start of next search).
     */
    static List<MatchOccurrence> findAll(
            Sequence seq,
            List<PatternElement> pattern,
            Expr condition,
            EvalContext ctx) {

        List<MatchOccurrence> results = new ArrayList<>();
        int searchFrom = 0;
        int n = seq.size();

        while (searchFrom <= n) {
            boolean found = false;
            for (int anchor = searchFrom; anchor <= n; anchor++) {
                Optional<Map<String, Tag>> result = tryMatch(seq, pattern, condition, anchor, ctx);
                if (result.isPresent()) {
                    Map<String, Tag> tags = result.get();
                    int end = matchedEnd(tags);
                    results.add(new MatchOccurrence(anchor, end, tags));
                    searchFrom = Math.max(anchor + 1, end); // no overlaps
                    found = true;
                    break;
                }
            }
            if (!found) break;
        }
        return results;
    }

    // -----------------------------------------------------------------------

    record MatchOccurrence(int startIndex, int endIndex, Map<String, Tag> tags) {}

    // -----------------------------------------------------------------------
    // Core recursive matching
    // -----------------------------------------------------------------------

    /**
     * Attempts a match rooted at {@code anchor} (index of first consumed event).
     * Returns populated tag map on success, empty on failure.
     */
    private static Optional<Map<String, Tag>> tryMatch(
            Sequence seq,
            List<PatternElement> pattern,
            Expr condition,
            int anchor,
            EvalContext ctx) {

        // Filter to non-anchor elements only (START/END anchors are position checks)
        List<PatternElement> elems = pattern.stream()
                .filter(e -> e.anchor() == null)
                .toList();

        boolean needStart = pattern.stream().anyMatch(e -> e.anchor() == Anchor.START);
        boolean needEnd   = pattern.stream().anyMatch(e -> e.anchor() == Anchor.END);

        if (needStart && anchor != 0) return Optional.empty();

        Map<String, Tag> tags = new LinkedHashMap<>();
        int pos = matchElements(seq, elems, 0, anchor, tags, ctx);

        if (pos < 0) return Optional.empty();
        if (needEnd && pos != seq.size()) return Optional.empty();

        // Evaluate IF condition with the candidate tags in context
        if (condition != null) {
            Map<String, Tag> combined = buildCombinedTags(seq, tags, anchor, pos);
            EvalContext condCtx = new EvalContext(seq, combined);
            Object condResult = ExpressionEvaluator.eval(condition, condCtx);
            if (!Boolean.TRUE.equals(condResult)) return Optional.empty();
            // propagate unexpected nulls
            condCtx.unexpectedNulls().forEach(u -> ctx.recordNull(u.location(), u.reason()));
        }

        return Optional.of(buildCombinedTags(seq, tags, anchor, pos));
    }

    /**
     * Recursive element matcher. Returns the position after the last consumed event,
     * or -1 on failure.
     */
    private static int matchElements(
            Sequence seq,
            List<PatternElement> elems,
            int elemIdx,
            int pos,
            Map<String, Tag> tags,
            EvalContext ctx) {

        if (elemIdx == elems.size()) return pos; // all elements consumed

        PatternElement elem = elems.get(elemIdx);
        Quantifier q = elem.quantifier();
        int n = seq.size();
        boolean isEdge = (elemIdx == 0 || elemIdx == elems.size() - 1);

        // Try each count from min..max (greedy for edge, lazy for middle)
        int lo = q.min();
        int hi = q.isUnbounded() ? n - pos : Math.min(q.max(), n - pos);

        if (lo > hi + 1) return -1; // can't satisfy minimum

        if (isEdge) {
            // greedy: try from hi down to lo
            for (int count = hi; count >= lo; count--) {
                if (!canConsume(seq, elem, pos, count)) continue;
                Map<String, Tag> attempt = new LinkedHashMap<>(tags);
                addTag(attempt, elem, pos, count);
                int next = matchElements(seq, elems, elemIdx + 1, pos + count, attempt, ctx);
                if (next >= 0) { tags.putAll(attempt); return next; }
            }
        } else {
            // lazy: try from lo up to hi
            for (int count = lo; count <= hi + (q.isOptional() ? 1 : 0); count++) {
                if (count > 0 && !canConsume(seq, elem, pos, count)) break;
                if (count == 0 && lo > 0) continue;
                Map<String, Tag> attempt = new LinkedHashMap<>(tags);
                addTag(attempt, elem, pos, count);
                int next = matchElements(seq, elems, elemIdx + 1, pos + count, attempt, ctx);
                if (next >= 0) { tags.putAll(attempt); return next; }
            }
        }
        return -1;
    }

    private static boolean canConsume(Sequence seq, PatternElement elem, int from, int count) {
        if (elem.isWildcard()) return true;
        for (int i = from; i < from + count; i++) {
            if (i >= seq.size()) return false;
            if (!matches(seq.get(i), elem)) return false;
        }
        return true;
    }

    private static boolean matches(Event event, PatternElement elem) {
        if (elem.isWildcard()) return true;
        String name = event.name();
        if (!elem.excluded().isEmpty() && elem.excluded().contains(name)) return false;
        return elem.eventNames().isEmpty() || elem.eventNames().contains(name);
    }

    private static void addTag(Map<String, Tag> tags, PatternElement elem, int from, int count) {
        if (elem.tagName() != null) {
            tags.put(elem.tagName(), new Tag(elem.tagName(), from, from + count));
        } else if (count > 0 && elem.eventNames().size() == 1 && elem.excluded().isEmpty()) {
            // Bare single-event element (`match home_page >> …`) — emit an implicit
            // tag named after the event, so untagged patterns still surface spans
            // (Motif-style: the bare name IS the tag). Wildcards and multi-event
            // alternations stay untagged.
            String name = elem.eventNames().get(0);
            tags.put(name, new Tag(name, from, from + count));
        }
    }

    private static int matchedEnd(Map<String, Tag> tags) {
        // Use MATCHED tag only — SEQ/PREFIX/SUFFIX span the whole sequence and
        // would always advance searchFrom to seq.size(), preventing further matches.
        Tag m = tags.get("MATCHED");
        return m != null ? m.to() : 0;
    }

    private static Map<String, Tag> buildCombinedTags(Sequence seq, Map<String, Tag> named, int from, int to) {
        Map<String, Tag> all = new LinkedHashMap<>(named);
        // implicit tags
        all.put("SEQ",     new Tag("SEQ",     0,    seq.size()));
        all.put("MATCHED", new Tag("MATCHED", from, to));
        if (from > 0)         all.put("PREFIX", new Tag("PREFIX", 0, from));
        if (to < seq.size())  all.put("SUFFIX", new Tag("SUFFIX", to, seq.size()));
        return all;
    }
}
