package com.sol.engine;

import com.sol.expr.Expr;

import java.util.List;
import java.util.Map;

/**
 * One step in a SOL pipeline. Operations are applied left-to-right over a
 * {@link com.sol.model.Sequence}.
 */
public sealed interface SolOperation
        permits SolOperation.MatchOp,
                SolOperation.MatchSplitOp,
                SolOperation.FilterOp,
                SolOperation.SetOp,
                SolOperation.ReplaceOp,
                SolOperation.CombineOp {

    /**
     * Matches the first occurrence of a pattern and labels sub-sequences with tags.
     *
     * @param pattern   ordered list of pattern elements
     * @param condition optional IF condition (null = no condition)
     */
    record MatchOp(List<PatternElement> pattern, Expr condition) implements SolOperation {}

    /**
     * Like {@link MatchOp} but finds all non-overlapping occurrences and splits
     * the sequence before each match.
     */
    record MatchSplitOp(List<PatternElement> pattern, Expr condition) implements SolOperation {}

    /**
     * Retains only sequences for which the condition evaluates to {@code true}.
     * Use {@code tag("MATCHED")} or {@code tag("not MATCHED")} as special short-hands.
     */
    record FilterOp(Expr condition) implements SolOperation {}

    /**
     * Creates or updates dimensions on events or on the sequence itself.
     *
     * @param target     target path — e.g. {@code "seq_dim"}, {@code "Tag.dim"}, {@code "Tag[1:3].dim"}
     * @param expression value expression
     */
    record SetOp(String target, Expr expression) implements SolOperation {}

    /**
     * Replaces one tag's events with a replacement sub-sequence.
     *
     * @param tagName          tag to replace
     * @param replacements     ordered replacement elements (empty = {@code null} = remove)
     * @param dimAssignments   map of {@code newTag.dim = expr} assignments applied after replacement
     */
    record ReplaceOp(String tagName,
                     List<ReplacementElement> replacements,
                     Map<String, Expr> dimAssignments) implements SolOperation {}

    /**
     * Merges sub-sequences created by a previous MATCH SPLIT back into one sequence.
     *
     * @param aggregations  optional sequence-dim aggregations computed during merge
     */
    record CombineOp(Map<String, Expr> aggregations) implements SolOperation {}

    // -----------------------------------------------------------------------
    // Pattern elements used by MatchOp / MatchSplitOp
    // -----------------------------------------------------------------------

    /**
     * One element of a match pattern.
     *
     * @param tagName       label for this element (null = anonymous)
     * @param eventNames    set of accepted event names; empty = wildcard (*)
     * @param excluded      event names that must NOT match (^event)
     * @param quantifier    repetition quantifier
     * @param anchor        START or END anchor, null = none
     */
    record PatternElement(
            String tagName,
            List<String> eventNames,
            List<String> excluded,
            Quantifier quantifier,
            Anchor anchor
    ) {
        public boolean isWildcard()  { return eventNames.isEmpty() && excluded.isEmpty(); }
        public boolean isStartAnchor() { return anchor == Anchor.START; }
        public boolean isEndAnchor()   { return anchor == Anchor.END; }
    }

    record Quantifier(int min, int max) {
        /** {@code max == -1} means unbounded. */
        public static final Quantifier ONE       = new Quantifier(1, 1);
        public static final Quantifier ZERO_ONE  = new Quantifier(0, 1);
        public static final Quantifier ZERO_MORE = new Quantifier(0, -1);
        public static final Quantifier ONE_MORE  = new Quantifier(1, -1);

        public boolean isOptional()   { return min == 0; }
        public boolean isUnbounded()  { return max == -1; }
        public boolean allows(int n)  { return n >= min && (max == -1 || n <= max); }
    }

    enum Anchor { START, END }

    // -----------------------------------------------------------------------
    // Replacement elements used by ReplaceOp
    // -----------------------------------------------------------------------

    sealed interface ReplacementElement
            permits ReplacementElement.NewEvent, ReplacementElement.CopyTag {

        /** Insert a brand-new synthetic event. */
        record NewEvent(String tagName, String eventName) implements ReplacementElement {}

        /** Copy events from an existing tag (possibly renamed). */
        record CopyTag(String newTagName, String sourceTagName) implements ReplacementElement {}
    }
}
