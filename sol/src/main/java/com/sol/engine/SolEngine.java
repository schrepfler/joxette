package com.sol.engine;

import com.sol.engine.MatchEngine.MatchOccurrence;
import com.sol.engine.SolOperation.*;
import com.sol.expr.EvalContext;
import com.sol.expr.ExpressionEvaluator;
import com.sol.model.Event;
import com.sol.model.Sequence;
import com.sol.model.Tag;

import java.time.Instant;
import java.util.*;

/**
 * Executes a SOL pipeline (a list of {@link SolOperation}s) against a
 * {@link Sequence}.
 *
 * <p>Each operation is applied in order. The result of each step is the input
 * to the next. Returns a {@link SolResult} containing the final sequence,
 * active tags, match status, and any unexpected nulls.
 */
public final class SolEngine {

    private SolEngine() {}

    public static SolResult execute(List<SolOperation> operations, Sequence input) {
        State state = new State(List.of(input), Map.of(), new ArrayList<>());

        for (SolOperation op : operations) {
            state = switch (op) {
                case MatchOp m      -> applyMatch(m, state, false);
                case MatchSplitOp m -> applyMatch(new MatchOp(m.pattern(), m.condition()), state, true);
                case FilterOp f     -> applyFilter(f, state);
                case SetOp s        -> applySet(s, state);
                case ReplaceOp r    -> applyReplace(r, state);
                case CombineOp c    -> applyCombine(c, state);
            };
            // Drop sequences that were filtered to null
            state = new State(
                    state.sequences().stream().filter(Objects::nonNull).toList(),
                    state.tags(),
                    state.nulls());
        }

        // Collapse multiple sequences (after split+combine) back into one for result
        Sequence finalSeq = state.sequences().isEmpty()
                ? new Sequence(input.id(), List.of(), input.dims())
                : mergeTo(input.id(), state.sequences(), input.dims());

        boolean matched = state.tags().containsKey("MATCHED")
                && !state.tags().get("MATCHED").isEmpty();

        return new SolResult(finalSeq, state.tags(), matched, List.copyOf(state.nulls()));
    }

    // -----------------------------------------------------------------------
    // MATCH / MATCH SPLIT
    // -----------------------------------------------------------------------

    private static State applyMatch(MatchOp op, State state, boolean split) {
        List<Sequence> out   = new ArrayList<>();
        Map<String, Tag> lastTags = new LinkedHashMap<>();
        List<SolResult.UnexpectedNull> nulls = new ArrayList<>(state.nulls());

        for (Sequence seq : state.sequences()) {
            EvalContext ctx = new EvalContext(seq, new LinkedHashMap<>());

            if (!split) {
                Optional<Map<String, Tag>> match =
                        MatchEngine.findFirst(seq, op.pattern(), op.condition(), 0, ctx);
                nulls.addAll(ctx.unexpectedNulls());
                if (match.isPresent()) {
                    lastTags.putAll(match.get());
                    out.add(seq);
                } else {
                    lastTags.put("MATCHED", new Tag("MATCHED", 0, 0)); // unmatched
                    out.add(seq);
                }
            } else {
                // MATCH SPLIT: split sequence before each occurrence
                List<MatchOccurrence> occurrences = MatchEngine.findAll(seq, op.pattern(), op.condition(), ctx);
                nulls.addAll(ctx.unexpectedNulls());
                if (occurrences.isEmpty()) {
                    out.add(seq);
                } else {
                    int splitCount = occurrences.size();
                    int prev = 0;
                    for (int i = 0; i < splitCount; i++) {
                        MatchOccurrence occ = occurrences.get(i);
                        // sub-sequence up to (but not including) the match start
                        if (occ.startIndex() > prev) {
                            Sequence prefix = subseq(seq, prev, occ.startIndex(),
                                    Map.of("split_index", (long) i, "split_count", (long) splitCount));
                            out.add(prefix);
                        }
                        // the matched sub-sequence
                        Sequence matched = subseq(seq, occ.startIndex(), occ.endIndex(),
                                Map.of("split_index", (long) i, "split_count", (long) splitCount));
                        out.add(matched);
                        prev = occ.endIndex();
                    }
                    // tail after last match
                    if (prev < seq.size()) {
                        Sequence tail = subseq(seq, prev, seq.size(),
                                Map.of("split_index", (long) splitCount, "split_count", (long) splitCount));
                        out.add(tail);
                    }
                }
            }
        }
        return new State(out, lastTags, nulls);
    }

    // -----------------------------------------------------------------------
    // FILTER
    // -----------------------------------------------------------------------

    private static State applyFilter(FilterOp op, State state) {
        List<Sequence> out = new ArrayList<>();
        List<SolResult.UnexpectedNull> nulls = new ArrayList<>(state.nulls());

        for (Sequence seq : state.sequences()) {
            EvalContext ctx = new EvalContext(seq, state.tags());
            boolean keep = ExpressionEvaluator.evalBoolean(op.condition(), ctx);
            nulls.addAll(ctx.unexpectedNulls());
            if (keep) out.add(seq);
        }
        return new State(out, state.tags(), nulls);
    }

    // -----------------------------------------------------------------------
    // SET
    // -----------------------------------------------------------------------

    private static State applySet(SetOp op, State state) {
        List<SolResult.UnexpectedNull> nulls = new ArrayList<>(state.nulls());

        for (Sequence seq : state.sequences()) {
            EvalContext ctx = new EvalContext(seq, state.tags());
            Object value = ExpressionEvaluator.eval(op.expression(), ctx);
            nulls.addAll(ctx.unexpectedNulls());
            applySetTarget(op.target(), value, seq, state.tags());
        }
        return new State(state.sequences(), state.tags(), nulls);
    }

    private static void applySetTarget(String target, Object value, Sequence seq, Map<String, Tag> tags) {
        // "seq_dim" — sequence-level dimension
        if (!target.contains(".")) {
            seq.setDim(target, value);
            return;
        }
        // "Tag.dim" or "Tag[n:m].dim"
        int dot = target.lastIndexOf('.');
        String tagPart = target.substring(0, dot);
        String dimName = target.substring(dot + 1);

        // resolve tag and optional slice
        String tagName  = tagPart.replaceAll("\\[.*]", "");
        Tag tag = "SEQ".equals(tagName) ? new Tag("SEQ", 0, seq.size()) : tags.get(tagName);
        if (tag == null) return;

        int from = tag.from(), to = tag.to();
        if (tagPart.contains("[")) {
            String slice = tagPart.replaceAll(".*\\[(.*)\\]", "$1");
            int[] bounds = parseSlice(slice, tag.length());
            from = tag.from() + bounds[0];
            to   = tag.from() + bounds[1];
        }

        switch (value) {
            case List<?> list -> {
                int len = to - from;
                for (int i = 0; i < len && i < list.size(); i++)
                    seq.get(from + i).setDim(dimName, list.get(i));
            }
            default -> {
                for (int i = from; i < to; i++)
                    seq.get(i).setDim(dimName, value);
            }
        }
    }

    // -----------------------------------------------------------------------
    // REPLACE
    // -----------------------------------------------------------------------

    private static State applyReplace(ReplaceOp op, State state) {
        List<Sequence> out = new ArrayList<>();
        Map<String, Tag> newTags = new LinkedHashMap<>(state.tags());

        for (Sequence seq : state.sequences()) {
            Tag tag = state.tags().get(op.tagName());
            if (tag == null) { out.add(seq); continue; }

            List<Event> events = new ArrayList<>(seq.events());
            List<Event> replacement = buildReplacement(op, tag, seq, state.tags());

            // Replace the range [tag.from, tag.to) with the replacement events
            List<Event> newEvents = new ArrayList<>();
            newEvents.addAll(events.subList(0, tag.from()));
            newEvents.addAll(replacement);
            newEvents.addAll(events.subList(tag.to(), events.size()));

            Sequence newSeq = new Sequence(seq.id(), newEvents, seq.dims());
            // Apply dim assignments
            EvalContext ctx = new EvalContext(newSeq, newTags);
            for (var entry : op.dimAssignments().entrySet()) {
                Object v = ExpressionEvaluator.eval(entry.getValue(), ctx);
                applySetTarget(entry.getKey(), v, newSeq, newTags);
            }
            out.add(newSeq);
            // Update tag positions (simplified: only update the replaced tag)
            newTags.put(op.tagName(), new Tag(op.tagName(), tag.from(), tag.from() + replacement.size()));
        }
        return new State(out, newTags, state.nulls());
    }

    private static List<Event> buildReplacement(ReplaceOp op, Tag tag, Sequence seq, Map<String, Tag> tags) {
        if (op.replacements().isEmpty()) return List.of(); // null = remove

        List<Event> result = new ArrayList<>();
        Instant baseTs = tag.isEmpty() ? Instant.now() : seq.get(tag.from()).ts();

        for (SolOperation.ReplacementElement elem : op.replacements()) {
            switch (elem) {
                case SolOperation.ReplacementElement.NewEvent(var tagName, var eventName) ->
                        result.add(new Event(eventName, baseTs));
                case SolOperation.ReplacementElement.CopyTag(var newTagName, var srcTagName) -> {
                    Tag src = tags.get(srcTagName);
                    if (src != null) result.addAll(src.events(seq));
                }
            }
        }
        return result;
    }

    // -----------------------------------------------------------------------
    // COMBINE
    // -----------------------------------------------------------------------

    private static State applyCombine(CombineOp op, State state) {
        if (state.sequences().isEmpty()) return state;

        // Merge all sub-sequences back in order
        List<Event> merged = new ArrayList<>();
        for (Sequence s : state.sequences()) merged.addAll(s.events());

        // Carry over sequence dims that are identical across all sub-sequences
        Map<String, Object> commonDims = new LinkedHashMap<>();
        if (!state.sequences().isEmpty()) {
            commonDims.putAll(state.sequences().getFirst().dims());
            for (int i = 1; i < state.sequences().size(); i++) {
                final int fi = i;
                commonDims.entrySet().removeIf(e ->
                        !Objects.equals(state.sequences().get(fi).dim(e.getKey()), e.getValue()));
            }
        }

        Sequence combined = new Sequence(state.sequences().getFirst().id(), merged, commonDims);

        // Apply aggregations
        if (!op.aggregations().isEmpty()) {
            EvalContext ctx = new EvalContext(combined, Map.of());
            for (var entry : op.aggregations().entrySet()) {
                Object v = ExpressionEvaluator.eval(entry.getValue(), ctx);
                combined.setDim(entry.getKey(), v);
            }
        }

        return new State(List.of(combined), Map.of(), state.nulls());
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static Sequence subseq(Sequence src, int from, int to, Map<String, Object> extraDims) {
        List<Event> slice = new ArrayList<>(src.events().subList(from, to));
        Map<String, Object> dims = new LinkedHashMap<>(src.dims());
        dims.putAll(extraDims);
        return new Sequence(src.id(), slice, dims);
    }

    private static Sequence mergeTo(String id, List<Sequence> seqs, Map<String, Object> baseDims) {
        List<Event> events = new ArrayList<>();
        for (Sequence s : seqs) events.addAll(s.events());
        return new Sequence(id, events, baseDims);
    }

    private static int[] parseSlice(String slice, int length) {
        // "n", "n:m", "n:", ":m", ":"
        String[] parts = slice.split(":", -1);
        int from = 0, to = length;
        if (parts.length == 1) {
            int i = Integer.parseInt(parts[0].trim());
            from = i < 0 ? Math.max(0, length + i) : i;
            to = from + 1;
        } else {
            if (!parts[0].isBlank()) {
                int i = Integer.parseInt(parts[0].trim());
                from = i < 0 ? Math.max(0, length + i) : i;
            }
            if (!parts[1].isBlank()) {
                int i = Integer.parseInt(parts[1].trim());
                to = i < 0 ? Math.max(0, length + i) : i;
            }
        }
        return new int[]{Math.min(from, length), Math.min(to, length)};
    }

    // -----------------------------------------------------------------------
    // Internal pipeline state
    // -----------------------------------------------------------------------

    private record State(
            List<Sequence> sequences,
            Map<String, Tag> tags,
            List<SolResult.UnexpectedNull> nulls) {}
}
