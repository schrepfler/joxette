package com.sol.expr;

import com.sol.engine.SolResult.UnexpectedNull;
import com.sol.model.Event;
import com.sol.model.Sequence;
import com.sol.model.Tag;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Holds the state available to {@link ExpressionEvaluator} during expression
 * evaluation for one sequence evaluation step.
 */
public final class EvalContext {

    private final Sequence sequence;
    private final Map<String, Tag> tags;
    private final List<UnexpectedNull> nulls;

    public EvalContext(Sequence sequence, Map<String, Tag> tags) {
        this.sequence = sequence;
        this.tags     = tags;
        this.nulls    = new ArrayList<>();
    }

    public Sequence sequence()       { return sequence; }
    public Map<String, Tag> tags()   { return tags; }

    public Tag tag(String name) { return tags.get(name); }

    /** Records an unexpected null and returns null for chaining. */
    public Object recordNull(String location, String reason) {
        nulls.add(new UnexpectedNull(location, reason));
        return null;
    }

    public List<UnexpectedNull> unexpectedNulls() { return List.copyOf(nulls); }
}
