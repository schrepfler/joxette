package com.sol.engine;

import com.sol.model.Event;
import com.sol.model.Sequence;
import com.sol.model.Tag;

import java.util.List;
import java.util.Map;

/**
 * The output of running a SOL pipeline against one {@link Sequence}.
 *
 * @param sequence        the transformed sequence (events + dims after all operations)
 * @param tags            tags that remain after the final operation
 * @param matched         true if the last MATCH / MATCH SPLIT found at least one match
 * @param unexpectedNulls null-cast events recorded during evaluation ("show must go on")
 */
public record SolResult(
        Sequence sequence,
        Map<String, Tag> tags,
        boolean matched,
        List<UnexpectedNull> unexpectedNulls
) {

    /** Returns the events of the named tag, or an empty list if the tag does not exist. */
    public List<Event> tagEvents(String tagName) {
        Tag t = tags.get(tagName);
        return t == null ? List.of() : t.events(sequence);
    }

    // -----------------------------------------------------------------------

    /**
     * Records one null-cast incident ("show must go on" error model).
     *
     * @param location   human-readable location in the query (e.g. "if condition")
     * @param reason     why the expression was null-cast
     */
    public record UnexpectedNull(String location, String reason) {}
}
