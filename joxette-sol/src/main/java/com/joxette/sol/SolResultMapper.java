package com.joxette.sol;

import com.joxette.replay.EntityRecord;
import com.sol.engine.SolResult;
import com.sol.model.Event;
import com.sol.model.Tag;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Maps a {@link SolResult} back to a list of {@link EntityRecord}s.
 *
 * <p>Only events that survive the SOL pipeline are returned. Each returned
 * record has a {@code sol_tags} extra field injected into its value JSON
 * listing the tag names the event belongs to (informational).
 *
 * <p>The original {@link EntityRecord} is preserved intact; enrichment is
 * done at a higher layer (the service) if needed.
 */
public final class SolResultMapper {

    private SolResultMapper() {}

    /**
     * Returns the subset of {@code originalRecords} whose positions correspond
     * to events in the final SOL sequence, in sequence order.
     *
     * <p>Because SET / REPLACE operations may add synthetic events or reorder
     * events, only events that still have a matching original record (by
     * {@code entity_id + topic + partition + offset}) are included.
     */
    public static List<EntityRecord> toEntityRecords(SolResult result, List<EntityRecord> originalRecords) {
        List<EntityRecord> out = new ArrayList<>();
        for (Event event : result.sequence().events()) {
            findOriginal(event, originalRecords).ifPresent(out::add);
        }
        return out;
    }

    /**
     * Returns the tag names (from the final tag map) that cover the given
     * event index. Useful for annotating matched events in the API response.
     */
    public static List<String> tagsForIndex(int seqIndex, Map<String, Tag> tags) {
        return tags.entrySet().stream()
                .filter(e -> seqIndex >= e.getValue().from() && seqIndex < e.getValue().to())
                .map(Map.Entry::getKey)
                .sorted()
                .toList();
    }

    private static java.util.Optional<EntityRecord> findOriginal(Event event, List<EntityRecord> records) {
        // Match by the stable identity dimensions that EntityRecordAdapter copies into dims
        Object topic = event.dim("topic");
        Object partition = event.dim("partition");
        Object offset = event.dim("offset");

        return records.stream().filter(r ->
                r.topic().equals(topic)
                && r.partition() == toInt(partition)
                && r.offset() == toLong(offset)
        ).findFirst();
    }

    private static int toInt(Object v) {
        return switch (v) {
            case Number n -> n.intValue();
            default       -> 0;
        };
    }

    private static long toLong(Object v) {
        return switch (v) {
            case Number n -> n.longValue();
            default       -> 0L;
        };
    }
}
