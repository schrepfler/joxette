package com.joxette.sol;

import com.joxette.replay.EntityRecord;
import com.sol.engine.SolResult;
import com.sol.model.Event;
import com.sol.model.Tag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
        return map(result, originalRecords).records();
    }

    /**
     * The surviving records plus the result's tags re-expressed as spans over those
     * records.
     *
     * <p>The engine's tag spans index its final sequence, which may hold synthetic events
     * (from REPLACE) that have no record and are therefore absent from {@code records}.
     * Consumers draw tags over {@code records} by index, so each span is translated by
     * counting the surviving events before its bounds; synthetic events inside a span
     * simply drop out of it.
     */
    public record Mapped(List<EntityRecord> records, Map<String, Tag> tags) {}

    public static Mapped map(SolResult result, List<EntityRecord> originalRecords) {
        Map<RecordId, EntityRecord> byId = new HashMap<>(originalRecords.size() * 2);
        for (EntityRecord r : originalRecords) byId.putIfAbsent(new RecordId(r.topic(), r.partition(), r.offset()), r);

        List<Event> events = result.sequence().events();
        List<EntityRecord> out = new ArrayList<>(events.size());
        // survivorsBefore[i] = number of records emitted for events [0, i)
        int[] survivorsBefore = new int[events.size() + 1];
        for (int i = 0; i < events.size(); i++) {
            Event e = events.get(i);
            EntityRecord r = byId.get(new RecordId(e.dim("topic"), toInt(e.dim("partition")), toLong(e.dim("offset"))));
            if (r != null) out.add(r);
            survivorsBefore[i + 1] = out.size();
        }

        Map<String, Tag> tags = new LinkedHashMap<>();
        for (Map.Entry<String, Tag> entry : result.tags().entrySet()) {
            Tag t = entry.getValue();
            int from = survivorsBefore[Math.clamp(t.from(), 0, events.size())];
            int to = survivorsBefore[Math.clamp(t.to(), 0, events.size())];
            tags.put(entry.getKey(), new Tag(t.name(), from, Math.max(from, to)));
        }
        return new Mapped(out, tags);
    }

    private record RecordId(Object topic, int partition, long offset) {}

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

    private static int toInt(Object v) {
        return switch (v) {
            case Number n -> n.intValue();
            case null, default -> 0;
        };
    }

    private static long toLong(Object v) {
        return switch (v) {
            case Number n -> n.longValue();
            case null, default -> 0L;
        };
    }
}
