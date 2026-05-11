package com.joxette.sol;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.replay.EntityRecord;
import com.sol.model.Event;
import com.sol.model.Sequence;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts a list of {@link EntityRecord}s (all for the same entity) into a
 * {@link Sequence} that the SOL engine can process.
 *
 * <p>Mapping:
 * <ul>
 *   <li>Event name  → {@code messageType} if non-null, otherwise {@code topic}</li>
 *   <li>Event ts    → {@code timestamp} (Kafka producer timestamp)</li>
 *   <li>Dims        → all scalar fields of the record; JSON {@code value} is
 *       decoded and its top-level fields are added as individual dimensions</li>
 * </ul>
 */
public final class EntityRecordAdapter {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private EntityRecordAdapter() {}

    public static Sequence toSequence(String entityId, List<EntityRecord> records) {
        List<Event> events = records.stream().map(EntityRecordAdapter::toEvent).toList();
        Map<String, Object> seqDims = Map.of("entity_id", entityId);
        return new Sequence(entityId, events, seqDims);
    }

    static Event toEvent(EntityRecord r) {
        Map<String, Object> dims = new HashMap<>();
        dims.put("topic",       r.topic());
        dims.put("partition",   r.partition());
        dims.put("offset",      r.offset());
        dims.put("recorded_at", r.recordedAt());
        dims.put("key",         r.key());
        dims.put("entity_id",   r.entityId());
        if (r.messageType() != null) dims.put("message_type", r.messageType());

        // Decode JSON value and add its top-level fields as individual dimensions
        if (r.value() != null) {
            try {
                byte[] bytes = Base64.getUrlDecoder().decode(r.value());
                Map<String, Object> valueMap = MAPPER.readValue(bytes, MAP_TYPE);
                dims.putAll(valueMap);
            } catch (Exception ignored) {
                dims.put("value_raw", r.value());
            }
        }

        String eventName = r.messageType() != null ? r.messageType() : r.topic();
        return new Event(eventName, r.timestamp(), dims);
    }
}
