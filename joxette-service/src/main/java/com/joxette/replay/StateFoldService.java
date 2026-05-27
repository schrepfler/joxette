package com.joxette.replay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Folds an ordered entity event sequence into a single state object.
 *
 * <p>The fold is purely in-memory: it consumes the full event list
 * (already loaded by the SOL or pagination path) and applies the chosen
 * {@link StateFoldStrategy}.
 */
@Service
public class StateFoldService {

    private final ObjectMapper objectMapper;

    public StateFoldService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Folds {@code records} into a {@link StateResult}.
     *
     * @param records   ordered (ASC) entity events; may be empty
     * @param strategy  fold strategy; defaults to {@link StateFoldStrategy#MERGE_PATCH} when null
     */
    public StateResult fold(List<EntityRecord> records, StateFoldStrategy strategy) {
        if (strategy == null) strategy = StateFoldStrategy.MERGE_PATCH;
        if (records.isEmpty()) {
            return new StateResult(objectMapper.createObjectNode(), null, 0);
        }

        JsonNode state = switch (strategy) {
            case MERGE_PATCH    -> foldMergePatch(records);
            case LAST_VALUE     -> foldLastValue(records);
            case LAST_PER_TOPIC -> foldLastPerTopic(records);
        };

        Instant asOf = records.get(records.size() - 1).timestamp();
        return new StateResult(state, asOf, records.size());
    }

    private JsonNode foldMergePatch(List<EntityRecord> records) {
        ObjectNode acc = objectMapper.createObjectNode();
        for (EntityRecord r : records) {
            JsonNode patch = decode(r.value());
            if (patch != null && patch.isObject()) {
                mergePatch(acc, (ObjectNode) patch);
            }
        }
        return acc;
    }

    private JsonNode foldLastValue(List<EntityRecord> records) {
        for (int i = records.size() - 1; i >= 0; i--) {
            JsonNode v = decode(records.get(i).value());
            if (v != null) return v;
        }
        return objectMapper.createObjectNode();
    }

    private JsonNode foldLastPerTopic(List<EntityRecord> records) {
        // Track last value per source topic, preserving first-appearance order
        Map<String, JsonNode> lastPerTopic = new LinkedHashMap<>();
        for (EntityRecord r : records) {
            JsonNode v = decode(r.value());
            if (v != null) lastPerTopic.put(r.topic(), v);
        }
        // Merge all per-topic last values together
        ObjectNode acc = objectMapper.createObjectNode();
        for (JsonNode v : lastPerTopic.values()) {
            if (v.isObject()) mergePatch(acc, (ObjectNode) v);
        }
        return acc;
    }

    /**
     * RFC 7396 JSON Merge Patch applied in-place to {@code target}.
     * Null values in {@code patch} remove the corresponding key from {@code target}.
     */
    private static void mergePatch(ObjectNode target, ObjectNode patch) {
        patch.fields().forEachRemaining(entry -> {
            String key = entry.getKey();
            JsonNode val = entry.getValue();
            if (val.isNull()) {
                target.remove(key);
            } else if (val.isObject() && target.has(key) && target.get(key).isObject()) {
                mergePatch((ObjectNode) target.get(key), (ObjectNode) val);
            } else {
                target.set(key, val.deepCopy());
            }
        });
    }

    private JsonNode decode(String base64urlValue) {
        if (base64urlValue == null) return null;
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(base64urlValue);
            return objectMapper.readTree(bytes);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Result of a state fold operation.
     *
     * @param state       the folded state JSON object
     * @param asOf        timestamp of the last event in the fold, or null for empty sequences
     * @param eventCount  number of events that were folded
     */
    public record StateResult(JsonNode state, Instant asOf, int eventCount) {}
}
