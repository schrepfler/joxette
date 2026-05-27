package com.joxette.replay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Annotates an ordered entity event sequence with per-event change metadata.
 *
 * <p>For each event this service tracks the running accumulated state (RFC 7396
 * merge-patch semantics — the same strategy used by {@link StateFoldService} for
 * {@code MERGE_PATCH}) and computes:
 * <ul>
 *   <li>{@code changedFields} — JSON-Pointer paths of the top-level keys that
 *       differ between the prior state and the state after this event.</li>
 *   <li>{@code before} — the values of those keys in the prior state.</li>
 * </ul>
 *
 * <p>The first event always has null {@code changedFields} and null {@code before}
 * (there is no prior state to compare against). Events whose decoded value is not
 * a JSON object still appear in the output — they receive null diff fields and do
 * not advance the accumulated state.
 */
@Service
public class DiffService {

    private final ObjectMapper objectMapper;

    public DiffService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Annotates each record in {@code records} with change metadata relative to
     * the accumulated state up to that point.
     *
     * @param records ordered (ASC) entity events; may be empty
     * @return a {@link DiffRecord} for every input record, in the same order
     */
    public List<DiffRecord> diff(List<EntityRecord> records) {
        List<DiffRecord> result = new ArrayList<>(records.size());
        ObjectNode state = objectMapper.createObjectNode();
        boolean first = true;

        for (EntityRecord record : records) {
            JsonNode patch = decode(record.value());

            if (patch == null || !patch.isObject()) {
                // Non-parseable value: include event, no diff, state unchanged
                result.add(new DiffRecord(record, null, null));
                continue;
            }

            ObjectNode patchObj = (ObjectNode) patch;

            if (first) {
                // First parseable event — no prior state, apply unconditionally
                applyMergePatch(state, patchObj);
                result.add(new DiffRecord(record, null, null));
                first = false;
                continue;
            }

            // Compute which top-level keys this patch changes relative to current state
            List<String> changedFields = new ArrayList<>();
            ObjectNode before = objectMapper.createObjectNode();

            Iterator<Map.Entry<String, JsonNode>> it = patchObj.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> entry = it.next();
                String key = entry.getKey();
                JsonNode newVal = entry.getValue();
                JsonNode oldVal = state.get(key);

                boolean removing = newVal.isNull();
                boolean adding   = (oldVal == null);
                boolean changing = !removing && !adding && !newVal.equals(oldVal);

                if (removing && state.has(key)) {
                    changedFields.add("/" + key);
                    before.set(key, oldVal.deepCopy());
                } else if (adding && !newVal.isNull()) {
                    changedFields.add("/" + key);
                    // No "before" value for a brand-new field — omit from before node
                } else if (changing) {
                    changedFields.add("/" + key);
                    before.set(key, oldVal.deepCopy());
                }
            }

            applyMergePatch(state, patchObj);

            result.add(new DiffRecord(
                    record,
                    changedFields.isEmpty() ? null : changedFields,
                    before.isEmpty() ? null : before));
        }
        return result;
    }

    /**
     * RFC 7396 merge-patch applied in-place to {@code target}.
     * Null values in {@code patch} remove the corresponding key.
     */
    private static void applyMergePatch(ObjectNode target, ObjectNode patch) {
        patch.fields().forEachRemaining(entry -> {
            String key = entry.getKey();
            JsonNode val = entry.getValue();
            if (val.isNull()) {
                target.remove(key);
            } else if (val.isObject() && target.has(key) && target.get(key).isObject()) {
                applyMergePatch((ObjectNode) target.get(key), (ObjectNode) val);
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
}
