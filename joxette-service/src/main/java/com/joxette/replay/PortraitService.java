package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.v3.oas.annotations.media.Schema;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds a compact entity portrait from an ordered event sequence.
 *
 * <p>The portrait includes aggregate metadata (event count, topic breakdown,
 * first/last seen) and a preview of the 3 most recent events. When a
 * {@link StateFoldService.StateResult} is provided (i.e. the caller also
 * requested {@code output=state}), the folded state is embedded as
 * {@code currentState}.
 */
@Service
public class PortraitService {

    private static final int RECENT_EVENT_LIMIT = 3;

    /**
     * Produces a {@link PortraitResult} from the ordered event list.
     *
     * @param entityType  entity type label
     * @param entityId    entity identifier
     * @param records     ordered (ASC) entity events; may be empty
     * @param stateResult optional folded state to embed; null means omit {@code currentState}
     */
    public PortraitResult portrait(String entityType, String entityId,
                                   List<EntityRecord> records,
                                   StateFoldService.StateResult stateResult) {
        if (records.isEmpty()) {
            return new PortraitResult(entityId, entityType, 0, null, null,
                    Map.of(), List.of(),
                    stateResult != null ? stateResult.state() : null);
        }

        int count = records.size();
        Instant firstSeen = records.get(0).timestamp();
        Instant lastSeen  = records.get(count - 1).timestamp();

        // Topic breakdown: count events per source topic
        Map<String, Integer> topicBreakdown = new LinkedHashMap<>();
        for (EntityRecord r : records) {
            if (r.topic() != null) {
                topicBreakdown.merge(r.topic(), 1, Integer::sum);
            }
        }

        // Recent events: last N in reverse-chronological order (most recent first)
        int fromIdx = Math.max(0, count - RECENT_EVENT_LIMIT);
        List<EntityRecord> recentEvents = records.subList(fromIdx, count)
                .reversed();

        JsonNode currentState = stateResult != null ? stateResult.state() : null;

        return new PortraitResult(entityId, entityType, count, firstSeen, lastSeen,
                topicBreakdown, recentEvents, currentState);
    }

    // -------------------------------------------------------------------------
    // Result type
    // -------------------------------------------------------------------------

    /**
     * Compact entity summary returned when {@code response_format=portrait}.
     */
    @Schema(description = "Compact entity summary suitable for list views or dashboards.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record PortraitResult(

            @Schema(description = "Entity identifier", example = "order-789")
            String entityId,

            @Schema(description = "Entity type", example = "order")
            String entityType,

            @Schema(description = "Total number of events in the entity's history (subject to active filters)",
                    example = "12")
            int eventCount,

            @Schema(description = "Timestamp of the earliest event", example = "2025-01-15T09:00:00Z")
            Instant firstSeen,

            @Schema(description = "Timestamp of the most recent event", example = "2025-01-15T14:30:00Z")
            Instant lastSeen,

            @Schema(description = "Number of events per source topic",
                    example = "{\"orders.events\": 7, \"payments.events\": 5}")
            Map<String, Integer> topicBreakdown,

            @Schema(description = "Preview of the 3 most recent events, in reverse-chronological order " +
                                  "(most recent first)")
            List<EntityRecord> recentEvents,

            @Schema(description = "Folded current state JSON object. Only present when output=state " +
                                  "is also requested.")
            JsonNode currentState
    ) {}
}
