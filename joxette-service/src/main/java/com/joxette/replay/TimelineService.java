package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Builds a time-bucketed timeline view from an ordered entity event sequence.
 *
 * <p>Granularity is auto-selected when {@code bucket} is null:
 * <ul>
 *   <li>{@link TimelineBucket#MINUTE} — total span &lt; 1 hour</li>
 *   <li>{@link TimelineBucket#HOUR}   — 1 hour ≤ span &lt; 7 days</li>
 *   <li>{@link TimelineBucket#DAY}    — span ≥ 7 days</li>
 * </ul>
 */
@Service
public class TimelineService {

    private static final Duration ONE_HOUR  = Duration.ofHours(1);
    private static final Duration SEVEN_DAYS = Duration.ofDays(7);

    /**
     * Groups {@code records} into time buckets and returns a {@link TimelineResult}.
     *
     * @param entityType  entity type label
     * @param entityId    entity identifier
     * @param records     ordered (ASC) entity events; may be empty
     * @param bucket      requested granularity, or null for auto-selection
     */
    public TimelineResult timeline(String entityType, String entityId,
                                   List<EntityRecord> records, TimelineBucket bucket) {
        if (records.isEmpty()) {
            TimelineBucket effective = bucket != null ? bucket : TimelineBucket.HOUR;
            return new TimelineResult(entityId, entityType, effective, List.of(), List.of());
        }

        TimelineBucket effective = bucket != null ? bucket : selectGranularity(records);

        // Collect the distinct source topics seen
        TreeSet<String> topics = new TreeSet<>();
        for (EntityRecord r : records) {
            if (r.topic() != null) topics.add(r.topic());
        }

        // Group events into buckets using an insertion-ordered map (keys are bucket start instants)
        Map<Instant, List<EntityRecord>> groups = new LinkedHashMap<>();
        for (EntityRecord r : records) {
            Instant bucketStart = truncate(r.timestamp(), effective);
            groups.computeIfAbsent(bucketStart, k -> new ArrayList<>()).add(r);
        }

        List<TimeBucket> buckets = new ArrayList<>(groups.size());
        for (Map.Entry<Instant, List<EntityRecord>> entry : groups.entrySet()) {
            buckets.add(new TimeBucket(entry.getKey(), effective, entry.getValue()));
        }

        return new TimelineResult(entityId, entityType, effective, buckets, new ArrayList<>(topics));
    }

    // -------------------------------------------------------------------------
    // Granularity selection
    // -------------------------------------------------------------------------

    private static TimelineBucket selectGranularity(List<EntityRecord> records) {
        Instant first = records.get(0).timestamp();
        Instant last  = records.get(records.size() - 1).timestamp();
        if (first == null || last == null) return TimelineBucket.HOUR;
        Duration span = Duration.between(first, last).abs();
        if (span.compareTo(ONE_HOUR) < 0) return TimelineBucket.MINUTE;
        if (span.compareTo(SEVEN_DAYS) < 0) return TimelineBucket.HOUR;
        return TimelineBucket.DAY;
    }

    private static Instant truncate(Instant ts, TimelineBucket bucket) {
        if (ts == null) return Instant.EPOCH;
        ZonedDateTime zdt = ts.atZone(ZoneOffset.UTC);
        return switch (bucket) {
            case MINUTE -> zdt.truncatedTo(ChronoUnit.MINUTES).toInstant();
            case HOUR   -> zdt.truncatedTo(ChronoUnit.HOURS).toInstant();
            case DAY    -> zdt.truncatedTo(ChronoUnit.DAYS).toInstant();
        };
    }

    // -------------------------------------------------------------------------
    // Result types
    // -------------------------------------------------------------------------

    /**
     * Full timeline response.
     */
    @Schema(description = "Time-bucketed view of an entity's event history.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record TimelineResult(

            @Schema(description = "Entity identifier", example = "order-789")
            String entityId,

            @Schema(description = "Entity type", example = "order")
            String entityType,

            @Schema(description = "Bucket granularity that was applied (auto-selected or user-requested)",
                    example = "HOUR")
            TimelineBucket granularity,

            @Schema(description = "Ordered list of time buckets (chronological)")
            List<TimeBucket> buckets,

            @Schema(description = "Distinct source topics seen across all events")
            List<String> topics
    ) {}

    /**
     * One time bucket containing the events that fall within it.
     */
    @Schema(description = "A single time bucket containing events that share the same truncated timestamp.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record TimeBucket(

            @Schema(description = "Start of this bucket (truncated to the granularity, UTC)",
                    example = "2025-01-15T10:00:00Z")
            Instant bucketStart,

            @Schema(description = "Granularity of this bucket", example = "HOUR")
            TimelineBucket granularity,

            @Schema(description = "Events that fall within this bucket, in chronological order")
            List<EntityRecord> events
    ) {}
}
