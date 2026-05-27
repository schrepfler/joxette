package com.joxette.replay;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link TimelineService}.
 */
class TimelineServiceTest {

    private TimelineService service;

    @BeforeEach
    void setUp() {
        service = new TimelineService();
    }

    // -------------------------------------------------------------------------
    // Empty sequence
    // -------------------------------------------------------------------------

    @Test
    void timeline_emptySequence_returnsEmptyBuckets() {
        TimelineService.TimelineResult result =
                service.timeline("order", "ORD-1", List.of(), null);

        assertThat(result.buckets()).isEmpty();
        assertThat(result.topics()).isEmpty();
        assertThat(result.entityId()).isEqualTo("ORD-1");
        assertThat(result.entityType()).isEqualTo("order");
    }

    // -------------------------------------------------------------------------
    // Auto granularity selection
    // -------------------------------------------------------------------------

    @Test
    void timeline_spanLessThanOneHour_selectsMinuteBucket() {
        List<EntityRecord> records = List.of(
                record("t1", Instant.parse("2025-01-15T10:00:00Z"), "orders.events"),
                record("t2", Instant.parse("2025-01-15T10:30:00Z"), "orders.events"));

        TimelineService.TimelineResult result =
                service.timeline("order", "ORD-1", records, null);

        assertThat(result.granularity()).isEqualTo(TimelineBucket.MINUTE);
    }

    @Test
    void timeline_spanBetweenOneHourAndSevenDays_selectsHourBucket() {
        List<EntityRecord> records = List.of(
                record("t1", Instant.parse("2025-01-15T10:00:00Z"), "orders.events"),
                record("t2", Instant.parse("2025-01-16T10:00:00Z"), "orders.events"));

        TimelineService.TimelineResult result =
                service.timeline("order", "ORD-1", records, null);

        assertThat(result.granularity()).isEqualTo(TimelineBucket.HOUR);
    }

    @Test
    void timeline_spanSevenDaysOrMore_selectsDayBucket() {
        List<EntityRecord> records = List.of(
                record("t1", Instant.parse("2025-01-01T10:00:00Z"), "orders.events"),
                record("t2", Instant.parse("2025-01-15T10:00:00Z"), "orders.events"));

        TimelineService.TimelineResult result =
                service.timeline("order", "ORD-1", records, null);

        assertThat(result.granularity()).isEqualTo(TimelineBucket.DAY);
    }

    // -------------------------------------------------------------------------
    // Manual granularity override
    // -------------------------------------------------------------------------

    @Test
    void timeline_manualBucketOverride_usesRequestedGranularity() {
        List<EntityRecord> records = List.of(
                record("t1", Instant.parse("2025-01-15T10:00:00Z"), "orders.events"),
                record("t2", Instant.parse("2025-01-15T10:30:00Z"), "orders.events"));

        TimelineService.TimelineResult result =
                service.timeline("order", "ORD-1", records, TimelineBucket.DAY);

        assertThat(result.granularity()).isEqualTo(TimelineBucket.DAY);
        // Both events are in the same day bucket
        assertThat(result.buckets()).hasSize(1);
    }

    // -------------------------------------------------------------------------
    // Bucketing
    // -------------------------------------------------------------------------

    @Test
    void timeline_eventsInSameBucket_groupedTogether() {
        List<EntityRecord> records = List.of(
                record("t1", Instant.parse("2025-01-15T10:05:00Z"), "orders.events"),
                record("t2", Instant.parse("2025-01-15T10:45:00Z"), "orders.events"),
                record("t3", Instant.parse("2025-01-15T11:15:00Z"), "orders.events"));

        TimelineService.TimelineResult result =
                service.timeline("order", "ORD-1", records, TimelineBucket.HOUR);

        assertThat(result.buckets()).hasSize(2);
        assertThat(result.buckets().get(0).bucketStart())
                .isEqualTo(Instant.parse("2025-01-15T10:00:00Z"));
        assertThat(result.buckets().get(0).events()).hasSize(2);
        assertThat(result.buckets().get(1).bucketStart())
                .isEqualTo(Instant.parse("2025-01-15T11:00:00Z"));
        assertThat(result.buckets().get(1).events()).hasSize(1);
    }

    @Test
    void timeline_eachEventInOwnBucket_oneEventPerBucket() {
        List<EntityRecord> records = List.of(
                record("t1", Instant.parse("2025-01-01T00:00:00Z"), "orders.events"),
                record("t2", Instant.parse("2025-01-02T00:00:00Z"), "orders.events"),
                record("t3", Instant.parse("2025-01-03T00:00:00Z"), "orders.events"));

        TimelineService.TimelineResult result =
                service.timeline("order", "ORD-1", records, TimelineBucket.DAY);

        assertThat(result.buckets()).hasSize(3);
        result.buckets().forEach(b -> assertThat(b.events()).hasSize(1));
    }

    // -------------------------------------------------------------------------
    // Topics
    // -------------------------------------------------------------------------

    @Test
    void timeline_multipleTopics_collectedAndSorted() {
        List<EntityRecord> records = List.of(
                record("t1", Instant.parse("2025-01-15T10:00:00Z"), "payments.events"),
                record("t2", Instant.parse("2025-01-15T10:05:00Z"), "orders.events"));

        TimelineService.TimelineResult result =
                service.timeline("order", "ORD-1", records, TimelineBucket.HOUR);

        assertThat(result.topics()).containsExactly("orders.events", "payments.events");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private EntityRecord record(String messageType, Instant timestamp, String topic) {
        return new EntityRecord("ORD-1", messageType, topic, 0, 0L,
                timestamp, timestamp, null, null, null);
    }
}
