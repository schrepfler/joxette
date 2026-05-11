package com.joxette.replay;

import com.joxette.replay.transform.TransformPipeline;
import com.joxette.support.DuckDBTestSupport;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for {@link EntityReplayService} against an in-memory DuckDB.
 *
 * <p>Tests cover:
 * <ul>
 *   <li>Entity event pagination and cursor navigation</li>
 *   <li>Cross-topic ordering (events from multiple source topics)</li>
 *   <li>Bucket queries and entity stats</li>
 *   <li>Known-entity listing and search with cursors</li>
 *   <li>Deduplication (same source offset recorded twice)</li>
 *   <li>Entity type validation guard</li>
 * </ul>
 */
class EntityReplayServiceTest {

    private static final String ENTITY_TYPE = "order";

    private Connection duckDB;
    private EntityReplayService service;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createEntityTable(duckDB, ENTITY_TYPE);
        service = new EntityReplayService(DSL.using(duckDB, SQLDialect.DUCKDB));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
    }

    // -------------------------------------------------------------------------
    // Basic entity event query
    // -------------------------------------------------------------------------

    @Test
    void queryEntityEvents_returnsEventsForEntityId() throws Exception {
        Instant ts = Instant.parse("2024-05-01T10:00:00Z");
        insertEntityRow("ORD-1", 42, "orders.events", 0, 0L, ts, b("{\"order_id\":\"ORD-1\"}"));
        insertEntityRow("ORD-2", 43, "orders.events", 0, 1L, ts.plusSeconds(1), b("{}"));

        PagedResponse<EntityRecord> page =
                service.queryEntityEvents(ENTITY_TYPE, "ORD-1", null, null, 50, null);

        assertThat(page.data()).hasSize(1);
        EntityRecord r = page.data().get(0);
        assertThat(r.entityId()).isEqualTo("ORD-1");
        assertThat(r.topic()).isEqualTo("orders.events");
        assertThat(r.offset()).isEqualTo(0L);
        assertThat(Base64.getUrlDecoder().decode(r.value()))
                .isEqualTo(b("{\"order_id\":\"ORD-1\"}"));
    }

    @Test
    void queryEntityEvents_emptyResult_whenEntityNotFound() throws Exception {
        PagedResponse<EntityRecord> page =
                service.queryEntityEvents(ENTITY_TYPE, "NONEXISTENT", null, null, 50, null);

        assertThat(page.data()).isEmpty();
        assertThat(page.hasMore()).isFalse();
    }

    // -------------------------------------------------------------------------
    // Cross-topic ordering
    // -------------------------------------------------------------------------

    @Test
    void queryEntityEvents_ordersCrossTopicByTimestamp() throws Exception {
        String entity = "ORD-X";
        Instant t1 = Instant.parse("2024-01-01T09:00:00Z");
        Instant t2 = Instant.parse("2024-01-01T10:00:00Z");
        Instant t3 = Instant.parse("2024-01-01T11:00:00Z");

        // Insert from two different topics, out of chronological order
        insertEntityRow(entity, 10, "payments.events", 0, 5L, t2, b("{\"type\":\"payment\"}"));
        insertEntityRow(entity, 10, "orders.events",   0, 3L, t1, b("{\"type\":\"order\"}"));
        insertEntityRow(entity, 10, "audit.log",       0, 1L, t3, b("{\"type\":\"audit\"}"));

        PagedResponse<EntityRecord> page =
                service.queryEntityEvents(ENTITY_TYPE, entity, null, null, 50, null);

        assertThat(page.data()).hasSize(3);
        // Should be ordered by timestamp ascending
        assertThat(page.data()).extracting(EntityRecord::timestamp)
                .containsExactly(t1, t2, t3);
        assertThat(page.data()).extracting(EntityRecord::topic)
                .containsExactly("orders.events", "payments.events", "audit.log");
    }

    // -------------------------------------------------------------------------
    // Pagination
    // -------------------------------------------------------------------------

    @Test
    void queryEntityEvents_paginatesWithCursor() throws Exception {
        Instant base = Instant.parse("2024-04-01T00:00:00Z");
        for (int i = 0; i < 5; i++) {
            insertEntityRow("ORD-P", 7, "orders.events", 0, i, base.plusSeconds(i), b("v" + i));
        }

        PagedResponse<EntityRecord> page1 =
                service.queryEntityEvents(ENTITY_TYPE, "ORD-P", null, null, 2, null);
        assertThat(page1.data()).hasSize(2);
        assertThat(page1.hasMore()).isTrue();
        assertThat(page1.data()).extracting(EntityRecord::offset).containsExactly(0L, 1L);

        PagedResponse<EntityRecord> page2 =
                service.queryEntityEvents(ENTITY_TYPE, "ORD-P", null, null, 2, page1.nextCursor());
        assertThat(page2.data()).hasSize(2);
        assertThat(page2.data()).extracting(EntityRecord::offset).containsExactly(2L, 3L);

        PagedResponse<EntityRecord> page3 =
                service.queryEntityEvents(ENTITY_TYPE, "ORD-P", null, null, 2, page2.nextCursor());
        assertThat(page3.data()).hasSize(1);
        assertThat(page3.hasMore()).isFalse();
        assertThat(page3.data().get(0).offset()).isEqualTo(4L);
    }

    // -------------------------------------------------------------------------
    // Deduplication
    // -------------------------------------------------------------------------

    @Test
    void queryEntityEvents_deduplicates_keepsMostRecentRecordedAt() throws Exception {
        Instant ts = Instant.parse("2024-02-01T08:00:00Z");
        Instant recordedOld = Instant.parse("2024-02-01T08:00:00Z");
        Instant recordedNew = Instant.parse("2024-02-01T08:00:10Z");

        // Same (topic, partition, offset) — two writes with different recorded_at.
        insertEntityRowAt("ORD-D", 5, "orders.events", 0, 0L, ts, recordedOld, b("first"));
        insertEntityRowAt("ORD-D", 5, "orders.events", 0, 0L, ts, recordedNew, b("second"));

        PagedResponse<EntityRecord> page =
                service.queryEntityEvents(ENTITY_TYPE, "ORD-D", null, null, 50, null);

        assertThat(page.data()).hasSize(1);
        assertThat(Base64.getUrlDecoder().decode(page.data().get(0).value()))
                .isEqualTo(b("second"));
    }

    // -------------------------------------------------------------------------
    // Timestamp filter
    // -------------------------------------------------------------------------

    @Test
    void queryEntityEvents_filtersByTimestampRange() throws Exception {
        Instant t1 = Instant.parse("2024-01-01T08:00:00Z");
        Instant t2 = Instant.parse("2024-01-01T12:00:00Z");
        Instant t3 = Instant.parse("2024-01-01T16:00:00Z");

        insertEntityRow("ORD-F", 1, "orders.events", 0, 0L, t1, null);
        insertEntityRow("ORD-F", 1, "orders.events", 0, 1L, t2, null);
        insertEntityRow("ORD-F", 1, "orders.events", 0, 2L, t3, null);

        PagedResponse<EntityRecord> page =
                service.queryEntityEvents(ENTITY_TYPE, "ORD-F",
                        t1.plusSeconds(1), t3.minusSeconds(1), 50, null);

        assertThat(page.data()).hasSize(1);
        assertThat(page.data().get(0).offset()).isEqualTo(1L);
    }

    // -------------------------------------------------------------------------
    // Known-entity listing and search
    // -------------------------------------------------------------------------

    @Test
    void listEntities_returnsAllByType() throws Exception {
        insertKnownEntity("order", "ORD-A", 1, "2024-01-01T00:00:00Z");
        insertKnownEntity("order", "ORD-B", 2, "2024-01-02T00:00:00Z");
        insertKnownEntity("customer", "CUST-1", 3, "2024-01-01T00:00:00Z"); // different type

        PagedResponse<EntityInfo> page = service.listEntities("order", 50, null, EntityReplayService.EntitySortBy.id);

        assertThat(page.data()).hasSize(2);
        assertThat(page.data()).extracting(EntityInfo::entityId)
                .containsExactly("ORD-A", "ORD-B"); // ordered by entity_id
    }

    @Test
    void listEntities_paginatesWithCursor() throws Exception {
        insertKnownEntity("order", "ORD-1", 1, "2024-01-01T00:00:00Z");
        insertKnownEntity("order", "ORD-2", 2, "2024-01-01T00:00:00Z");
        insertKnownEntity("order", "ORD-3", 3, "2024-01-01T00:00:00Z");

        PagedResponse<EntityInfo> page1 = service.listEntities("order", 2, null, EntityReplayService.EntitySortBy.id);
        assertThat(page1.data()).hasSize(2);
        assertThat(page1.hasMore()).isTrue();
        assertThat(page1.data()).extracting(EntityInfo::entityId).containsExactly("ORD-1", "ORD-2");

        PagedResponse<EntityInfo> page2 = service.listEntities("order", 2, page1.nextCursor(), EntityReplayService.EntitySortBy.id);
        assertThat(page2.data()).hasSize(1);
        assertThat(page2.hasMore()).isFalse();
        assertThat(page2.data().get(0).entityId()).isEqualTo("ORD-3");
    }

    @Test
    void searchEntities_matchesSubstring_caseInsensitive() throws Exception {
        insertKnownEntity("order", "ORD-ALPHA", 1, "2024-01-01T00:00:00Z");
        insertKnownEntity("order", "ORD-BETA",  2, "2024-01-01T00:00:00Z");
        insertKnownEntity("order", "ORD-GAMMA", 3, "2024-01-01T00:00:00Z");

        PagedResponse<EntityInfo> page = service.searchEntities("order", "alpha", 50, null, EntityReplayService.EntitySortBy.id);

        assertThat(page.data()).hasSize(1);
        assertThat(page.data().get(0).entityId()).isEqualTo("ORD-ALPHA");
    }

    @Test
    void searchEntities_noMatch_returnsEmpty() throws Exception {
        insertKnownEntity("order", "ORD-1", 1, "2024-01-01T00:00:00Z");

        PagedResponse<EntityInfo> page = service.searchEntities("order", "XXXXXX", 50, null, EntityReplayService.EntitySortBy.id);

        assertThat(page.data()).isEmpty();
    }

    // -------------------------------------------------------------------------
    // Entity stats
    // -------------------------------------------------------------------------

    @Test
    void getEntityStats_returnsAggregates() throws Exception {
        Instant t1 = Instant.parse("2024-03-01T10:00:00Z");
        Instant t2 = Instant.parse("2024-03-01T11:00:00Z");
        Instant t3 = Instant.parse("2024-03-01T12:00:00Z");

        insertEntityRow("ORD-S", 9, "orders.events",   0, 0L, t1, b("e1"));
        insertEntityRow("ORD-S", 9, "orders.events",   0, 1L, t2, b("e2"));
        insertEntityRow("ORD-S", 9, "payments.events", 0, 0L, t3, b("e3"));

        insertKnownEntity("order", "ORD-S", 9, t1.toString());

        EntityStats stats = service.getEntityStats(ENTITY_TYPE, "ORD-S");

        assertThat(stats.entityType()).isEqualTo(ENTITY_TYPE);
        assertThat(stats.entityId()).isEqualTo("ORD-S");
        assertThat(stats.messageCount()).isEqualTo(3);
        assertThat(stats.firstMessage()).isEqualTo(t1);
        assertThat(stats.lastMessage()).isEqualTo(t3);
        assertThat(stats.countByTopic()).containsEntry("orders.events", 2L);
        assertThat(stats.countByTopic()).containsEntry("payments.events", 1L);
    }

    // -------------------------------------------------------------------------
    // Entity type validation
    // -------------------------------------------------------------------------

    @Test
    void validateEntityType_rejectsInvalidPattern() {
        assertThatThrownBy(() -> EntityReplayService.validateEntityType("UPPER"))
                .isInstanceOf(com.joxette.api.error.ValidationException.class);
        assertThatThrownBy(() -> EntityReplayService.validateEntityType("1bad"))
                .isInstanceOf(com.joxette.api.error.ValidationException.class);
        assertThatThrownBy(() -> EntityReplayService.validateEntityType(null))
                .isInstanceOf(com.joxette.api.error.ValidationException.class);
    }

    @Test
    void validateEntityType_acceptsValidPattern() {
        // should not throw
        EntityReplayService.validateEntityType("order");
        EntityReplayService.validateEntityType("order_item");
        EntityReplayService.validateEntityType("a1b2c3");
    }

    // -------------------------------------------------------------------------
    // Bucket query via entity stats
    // -------------------------------------------------------------------------

    @Test
    void getEntityStats_countsOnlyForRequestedEntity() throws Exception {
        Instant ts = Instant.parse("2024-01-01T00:00:00Z");
        insertEntityRow("ORD-1", 1, "orders.events", 0, 0L, ts, null);
        insertEntityRow("ORD-2", 2, "orders.events", 0, 1L, ts, null);

        insertKnownEntity("order", "ORD-1", 1, ts.toString());
        insertKnownEntity("order", "ORD-2", 2, ts.toString());

        EntityStats stats1 = service.getEntityStats(ENTITY_TYPE, "ORD-1");
        EntityStats stats2 = service.getEntityStats(ENTITY_TYPE, "ORD-2");

        assertThat(stats1.messageCount()).isEqualTo(1);
        assertThat(stats2.messageCount()).isEqualTo(1);
    }

    // -------------------------------------------------------------------------
    // order=asc|desc — parameterized ordering and cursor round-trip
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(Order.class)
    void queryEntityEvents_returnsEventsInRequestedOrder(Order order) throws Exception {
        Instant base = Instant.parse("2026-01-01T00:00:00Z");
        int total = 5;
        for (int i = 0; i < total; i++) {
            insertEntityRow("ORD-O", 7, "orders.events", 0, i, base.plusSeconds(i), b("v" + i));
        }

        PagedResponse<EntityRecord> page = service.queryEntityEvents(
                ENTITY_TYPE, "ORD-O", null, null, 50, null,
                TransformPipeline.IDENTITY, "", order);

        assertThat(page.data()).hasSize(total);
        List<Long> offsets = page.data().stream().map(EntityRecord::offset).toList();
        if (order == Order.ASC) {
            assertThat(offsets).containsExactly(0L, 1L, 2L, 3L, 4L);
        } else {
            assertThat(offsets).containsExactly(4L, 3L, 2L, 1L, 0L);
        }
    }

    @ParameterizedTest
    @EnumSource(Order.class)
    void queryEntityEvents_paginatesWithCursor_inRequestedOrder(Order order) throws Exception {
        Instant base = Instant.parse("2026-02-01T00:00:00Z");
        int total = 6;
        for (int i = 0; i < total; i++) {
            insertEntityRow("ORD-P2", 4, "orders.events", 0, i, base.plusSeconds(i), b("v" + i));
        }

        List<Long> collected = new ArrayList<>();
        String cursor = null;
        do {
            PagedResponse<EntityRecord> page = service.queryEntityEvents(
                    ENTITY_TYPE, "ORD-P2", null, null, 2, cursor,
                    TransformPipeline.IDENTITY, "", order);
            page.data().forEach(r -> collected.add(r.offset()));
            cursor = page.nextCursor();
            if (!page.hasMore()) break;
        } while (cursor != null);

        assertThat(collected).hasSize(total);
        if (order == Order.ASC) {
            assertThat(collected).containsExactly(0L, 1L, 2L, 3L, 4L, 5L);
        } else {
            assertThat(collected).containsExactly(5L, 4L, 3L, 2L, 1L, 0L);
        }
    }

    @ParameterizedTest
    @EnumSource(Order.class)
    void queryEntityEvents_emptyResult_inRequestedOrder(Order order) throws Exception {
        PagedResponse<EntityRecord> page = service.queryEntityEvents(
                ENTITY_TYPE, "ORD-NONE", null, null, 50, null,
                TransformPipeline.IDENTITY, "", order);
        assertThat(page.data()).isEmpty();
        assertThat(page.hasMore()).isFalse();
        assertThat(page.nextCursor()).isNull();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void insertEntityRow(String entityId, int bucket, String topic,
            int partition, long offset, Instant timestamp, byte[] value) throws Exception {
        insertEntityRowAt(entityId, bucket, topic, partition, offset, timestamp, Instant.now(), value);
    }

    private void insertEntityRowAt(String entityId, int bucket, String topic,
            int partition, long offset, Instant timestamp, Instant recordedAt, byte[] value)
            throws Exception {
        DuckDBTestSupport.insertEntityRow(duckDB, ENTITY_TYPE, entityId, bucket, "testEvent",
                topic, partition, offset, timestamp, recordedAt, entityId, value);
    }

    private void insertKnownEntity(String entityType, String entityId, int bucket,
            String firstSeen) throws Exception {
        Instant fs = Instant.parse(firstSeen);
        try (PreparedStatement ps = duckDB.prepareStatement("""
                INSERT INTO known_entities
                    (entity_type, entity_id, first_seen, last_seen)
                VALUES (?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """)) {
            ps.setString(1, entityType);
            ps.setString(2, entityId);
            ps.setTimestamp(3, Timestamp.from(fs));
            ps.setTimestamp(4, Timestamp.from(fs));
            ps.executeUpdate();
        }
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
