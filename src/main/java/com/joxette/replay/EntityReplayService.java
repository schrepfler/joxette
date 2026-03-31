package com.joxette.replay;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Queries entity cassettes ({@code lake.entity_{type}}) and the
 * {@code lake.known_entities} registry.
 *
 * <h2>Deduplication</h2>
 * <p>The entity cassette PK is {@code (entity_id, timestamp, recorded_at)},
 * so the same {@code (topic, partition, offset)} triple can appear multiple
 * times. A {@code QUALIFY ROW_NUMBER() OVER (PARTITION BY topic, partition,
 * "offset" ORDER BY recorded_at DESC) = 1} clause keeps only the
 * most-recently recorded copy per source Kafka message.
 *
 * <h2>Cursor encoding</h2>
 * <p>The entity replay cursor encodes
 * {@code (timestamp, recorded_at, source_topic, source_partition,
 * source_offset)}, matching the five-column {@code ORDER BY} used after
 * deduplication. jOOQ's {@code seekAfter} generates the correct tuple
 * comparison for these five columns.
 *
 * <h2>Known-entity pagination</h2>
 * <p>List and search cursors encode only {@code entity_id} (ordering column)
 * as URL-safe base64.
 */
@Service
public class EntityReplayService {

    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[a-z][a-z0-9_]*");
    private static final int STREAM_PAGE_SIZE = 500;

    // -------------------------------------------------------------------------
    // Field references (shared across entity cassette and known_entities)
    // -------------------------------------------------------------------------

    private static final Field<String>         F_ENTITY_ID     = DSL.field(DSL.name("entity_id"),     String.class);
    private static final Field<Integer>        F_ENTITY_BUCKET = DSL.field(DSL.name("entity_bucket"), Integer.class);
    private static final Field<String>         F_TOPIC         = DSL.field(DSL.name("topic"),         String.class);
    private static final Field<Integer>        F_PARTITION     = DSL.field(DSL.name("partition"),     Integer.class);
    private static final Field<Long>           F_OFFSET        = DSL.field(DSL.name("offset"),        Long.class);
    private static final Field<OffsetDateTime> F_TIMESTAMP     = DSL.field(DSL.name("timestamp"),     OffsetDateTime.class);
    private static final Field<OffsetDateTime> F_RECORDED_AT   = DSL.field(DSL.name("recorded_at"),   OffsetDateTime.class);
    private static final Field<String>         F_KEY           = DSL.field(DSL.name("key"),           String.class);
    private static final Field<byte[]>         F_VALUE         = DSL.field(DSL.name("value"),         byte[].class);
    private static final Field<Object>         F_HEADERS       = DSL.field(DSL.name("headers"),       Object.class);

    // known_entities-specific fields
    private static final Table<?>              KNOWN_ENTITIES  = DSL.table(DSL.name("lake", "known_entities"));
    private static final Field<String>         F_ENTITY_TYPE   = DSL.field(DSL.name("entity_type"),   String.class);
    private static final Field<OffsetDateTime> F_FIRST_SEEN    = DSL.field(DSL.name("first_seen"),    OffsetDateTime.class);
    private static final Field<OffsetDateTime> F_LAST_SEEN     = DSL.field(DSL.name("last_seen"),     OffsetDateTime.class);

    // QUALIFY deduplication for entity cassette rows
    private static final Condition QUALIFY_DEDUP = DSL.rowNumber().over(
            DSL.partitionBy(F_TOPIC, F_PARTITION.cast(SQLDataType.BIGINT), F_OFFSET)
               .orderBy(F_RECORDED_AT.desc())
    ).eq(1);

    private final DSLContext dsl;

    public EntityReplayService(DSLContext dsl) {
        this.dsl = dsl;
    }

    // -------------------------------------------------------------------------
    // Entity event replay
    // -------------------------------------------------------------------------

    /**
     * Returns one page of deduplicated events for {@code entityId} from
     * {@code lake.entity_{entityType}}.
     */
    public PagedResponse<EntityRecord> queryEntityEvents(
            String entityType,
            String entityId,
            Instant from, Instant to,
            int limit,
            String cursor
    ) throws SQLException {
        validateEntityType(entityType);
        EntityCursor decoded = cursor != null ? EntityCursor.decode(cursor) : null;

        Table<?> entityTable = entityTable(entityType);

        Condition cond = F_ENTITY_ID.eq(entityId);
        if (from != null) cond = cond.and(F_TIMESTAMP.ge(from.atOffset(ZoneOffset.UTC)));
        if (to != null)   cond = cond.and(F_TIMESTAMP.le(to.atOffset(ZoneOffset.UTC)));

        var selectBase = dsl
                .select(F_ENTITY_ID, F_ENTITY_BUCKET, F_TOPIC, F_PARTITION, F_OFFSET,
                        F_TIMESTAMP, F_RECORDED_AT, F_KEY, F_VALUE, F_HEADERS)
                .from(entityTable)
                .where(cond)
                .qualify(QUALIFY_DEDUP)
                .orderBy(F_TIMESTAMP.asc(), F_RECORDED_AT.asc(), F_TOPIC.asc(),
                         F_PARTITION.asc(), F_OFFSET.asc());

        List<EntityRecord> records;
        if (decoded != null) {
            records = selectBase
                    .seekAfter(decoded.timestamp().atOffset(ZoneOffset.UTC),
                               decoded.recordedAt().atOffset(ZoneOffset.UTC),
                               decoded.sourceTopic(),
                               decoded.sourcePartition(),
                               decoded.sourceOffset())
                    .limit(limit + 1)
                    .fetch(EntityReplayService::mapEntityRecord);
        } else {
            records = selectBase
                    .limit(limit + 1)
                    .fetch(EntityReplayService::mapEntityRecord);
        }

        return TopicReplayService.buildPage(records, limit,
                r -> new EntityCursor(r.timestamp(), r.recordedAt(), r.topic(), r.partition(), r.offset()).encode());
    }

    /**
     * Streams all matching entity events through internal cursor pagination.
     */
    public void streamEntityEvents(
            String entityType, String entityId,
            Instant from, Instant to,
            Consumer<EntityRecord> sink
    ) throws SQLException {
        String pageCursor = null;
        do {
            PagedResponse<EntityRecord> page =
                    queryEntityEvents(entityType, entityId, from, to, STREAM_PAGE_SIZE, pageCursor);
            page.data().forEach(sink);
            pageCursor = page.nextCursor();
            if (!page.hasMore()) break;
        } while (true);
    }

    // -------------------------------------------------------------------------
    // Known-entity list / search
    // -------------------------------------------------------------------------

    /**
     * Lists known entities of {@code entityType} from {@code lake.known_entities},
     * ordered by {@code entity_id} ascending.
     */
    public PagedResponse<EntityInfo> listEntities(
            String entityType, int limit, String cursor
    ) throws SQLException {
        String afterId = cursor != null ? decodePlainCursor(cursor) : null;

        var selectBase = dsl
                .select(F_ENTITY_TYPE, F_ENTITY_ID, F_ENTITY_BUCKET, F_FIRST_SEEN, F_LAST_SEEN)
                .from(KNOWN_ENTITIES)
                .where(F_ENTITY_TYPE.eq(entityType))
                .orderBy(F_ENTITY_ID.asc());

        List<EntityInfo> entities;
        if (afterId != null) {
            entities = selectBase.seekAfter(afterId).limit(limit + 1).fetch(EntityReplayService::mapEntityInfo);
        } else {
            entities = selectBase.limit(limit + 1).fetch(EntityReplayService::mapEntityInfo);
        }

        return TopicReplayService.buildPage(entities, limit,
                e -> encodePlainCursor(e.entityId()));
    }

    /**
     * Searches known entities whose {@code entity_id} contains {@code q} (case-insensitive).
     */
    public PagedResponse<EntityInfo> searchEntities(
            String entityType, String q, int limit, String cursor
    ) throws SQLException {
        String afterId = cursor != null ? decodePlainCursor(cursor) : null;

        var selectBase = dsl
                .select(F_ENTITY_TYPE, F_ENTITY_ID, F_ENTITY_BUCKET, F_FIRST_SEEN, F_LAST_SEEN)
                .from(KNOWN_ENTITIES)
                .where(F_ENTITY_TYPE.eq(entityType)
                        .and(F_ENTITY_ID.likeIgnoreCase("%" + escapeLike(q) + "%")))
                .orderBy(F_ENTITY_ID.asc());

        List<EntityInfo> entities;
        if (afterId != null) {
            entities = selectBase.seekAfter(afterId).limit(limit + 1).fetch(EntityReplayService::mapEntityInfo);
        } else {
            entities = selectBase.limit(limit + 1).fetch(EntityReplayService::mapEntityInfo);
        }

        return TopicReplayService.buildPage(entities, limit,
                e -> encodePlainCursor(e.entityId()));
    }

    // -------------------------------------------------------------------------
    // Entity stats
    // -------------------------------------------------------------------------

    public EntityStats getEntityStats(String entityType, String entityId) throws SQLException {
        validateEntityType(entityType);
        Table<?> entityTable = entityTable(entityType);

        // Deduplicated inner table used by both aggregation sub-queries
        Table<?> deduped = dsl
                .select(F_TIMESTAMP, F_TOPIC)
                .from(entityTable)
                .where(F_ENTITY_ID.eq(entityId))
                .qualify(QUALIFY_DEDUP)
                .asTable("deduped");

        Field<OffsetDateTime> dedupedTs    = DSL.field(DSL.name("timestamp"),   OffsetDateTime.class);
        Field<String>         dedupedTopic = DSL.field(DSL.name("topic"),        String.class);
        Field<Integer>        fCnt         = DSL.count().as("cnt");
        Field<OffsetDateTime> fFirstMsg    = DSL.min(dedupedTs).as("first_msg");
        Field<OffsetDateTime> fLastMsg     = DSL.max(dedupedTs).as("last_msg");

        // Aggregate count and timestamp range
        long count = 0;
        Instant firstMsg = null;
        Instant lastMsg  = null;

        var aggRecord = dsl.select(fCnt, fFirstMsg, fLastMsg).from(deduped).fetchOne();
        if (aggRecord != null) {
            count = aggRecord.get(fCnt).longValue();
            OffsetDateTime f = aggRecord.get(fFirstMsg);
            OffsetDateTime l = aggRecord.get(fLastMsg);
            if (f != null) firstMsg = f.toInstant();
            if (l != null) lastMsg  = l.toInstant();
        }

        // Count per source topic
        Map<String, Long> countByTopic = new LinkedHashMap<>();
        dsl.select(dedupedTopic, fCnt)
                .from(deduped)
                .groupBy(dedupedTopic)
                .orderBy(dedupedTopic.asc())
                .forEach(r -> countByTopic.put(r.get(dedupedTopic), r.get(fCnt).longValue()));

        // First / last seen from the entity registry
        Instant firstSeen = null;
        Instant lastSeen  = null;

        var regRecord = dsl
                .select(F_FIRST_SEEN, F_LAST_SEEN)
                .from(KNOWN_ENTITIES)
                .where(F_ENTITY_TYPE.eq(entityType).and(F_ENTITY_ID.eq(entityId)))
                .fetchOne();
        if (regRecord != null) {
            firstSeen = regRecord.get(F_FIRST_SEEN).toInstant();
            lastSeen  = regRecord.get(F_LAST_SEEN).toInstant();
        }

        return new EntityStats(entityType, entityId, count, firstMsg, lastMsg,
                firstSeen, lastSeen, countByTopic);
    }

    // -------------------------------------------------------------------------
    // Record mappers
    // -------------------------------------------------------------------------

    private static EntityRecord mapEntityRecord(Record r) {
        try {
            return new EntityRecord(
                    r.get(F_ENTITY_ID),
                    r.get(F_ENTITY_BUCKET),
                    r.get(F_TOPIC),
                    r.get(F_PARTITION),
                    r.get(F_OFFSET),
                    r.get(F_TIMESTAMP).toInstant(),
                    r.get(F_RECORDED_AT).toInstant(),
                    r.get(F_KEY),
                    TopicReplayService.encodeBlob(r.get(F_VALUE)),
                    TopicReplayService.mapHeaders(r.get(F_HEADERS))
            );
        } catch (SQLException e) {
            throw new DataAccessException("Failed to map entity record headers", e);
        }
    }

    private static EntityInfo mapEntityInfo(Record r) {
        return new EntityInfo(
                r.get(F_ENTITY_TYPE),
                r.get(F_ENTITY_ID),
                r.get(F_ENTITY_BUCKET),
                r.get(F_FIRST_SEEN).toInstant(),
                r.get(F_LAST_SEEN).toInstant()
        );
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static Table<?> entityTable(String entityType) {
        return DSL.table(DSL.name("lake", "entity_" + entityType));
    }

    private static String encodePlainCursor(String entityId) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(entityId.getBytes(StandardCharsets.UTF_8));
    }

    private static String decodePlainCursor(String encoded) {
        return new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.UTF_8);
    }

    private static String escapeLike(String q) {
        return q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }

    static void validateEntityType(String type) {
        if (type == null || !SAFE_IDENTIFIER.matcher(type).matches()) {
            throw new IllegalArgumentException(
                    "Invalid entity type '%s': must match [a-z][a-z0-9_]*".formatted(type));
        }
    }
}
