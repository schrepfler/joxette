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

import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformContext;
import com.joxette.replay.transform.TransformPipeline;
import com.joxette.replay.transform.steps.SqlPushdownAnalyzer;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Queries entity cassettes ({@code lake.entity_{type}}) and the
 * {@code known_entities} registry (plain DuckDB, main schema).
 *
 * <h2>Deduplication</h2>
 * <p>The entity cassette uses a {@code QUALIFY ROW_NUMBER() OVER (PARTITION BY topic, partition,
 * "offset" ORDER BY recorded_at DESC) = 1} clause to keep only the most-recently
 * recorded copy per source Kafka message.
 *
 * <h2>Cursor encoding</h2>
 * <p>The entity replay cursor encodes
 * {@code (timestamp, recorded_at, source_topic, source_partition, source_offset)},
 * matching the five-column {@code ORDER BY} used after deduplication.
 *
 * <h2>Known-entity pagination</h2>
 * <p>List and search cursors encode only {@code entity_id} (ordering column)
 * as URL-safe base64.
 */
@Service
public class EntityReplayService implements EntityCassetteSource {

    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[a-z][a-z0-9_]*");
    private static final int STREAM_PAGE_SIZE = 500;

    /**
     * JSONPath field names eligible for SQL pushdown in the entity cassette table.
     * Includes {@code $.topic} because entity tables have a {@code topic} column.
     */
    private static final Set<String> PUSHDOWN_ELIGIBLE = Set.of(
            "$.topic", "$.partition", "$.offset", "$.timestamp", "$.key", "$.recorded_at");

    // -------------------------------------------------------------------------
    // Field references for entity cassette tables
    // -------------------------------------------------------------------------

    private static final Field<String>         F_ENTITY_ID    = DSL.field(DSL.name("entity_id"),       String.class);
    private static final Field<String>         F_MESSAGE_TYPE = DSL.field(DSL.name("message_type"),    String.class);
    private static final Field<String>         F_TOPIC        = DSL.field(DSL.name("topic"),           String.class);
    private static final Field<Integer>        F_PARTITION    = DSL.field(DSL.name("kafka_partition"),  Integer.class);
    private static final Field<Long>           F_OFFSET       = DSL.field(DSL.name("kafka_offset"),    Long.class);
    private static final Field<OffsetDateTime> F_TIMESTAMP    = DSL.field(DSL.name("kafka_timestamp"), OffsetDateTime.class);
    private static final Field<OffsetDateTime> F_RECORDED_AT  = DSL.field(DSL.name("recorded_at"),    OffsetDateTime.class);
    private static final Field<String>         F_KEY          = DSL.field(DSL.name("kafka_key"),       String.class);
    private static final Field<byte[]>         F_VALUE        = DSL.field(DSL.name("kafka_value"),     byte[].class);
    private static final Field<Object>         F_HEADERS      = DSL.field(DSL.name("headers"),         Object.class);

    // -------------------------------------------------------------------------
    // Field references for known_entities (plain DuckDB, unqualified name)
    // -------------------------------------------------------------------------

    private static final Table<?>              KNOWN_ENTITIES = DSL.table(DSL.name("known_entities"));
    private static final Field<String>         F_ENTITY_TYPE  = DSL.field(DSL.name("entity_type"), String.class);
    private static final Field<OffsetDateTime> F_FIRST_SEEN   = DSL.field(DSL.name("first_seen"),  OffsetDateTime.class);
    private static final Field<OffsetDateTime> F_LAST_SEEN    = DSL.field(DSL.name("last_seen"),   OffsetDateTime.class);

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
     * Convenience overload — equivalent to calling the pipeline-aware variant with
     * {@link TransformPipeline#IDENTITY} (no transformation, no metadata injection).
     */
    public PagedResponse<EntityRecord> queryEntityEvents(
            String entityType,
            String entityId,
            Instant from, Instant to,
            int limit,
            String cursor
    ) throws SQLException {
        return queryEntityEvents(entityType, entityId, from, to, limit, cursor,
                                 TransformPipeline.IDENTITY, "", Order.ASC);
    }

    /**
     * Pipeline-aware overload preserved for callers that do not supply an order.
     * Defaults to {@link Order#ASC} to keep the pre-order-param API behaviour.
     */
    public PagedResponse<EntityRecord> queryEntityEvents(
            String entityType,
            String entityId,
            Instant from, Instant to,
            int limit,
            String cursor,
            TransformPipeline pipeline,
            String replayId
    ) throws SQLException {
        return queryEntityEvents(entityType, entityId, from, to, limit, cursor,
                                 pipeline, replayId, Order.ASC);
    }

    /**
     * Returns one page of deduplicated events for {@code entityId}, with each
     * record passed through {@code pipeline} before inclusion in the result.
     * Records dropped by a pipeline step are excluded and do not count toward
     * {@code limit}.
     *
     * @param pipeline  transform pipeline applied per-record
     * @param replayId  UUID string for this replay session
     */
    public PagedResponse<EntityRecord> queryEntityEvents(
            String entityType,
            String entityId,
            Instant from, Instant to,
            int limit,
            String cursor,
            TransformPipeline pipeline,
            String replayId,
            Order order
    ) throws SQLException {
        validateEntityType(entityType);

        // Push eligible filter_drop steps down to SQL before materialising rows.
        SqlPushdownAnalyzer.PushdownResult pushdown =
                SqlPushdownAnalyzer.analyze(pipeline.steps(), PUSHDOWN_ELIGIBLE);
        TransformPipeline prunedPipeline = pipeline.withSteps(pushdown.remainingSteps());

        EntityCursor decoded = cursor != null ? EntityCursor.decode(cursor) : null;

        Table<?> entityTable = entityTable(entityType);

        Condition cond = pushdown.pushdownCondition().and(F_ENTITY_ID.eq(entityId));
        if (from != null) cond = cond.and(F_TIMESTAMP.ge(from.atOffset(ZoneOffset.UTC)));
        if (to != null)   cond = cond.and(F_TIMESTAMP.le(to.atOffset(ZoneOffset.UTC)));

        boolean desc = order == Order.DESC;
        var selectBase = dsl
                .select(F_ENTITY_ID, F_MESSAGE_TYPE, F_TOPIC, F_PARTITION, F_OFFSET,
                        F_TIMESTAMP, F_RECORDED_AT, F_KEY, F_VALUE, F_HEADERS)
                .from(entityTable)
                .where(cond)
                .qualify(QUALIFY_DEDUP)
                .orderBy(desc ? F_TIMESTAMP.desc()   : F_TIMESTAMP.asc(),
                         desc ? F_RECORDED_AT.desc() : F_RECORDED_AT.asc(),
                         desc ? F_TOPIC.desc()       : F_TOPIC.asc(),
                         desc ? F_PARTITION.desc()   : F_PARTITION.asc(),
                         desc ? F_OFFSET.desc()      : F_OFFSET.asc());

        List<EntityRecord> records;
        if (decoded != null) {
            // seekAfter is direction-aware in jOOQ: it inspects the ORDER BY
            // and emits the correct comparison for ASC / DESC. The cursor is
            // the same tuple; only the query direction changes.
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

        if (!prunedPipeline.isIdentity()) {
            List<EntityRecord> transformed = new ArrayList<>();
            for (EntityRecord r : records) {
                List<ReplayMessage> results = prunedPipeline.apply(new ReplayMessage(r), replayId);
                results.stream().map(ReplayMessage::toEntityRecord).forEach(transformed::add);
            }
            records = transformed;
        }

        Boolean transformApplied = !pipeline.steps().isEmpty() ? Boolean.TRUE : null;
        return TopicReplayService.buildPage(records, limit,
                r -> new EntityCursor(r.timestamp(), r.recordedAt(), r.topic(), r.partition(), r.offset()).encode(),
                transformApplied);
    }

    /**
     * Streams all matching entity events through internal cursor pagination.
     * Convenience overload — equivalent to calling the pipeline-aware variant with
     * {@link TransformPipeline#IDENTITY} (no transformation, no metadata injection).
     */
    @Override
    public void streamEntityEvents(
            String entityType, String entityId,
            Instant from, Instant to,
            Consumer<EntityRecord> sink
    ) throws SQLException {
        streamEntityEvents(entityType, entityId, from, to, sink,
                           TransformPipeline.IDENTITY, "", Order.ASC);
    }

    /**
     * Streams all matching entity events, applying {@code pipeline} to each record
     * before passing it to the sink. Records dropped by the pipeline are skipped.
     *
     * <p>A single {@link TransformContext} is shared across all messages in the stream,
     * enabling stateful steps such as {@code time_compress}.  After each
     * {@link TransformPipeline#apply} call, this method sleeps
     * {@link TransformContext#getPendingSleep()} before passing the record to
     * {@code sink}.  Paginated paths ({@link #queryEntityEvents}) use a throwaway
     * per-message context and never sleep.
     *
     * @param pipeline  transform pipeline applied per-record
     * @param replayId  UUID string for this replay session
     */
    public void streamEntityEvents(
            String entityType, String entityId,
            Instant from, Instant to,
            Consumer<EntityRecord> sink,
            TransformPipeline pipeline,
            String replayId
    ) throws SQLException {
        streamEntityEvents(entityType, entityId, from, to, sink, pipeline, replayId,
                           Order.ASC, null, null);
    }

    /**
     * ASC-compatible overload for follow-mode callers that predate the
     * {@link Order} parameter.
     */
    public void streamEntityEvents(
            String entityType, String entityId,
            Instant from, Instant to,
            Consumer<EntityRecord> sink,
            TransformPipeline pipeline,
            String replayId,
            FollowSubscription<EntityRecord, EntityCursor> follow,
            FollowHooks<EntityRecord> hooks
    ) throws SQLException {
        streamEntityEvents(entityType, entityId, from, to, sink, pipeline, replayId,
                           Order.ASC, follow, hooks);
    }

    /** Direction-aware overload without follow support. */
    public void streamEntityEvents(
            String entityType, String entityId,
            Instant from, Instant to,
            Consumer<EntityRecord> sink,
            TransformPipeline pipeline,
            String replayId,
            Order order
    ) throws SQLException {
        streamEntityEvents(entityType, entityId, from, to, sink, pipeline, replayId,
                           order, null, null);
    }

    /**
     * Streams historical events then, if {@code follow} is non-null, enters a
     * live loop fed by {@code follow}.  Each emitted record advances
     * {@code follow.lastEmittedCursor}; once the drain completes, buffered
     * live records are flushed with duplicate-suppression, then the live loop
     * emits records or heartbeats until the emitter closes or the bus
     * subscription overflows.
     */
    public void streamEntityEvents(
            String entityType, String entityId,
            Instant from, Instant to,
            Consumer<EntityRecord> sink,
            TransformPipeline pipeline,
            String replayId,
            Order order,
            FollowSubscription<EntityRecord, EntityCursor> follow,
            FollowHooks<EntityRecord> hooks
    ) throws SQLException {
        Consumer<EntityRecord> tracked = follow == null
                ? sink
                : r -> { sink.accept(r); follow.onEmitted(r); };

        if (pipeline.isIdentity()) {
            // Fast path: no per-record overhead, no context needed
            String pageCursor = null;
            do {
                PagedResponse<EntityRecord> page =
                        queryEntityEvents(entityType, entityId, from, to,
                                          STREAM_PAGE_SIZE, pageCursor,
                                          TransformPipeline.IDENTITY, "", order);
                page.data().forEach(tracked);
                pageCursor = page.nextCursor();
                if (!page.hasMore()) break;
            } while (true);
        } else {
            // Per-record path: shared context for stateful steps (e.g. time_compress)
            TransformContext ctx = new TransformContext();
            String pageCursor = null;
            do {
                PagedResponse<EntityRecord> rawPage =
                        queryEntityEvents(entityType, entityId, from, to,
                                          STREAM_PAGE_SIZE, pageCursor,
                                          TransformPipeline.IDENTITY, "", order);
                for (EntityRecord r : rawPage.data()) {
                    Optional<ReplayMessage> result =
                            pipeline.apply(new ReplayMessage(r), replayId, ctx);
                    sleepPending(ctx);
                    result.map(ReplayMessage::toEntityRecord).ifPresent(tracked);
                }
                pageCursor = rawPage.nextCursor();
                if (!rawPage.hasMore()) break;
            } while (true);
        }

        if (follow == null) return;

        if (hooks != null) hooks.onHistoricalEnd();

        follow.drainBuffered(sink);

        Duration heartbeat = hooks != null ? hooks.heartbeatInterval() : Duration.ofSeconds(15);
        while (!follow.isOverflowed()) {
            EntityRecord next;
            try {
                next = follow.awaitNext(heartbeat);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (next == null) {
                if (hooks != null) hooks.onHeartbeat();
            } else {
                sink.accept(next);
            }
        }
        if (hooks != null) hooks.onOverflow();
    }

    /**
     * Sleeps for {@link TransformContext#getPendingSleep()} if non-zero.
     * On interrupt, restores the interrupt flag and returns immediately.
     */
    private static void sleepPending(TransformContext ctx) {
        Duration sleep = ctx.getPendingSleep();
        if (sleep.isZero()) return;
        try {
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // -------------------------------------------------------------------------
    // Known-entity list / search
    // -------------------------------------------------------------------------

    /**
     * Lists known entities of {@code entityType} from {@code known_entities},
     * ordered by {@code entity_id} ascending.
     */
    public PagedResponse<EntityInfo> listEntities(
            String entityType, int limit, String cursor
    ) throws SQLException {
        String afterId = cursor != null ? decodePlainCursor(cursor) : null;

        var selectBase = dsl
                .select(F_ENTITY_TYPE, F_ENTITY_ID, F_FIRST_SEEN, F_LAST_SEEN)
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
                .select(F_ENTITY_TYPE, F_ENTITY_ID, F_FIRST_SEEN, F_LAST_SEEN)
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

        // Deduplicated inner table used by both aggregation sub-queries.
        // Alias kafka_timestamp -> timestamp so aggregate field references are simple.
        Field<OffsetDateTime> tsAlias    = F_TIMESTAMP.as("ts");
        Field<String>         topicAlias = F_TOPIC.as("topic");
        Table<?> deduped = dsl
                .select(tsAlias, topicAlias)
                .from(entityTable)
                .where(F_ENTITY_ID.eq(entityId))
                .qualify(QUALIFY_DEDUP)
                .asTable("deduped");

        Field<OffsetDateTime> dedupedTs    = DSL.field(DSL.name("ts"),    OffsetDateTime.class);
        Field<String>         dedupedTopic = DSL.field(DSL.name("topic"), String.class);
        Field<Integer>        fCnt         = DSL.count().as("cnt");
        Field<OffsetDateTime> fFirstMsg    = DSL.min(dedupedTs).as("first_msg");
        Field<OffsetDateTime> fLastMsg     = DSL.max(dedupedTs).as("last_msg");

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

        Map<String, Long> countByTopic = new LinkedHashMap<>();
        dsl.select(dedupedTopic, fCnt)
                .from(deduped)
                .groupBy(dedupedTopic)
                .orderBy(dedupedTopic.asc())
                .forEach(r -> countByTopic.put(r.get(dedupedTopic), r.get(fCnt).longValue()));

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
                    r.get(F_MESSAGE_TYPE),
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
                r.get(F_FIRST_SEEN).toInstant(),
                r.get(F_LAST_SEEN).toInstant()
        );
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static Table<?> entityTable(String entityType) {
        return DSL.table(DSL.name("lake", "main", "entity_" + entityType));
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
