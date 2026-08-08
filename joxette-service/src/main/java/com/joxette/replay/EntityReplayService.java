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

import com.joxette.config.JoxetteProperties;
import com.joxette.db.SchemaManager;
import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformContext;
import com.joxette.replay.transform.TransformPipeline;
import com.joxette.replay.transform.steps.SqlPushdownAnalyzer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
import java.util.UUID;
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

    private static final Logger log = LoggerFactory.getLogger(EntityReplayService.class);

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

    private static final Table<?>              KNOWN_ENTITIES      = DSL.table(DSL.name("known_entities"));
    private static final Field<String>         F_ENTITY_TYPE       = DSL.field(DSL.name("entity_type"),       String.class);
    private static final Field<OffsetDateTime> F_FIRST_SEEN        = DSL.field(DSL.name("first_seen"),        OffsetDateTime.class);
    private static final Field<OffsetDateTime> F_LAST_SEEN         = DSL.field(DSL.name("last_seen"),         OffsetDateTime.class);
    private static final Field<Long>           F_MESSAGE_COUNT     = DSL.field(DSL.name("message_count"),     Long.class);
    private static final Field<String[]>       F_SOURCE_TOPICS     = DSL.field(DSL.name("source_topics"),     String[].class);
    private static final Field<String>         F_LAST_MESSAGE_TYPE = DSL.field(DSL.name("last_message_type"), String.class);

    // QUALIFY deduplication clauses — chosen at query time based on DedupPolicy
    private static final Condition QUALIFY_DEDUP_OFFSET = DSL.rowNumber().over(
            DSL.partitionBy(F_TOPIC, F_PARTITION.cast(SQLDataType.BIGINT), F_OFFSET)
               .orderBy(F_RECORDED_AT.desc())
    ).eq(1);

    // Partition by (topic, value) — stronger dedup for idempotent producers
    private static final Condition QUALIFY_DEDUP_VALUE = DSL.rowNumber().over(
            DSL.partitionBy(F_TOPIC, F_VALUE)
               .orderBy(F_RECORDED_AT.desc())
    ).eq(1);

    // Keep the old name as an alias so internal callers that don't pass a policy use OFFSET
    private static final Condition QUALIFY_DEDUP = QUALIFY_DEDUP_OFFSET;

    /** Carries getEntityStats' three query results out of the withObjectStoreRetry lambda. */
    private record StatsQueryResult(
            long count, Instant firstMsg, Instant lastMsg,
            Map<String, Long> countByTopic, Instant firstSeen, Instant lastSeen,
            int fileCount) {}

    private final DSLContext dsl;
    private final Connection duckDB;
    private final JoxetteProperties props;

    public EntityReplayService(DSLContext dsl, Connection duckDB, JoxetteProperties props) {
        this.dsl = dsl;
        this.duckDB = duckDB;
        this.props = props;
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
        return queryEntityEvents(entityType, entityId, from, to, limit, cursor,
                                 pipeline, replayId, order, null);
    }

    public PagedResponse<EntityRecord> queryEntityEvents(
            String entityType,
            String entityId,
            Instant from, Instant to,
            int limit,
            String cursor,
            TransformPipeline pipeline,
            String replayId,
            Order order,
            List<String> messageTypes
    ) throws SQLException {
        return queryEntityEvents(entityType, entityId, from, to, limit, cursor,
                                 pipeline, replayId, order, messageTypes, null, null);
    }

    /**
     * Full-signature overload that adds {@code lastN} tail-window and {@code dedup} policy.
     *
     * <p>{@code lastN} is mutually exclusive with {@code from}/{@code to}/{@code cursor}.
     * When set, the query runs DESC limited to {@code lastN}, then the result is reversed
     * in Java to produce chronological order.
     *
     * @param lastN       when non-null, return the last N events (tail window); ignores from/to/cursor
     * @param dedup       deduplication policy; null defaults to {@link DedupPolicy#OFFSET}
     */
    public PagedResponse<EntityRecord> queryEntityEvents(
            String entityType,
            String entityId,
            Instant from, Instant to,
            int limit,
            String cursor,
            TransformPipeline pipeline,
            String replayId,
            Order order,
            List<String> messageTypes,
            Integer lastN,
            DedupPolicy dedup
    ) throws SQLException {
        validateEntityType(entityType);

        // Push eligible filter_drop steps down to SQL before materialising rows.
        SqlPushdownAnalyzer.PushdownResult pushdown =
                SqlPushdownAnalyzer.analyze(pipeline.steps(), PUSHDOWN_ELIGIBLE);
        TransformPipeline prunedPipeline = pipeline.withSteps(pushdown.remainingSteps());

        Table<?> entityTable = entityTable(entityType);

        Condition cond = pushdown.pushdownCondition().and(F_ENTITY_ID.eq(entityId));
        if (messageTypes != null && !messageTypes.isEmpty())
            cond = cond.and(F_MESSAGE_TYPE.in(messageTypes));
        final Condition finalCond = cond;

        Condition qualifyClause = switch (dedup == null ? DedupPolicy.OFFSET : dedup) {
            case OFFSET -> QUALIFY_DEDUP_OFFSET;
            case VALUE  -> QUALIFY_DEDUP_VALUE;
            case NONE   -> null;
        };

        // last_n: tail-window query — ignore from/to/cursor, query DESC, reverse result
        if (lastN != null) {
            List<EntityRecord> tail = TopicReplayService.withObjectStoreRetry(
                    "queryEntityEvents:lastN:" + entityType, () -> {
                synchronized (duckDB) {
                    var tailBase = dsl
                            .select(F_ENTITY_ID, F_MESSAGE_TYPE, F_TOPIC, F_PARTITION, F_OFFSET,
                                    F_TIMESTAMP, F_RECORDED_AT, F_KEY, F_VALUE, F_HEADERS)
                            .from(entityTable)
                            .where(finalCond);
                    var qualified = qualifyClause != null ? tailBase.qualify(qualifyClause) : tailBase;
                    return qualified
                            .orderBy(F_TIMESTAMP.desc(), F_RECORDED_AT.desc(),
                                     F_TOPIC.desc(), F_PARTITION.desc(), F_OFFSET.desc())
                            .limit(lastN)
                            .fetch(EntityReplayService::mapEntityRecord);
                }
            });
            // Reverse to chronological order
            java.util.Collections.reverse(tail);

            if (!prunedPipeline.isIdentity()) {
                List<EntityRecord> transformed = new ArrayList<>();
                for (EntityRecord r : tail) {
                    prunedPipeline.apply(new ReplayMessage(r), replayId)
                            .stream().map(ReplayMessage::toEntityRecord).forEach(transformed::add);
                }
                tail = transformed;
            }
            Boolean transformApplied = !pipeline.steps().isEmpty() ? Boolean.TRUE : null;
            // last_n is single-shot — no cursor, hasMore always false
            return new PagedResponse<>(tail, null, false, transformApplied, null);
        }

        EntityCursor decoded = cursor != null ? EntityCursor.decode(cursor) : null;

        if (from != null) cond = cond.and(F_TIMESTAMP.ge(from.atOffset(ZoneOffset.UTC)));
        if (to != null)   cond = cond.and(F_TIMESTAMP.le(to.atOffset(ZoneOffset.UTC)));
        final Condition finalCondWithRange = cond;

        boolean desc = order == Order.DESC;
        // kafka_timestamp is producer-assigned and subject to cross-host clock skew when
        // events originate from multiple topics / services. recorded_at is the tiebreaker
        // and provides a consistent single-clock view, but it is not the primary sort key.
        // See docs/entity-ordering.md for guidance on when to prefer recorded_at ordering.
        List<EntityRecord> records = TopicReplayService.withObjectStoreRetry(
                "queryEntityEvents:" + entityType, () -> {
            synchronized (duckDB) {
                var baseSelect = dsl
                        .select(F_ENTITY_ID, F_MESSAGE_TYPE, F_TOPIC, F_PARTITION, F_OFFSET,
                                F_TIMESTAMP, F_RECORDED_AT, F_KEY, F_VALUE, F_HEADERS)
                        .from(entityTable)
                        .where(finalCondWithRange);
                var selectBase = qualifyClause != null
                        ? baseSelect.qualify(qualifyClause)
                                .orderBy(desc ? F_TIMESTAMP.desc()   : F_TIMESTAMP.asc(),
                                         desc ? F_RECORDED_AT.desc() : F_RECORDED_AT.asc(),
                                         desc ? F_TOPIC.desc()       : F_TOPIC.asc(),
                                         desc ? F_PARTITION.desc()   : F_PARTITION.asc(),
                                         desc ? F_OFFSET.desc()      : F_OFFSET.asc())
                        : baseSelect
                                .orderBy(desc ? F_TIMESTAMP.desc()   : F_TIMESTAMP.asc(),
                                         desc ? F_RECORDED_AT.desc() : F_RECORDED_AT.asc(),
                                         desc ? F_TOPIC.desc()       : F_TOPIC.asc(),
                                         desc ? F_PARTITION.desc()   : F_PARTITION.asc(),
                                         desc ? F_OFFSET.desc()      : F_OFFSET.asc());

                // seekAfter is direction-aware in jOOQ: it inspects the ORDER BY
                // and emits the correct comparison for ASC / DESC. The cursor is
                // the same tuple; only the query direction changes.
                if (decoded != null) {
                    return selectBase
                            .seekAfter(decoded.timestamp().atOffset(ZoneOffset.UTC),
                                       decoded.recordedAt().atOffset(ZoneOffset.UTC),
                                       decoded.sourceTopic(),
                                       decoded.sourcePartition(),
                                       decoded.sourceOffset())
                            .limit(limit + 1)
                            .fetch(EntityReplayService::mapEntityRecord);
                } else {
                    return selectBase
                            .limit(limit + 1)
                            .fetch(EntityReplayService::mapEntityRecord);
                }
            }
        });

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
        streamEntityEvents(entityType, entityId, from, to, sink, pipeline, replayId,
                           order, follow, hooks, null);
    }

    public void streamEntityEvents(
            String entityType, String entityId,
            Instant from, Instant to,
            Consumer<EntityRecord> sink,
            TransformPipeline pipeline,
            String replayId,
            Order order,
            FollowSubscription<EntityRecord, EntityCursor> follow,
            FollowHooks<EntityRecord> hooks,
            List<String> messageTypes
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
                                          TransformPipeline.IDENTITY, "", order, messageTypes);
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
                                          TransformPipeline.IDENTITY, "", order, messageTypes);
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

    /** Sort order for the known-entities list. */
    public enum EntitySortBy { id, lastActive, mostMessages }

    /**
     * Lists known entities of {@code entityType} from {@code known_entities}.
     *
     * @param sortBy  one of id (default), lastActive (DESC last_seen, ASC entity_id),
     *                mostMessages (DESC message_count, ASC entity_id)
     */
    public PagedResponse<EntityInfo> listEntities(
            String entityType, int limit, String cursor, EntitySortBy sortBy
    ) throws SQLException {
        Condition where = F_ENTITY_TYPE.eq(entityType);
        return fetchEntityPage(where, limit, cursor, sortBy);
    }

    /**
     * Searches known entities whose {@code entity_id} contains {@code q} (case-insensitive).
     */
    public PagedResponse<EntityInfo> searchEntities(
            String entityType, String q, int limit, String cursor, EntitySortBy sortBy
    ) throws SQLException {
        Condition where = F_ENTITY_TYPE.eq(entityType)
                .and(F_ENTITY_ID.likeIgnoreCase("%" + escapeLike(q) + "%"));
        return fetchEntityPage(where, limit, cursor, sortBy);
    }

    private PagedResponse<EntityInfo> fetchEntityPage(
            Condition where, int limit, String cursor, EntitySortBy sortBy
    ) {
        // Decode and fully parse the cursor *before* touching the database. Keeping
        // decode/parse isolated from the query-execution below means an unrelated
        // IllegalArgumentException thrown while fetching or mapping rows can never be
        // mislabeled as a cursor error.
        String afterId = null;
        OffsetDateTime afterLastSeen = null;
        String afterLastSeenEntityId = null;
        long afterMessageCount = 0;
        String afterMessageCountEntityId = null;
        try {
            if (cursor != null) {
                switch (sortBy) {
                    case id -> afterId = decodePlainCursor(cursor);
                    case lastActive -> {
                        String[] parts = decodeTupleCursor(cursor);
                        afterLastSeen = OffsetDateTime.ofInstant(
                                Instant.ofEpochMilli(Long.parseLong(parts[0])), ZoneOffset.UTC);
                        afterLastSeenEntityId = parts[1];
                    }
                    case mostMessages -> {
                        String[] parts = decodeTupleCursor(cursor);
                        afterMessageCount = Long.parseLong(parts[0]);
                        afterMessageCountEntityId = parts[1];
                    }
                }
            }
        } catch (IllegalArgumentException e) {
            // Base64.decode() throws IllegalArgumentException on malformed input, and
            // Long.parseLong() (a subclass, NumberFormatException) on a non-numeric tuple
            // component — both mean "cursor could not be decoded", exactly what
            // TopicCursor/EntityCursor (Task D) already map to InvalidCursorException so
            // GlobalExceptionHandler renders 400 ERR_INVALID_CURSOR instead of falling
            // through to a generic 500 ERR_INTERNAL. decodeTupleCursor throws
            // InvalidCursorException directly for a missing separator, which is not an
            // IllegalArgumentException and so passes through this catch untouched.
            throw com.joxette.api.error.InvalidCursorException.malformed(e);
        }

        List<EntityInfo> entities;
        synchronized (duckDB) {
            var select = dsl
                    .select(F_ENTITY_TYPE, F_ENTITY_ID, F_FIRST_SEEN, F_LAST_SEEN,
                            F_MESSAGE_COUNT, F_SOURCE_TOPICS, F_LAST_MESSAGE_TYPE)
                    .from(KNOWN_ENTITIES)
                    .where(where);

            entities = switch (sortBy) {
                case id -> {
                    var sorted = select.orderBy(F_ENTITY_ID.asc());
                    yield afterId != null
                            ? sorted.seekAfter(afterId).limit(limit + 1).fetch(EntityReplayService::mapEntityInfo)
                            : sorted.limit(limit + 1).fetch(EntityReplayService::mapEntityInfo);
                }
                case lastActive -> {
                    var sorted = select.orderBy(F_LAST_SEEN.desc(), F_ENTITY_ID.asc());
                    yield afterLastSeenEntityId != null
                            ? sorted.seekAfter(afterLastSeen, afterLastSeenEntityId)
                              .limit(limit + 1).fetch(EntityReplayService::mapEntityInfo)
                            : sorted.limit(limit + 1).fetch(EntityReplayService::mapEntityInfo);
                }
                case mostMessages -> {
                    var sorted = select.orderBy(F_MESSAGE_COUNT.desc(), F_ENTITY_ID.asc());
                    yield afterMessageCountEntityId != null
                            ? sorted.seekAfter(afterMessageCount, afterMessageCountEntityId)
                              .limit(limit + 1).fetch(EntityReplayService::mapEntityInfo)
                            : sorted.limit(limit + 1).fetch(EntityReplayService::mapEntityInfo);
                }
            };
        }

        return TopicReplayService.buildPage(entities, limit, e -> switch (sortBy) {
            case id           -> encodePlainCursor(e.entityId());
            case lastActive   -> encodeTupleCursor(String.valueOf(e.lastSeen().toEpochMilli()), e.entityId());
            case mostMessages -> encodeTupleCursor(String.valueOf(e.messageCount()), e.entityId());
        });
    }

    // -------------------------------------------------------------------------
    // Entity stats
    // -------------------------------------------------------------------------

    public EntityStats getEntityStats(String entityType, String entityId) throws SQLException {
        validateEntityType(entityType);

        // Pure-read CTE — no temp table writes, safe for concurrent callers on any
        // catalog backend (embedded DuckDB, Quack, PostgreSQL).
        // entityType is validated above ([a-z][a-z0-9_]*); entityId is a bind param.
        String tableName = "lake.main.entity_" + entityType;
        String bareTable = "entity_" + entityType;
        String dedupCte =
                "WITH deduped AS ("
                + "  SELECT kafka_timestamp AS ts, topic"
                + "  FROM " + tableName
                + "  WHERE entity_id = {0}"
                + "  QUALIFY ROW_NUMBER() OVER"
                + "    (PARTITION BY topic, kafka_partition, kafka_offset"
                + "     ORDER BY recorded_at DESC) = 1"
                + ") ";

        org.jooq.Param<String> idParam = DSL.val(entityId);

        StatsQueryResult result = TopicReplayService.withObjectStoreRetry(
                "getEntityStats:" + entityType, () -> {
            long count = 0;
            Instant firstMsg = null;
            Instant lastMsg  = null;
            Map<String, Long> countByTopic = new LinkedHashMap<>();
            Instant firstSeen = null;
            Instant lastSeen  = null;
            synchronized (duckDB) {
                var aggRecord = dsl.fetchOne(
                        DSL.resultQuery(dedupCte
                                + "SELECT COUNT(*) AS cnt, MIN(ts) AS first_msg, MAX(ts) AS last_msg"
                                + " FROM deduped",
                                idParam));
                if (aggRecord != null) {
                    count = ((Number) aggRecord.get("cnt")).longValue();
                    var f = aggRecord.get("first_msg", java.time.OffsetDateTime.class);
                    var l = aggRecord.get("last_msg",  java.time.OffsetDateTime.class);
                    if (f != null) firstMsg = f.toInstant();
                    if (l != null) lastMsg  = l.toInstant();
                }

                dsl.fetch(
                        DSL.resultQuery(dedupCte
                                + "SELECT topic, COUNT(*) AS cnt FROM deduped"
                                + " GROUP BY topic ORDER BY topic",
                                idParam))
                   .forEach(r -> countByTopic.put(
                           r.get("topic", String.class),
                           ((Number) r.get("cnt")).longValue()));

                var regRecord = dsl
                        .select(F_FIRST_SEEN, F_LAST_SEEN)
                        .from(KNOWN_ENTITIES)
                        .where(F_ENTITY_TYPE.eq(entityType).and(F_ENTITY_ID.eq(entityId)))
                        .fetchOne();
                if (regRecord != null) {
                    firstSeen = regRecord.get(F_FIRST_SEEN).toInstant();
                    lastSeen  = regRecord.get(F_LAST_SEEN).toInstant();
                }

                int fileCount;
                try {
                    fileCount = countEntityFiles(bareTable, entityType, entityId);
                } catch (SQLException e) {
                    throw new DataAccessException("countEntityFiles failed for " + bareTable, e);
                }
                return new StatsQueryResult(count, firstMsg, lastMsg, countByTopic, firstSeen, lastSeen, fileCount);
            }
        });

        long count = result.count();
        Instant firstMsg = result.firstMsg();
        Instant lastMsg = result.lastMsg();
        Map<String, Long> countByTopic = result.countByTopic();
        Instant firstSeen = result.firstSeen();
        Instant lastSeen = result.lastSeen();

        String objectStoreDirectory = computeObjectStoreDirectory(bareTable);
        String storageConsoleUrl = computeStorageConsoleUrl(bareTable);

        return new EntityStats(entityType, entityId, count, firstMsg, lastMsg,
                firstSeen, lastSeen, countByTopic, result.fileCount(),
                objectStoreDirectory, storageConsoleUrl);
    }

    // -------------------------------------------------------------------------
    // Entity stats: object-store location
    // -------------------------------------------------------------------------

    private String computeObjectStoreDirectory(String table) {
        String objectStoragePath = props.getCatalog().getObjectStoragePath();
        if (objectStoragePath == null || objectStoragePath.isBlank()) return null;
        String base = objectStoragePath.endsWith("/") ? objectStoragePath : objectStoragePath + "/";
        return base + "main/" + table + "/";
    }

    private String computeStorageConsoleUrl(String table) {
        String objectStoragePath = props.getCatalog().getObjectStoragePath();
        String urlTemplate = props.getStorageConsole().getUrlTemplate();
        if (objectStoragePath == null || objectStoragePath.isBlank()
                || urlTemplate == null || urlTemplate.isBlank()) {
            return null;
        }
        String noScheme = objectStoragePath.replaceFirst("^s3://", "");
        int slash = noScheme.indexOf('/');
        String bucket = slash < 0 ? noScheme : noScheme.substring(0, slash);
        String rest = slash < 0 ? "" : noScheme.substring(slash + 1);
        if (!rest.isEmpty() && !rest.endsWith("/")) rest = rest + "/";
        String prefix = rest + "main/" + table + "/";
        String encodedPrefix = java.net.URLEncoder.encode(prefix, java.nio.charset.StandardCharsets.UTF_8);
        return urlTemplate.replace("{bucket}", bucket).replace("{prefix}", encodedPrefix);
    }

    // -------------------------------------------------------------------------
    // Entity stats: file count (two-stage catalog-prune + verify)
    // -------------------------------------------------------------------------

    /**
     * Exact count of physical Parquet files containing at least one row for
     * {@code entityId}. Stage 1 prunes candidates via DuckLake's internal
     * per-file column statistics (cheap, catalog-only); stage 2 verifies the
     * survivors with a targeted read. Falls back to verifying every file for
     * the table if stage 1's internal-metadata query fails (not a stable
     * app-facing API); reports 0 if DuckLake's own {@code ducklake_list_files}
     * is unavailable at all (e.g. not a real DuckLake-backed catalog).
     */
    private int countEntityFiles(String table, String entityType, String entityId) throws SQLException {
        String objectStoragePath = props.getCatalog().getObjectStoragePath();
        if (objectStoragePath == null || objectStoragePath.isBlank()) return 0;

        List<String> candidates = pruneCandidateFiles(table, entityType, entityId);
        if (candidates.isEmpty()) return 0;
        return verifyCandidates(candidates, entityId);
    }

    private List<String> pruneCandidateFiles(String table, String entityType, String entityId) {
        try {
            if (!SchemaManager.hasUnpartitionedFiles(duckDB, table)) {
                return globBucketDirectory(table, entityType, entityId);
            }
        } catch (SQLException e) {
            log.debug("countEntityFiles: bucket-glob fast path unavailable for table '{}' ({}); " +
                    "falling back to full scan", table, e.getMessage());
        }

        List<String> allFiles;
        try {
            allFiles = listAllFiles(table);
        } catch (SQLException e) {
            log.debug("countEntityFiles: ducklake_list_files unavailable for table '{}' ({}); reporting 0 files",
                    table, e.getMessage());
            return List.of();
        }
        if (allFiles.isEmpty()) return List.of();

        Set<String> candidateFilenames;
        try {
            candidateFilenames = candidateFilenamesViaColumnStats(table, entityId);
        } catch (SQLException e) {
            log.debug("countEntityFiles: catalog column-stats prune unavailable for table '{}' ({}); " +
                    "verifying all {} file(s)", table, e.getMessage(), allFiles.size());
            return allFiles;
        }

        List<String> filtered = new ArrayList<>();
        for (String path : allFiles) {
            String basename = path.substring(path.lastIndexOf('/') + 1);
            if (candidateFilenames.contains(basename)) filtered.add(path);
        }
        return filtered;
    }

    /**
     * Fast path: once a table has no legacy unpartitioned files left
     * ({@link SchemaManager#hasUnpartitionedFiles} returned {@code false}),
     * every file that could contain this entity's rows lives under its own
     * {@code bucket=N/} directory — glob just that directory instead of
     * scanning the whole table's catalog metadata.
     */
    private List<String> globBucketDirectory(String table, String entityType, String entityId) throws SQLException {
        int bucketCount = lookupBucketCount(entityType);
        int bucket = MessageRouter.computeBucket(entityType, entityId, bucketCount);
        String objectStoragePath = props.getCatalog().getObjectStoragePath();
        String base = objectStoragePath.endsWith("/") ? objectStoragePath : objectStoragePath + "/";
        String glob = base + "main/" + table + "/bucket=" + bucket + "/**/*.parquet";
        List<String> files = new ArrayList<>();
        try (PreparedStatement ps = duckDB.prepareStatement("SELECT file FROM glob(?)")) {
            ps.setString(1, glob);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) files.add(rs.getString(1));
            }
        }
        return files;
    }

    /** Bucket count configured for {@code entityType}, defaulting to 256 if not registered. */
    private int lookupBucketCount(String entityType) throws SQLException {
        try (PreparedStatement ps = duckDB.prepareStatement(
                "SELECT bucket_count FROM entity_type_configs WHERE entity_type = ?")) {
            ps.setString(1, entityType);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 256;
            }
        }
    }

    /** Full, fully-resolved file list for {@code table} via DuckLake's own function. */
    private List<String> listAllFiles(String table) throws SQLException {
        List<String> files = new ArrayList<>();
        try (PreparedStatement ps = duckDB.prepareStatement(
                "SELECT data_file FROM ducklake_list_files('lake', ?)")) {
            ps.setString(1, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) files.add(rs.getString(1));
            }
        }
        return files;
    }

    /**
     * Bare filenames (not full paths) of files whose entity_id column
     * min/max range could include {@code entityId}, per DuckLake's internal
     * per-file column statistics. Zone-map pruning: a file surviving this
     * filter isn't guaranteed to actually contain the value, only that it
     * isn't excluded by range — {@link #verifyCandidates} does the exact check.
     */
    private Set<String> candidateFilenamesViaColumnStats(String table, String entityId) throws SQLException {
        String sql =
                "SELECT df.path "
              + "FROM __ducklake_metadata_lake.ducklake_file_column_stats fcs "
              + "JOIN __ducklake_metadata_lake.ducklake_data_file df USING (data_file_id) "
              + "JOIN __ducklake_metadata_lake.ducklake_table t ON t.table_id = fcs.table_id "
              + "JOIN __ducklake_metadata_lake.ducklake_column c "
              + "  ON c.table_id = fcs.table_id AND c.column_id = fcs.column_id "
              + "WHERE t.table_name = ? AND t.end_snapshot IS NULL "
              + "  AND c.column_name = 'entity_id' AND c.end_snapshot IS NULL "
              + "  AND (? >= fcs.min_value OR fcs.min_value IS NULL) "
              + "  AND (? <= fcs.max_value OR fcs.max_value IS NULL)";
        Set<String> filenames = new java.util.HashSet<>();
        try (PreparedStatement ps = duckDB.prepareStatement(sql)) {
            ps.setString(1, table);
            ps.setString(2, entityId);
            ps.setString(3, entityId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String p = rs.getString(1);
                    filenames.add(p.substring(p.lastIndexOf('/') + 1));
                }
            }
        }
        return filenames;
    }

    /** Exact count: reads only {@code candidates}, filtered to real entity_id matches. */
    private int verifyCandidates(List<String> candidates, String entityId) throws SQLException {
        String filesLiteral = candidates.stream()
                .map(p -> "'" + p.replace("'", "''") + "'")
                .collect(java.util.stream.Collectors.joining(", "));
        String sql = "SELECT COUNT(DISTINCT filename) FROM read_parquet([" + filesLiteral
                + "], filename => true) WHERE entity_id = ?";
        try (PreparedStatement ps = duckDB.prepareStatement(sql)) {
            ps.setString(1, entityId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
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
        String[] topicsArr = r.get(F_SOURCE_TOPICS);
        java.util.List<String> topics = topicsArr != null ? java.util.List.of(topicsArr) : java.util.List.of();
        Long msgCount = r.get(F_MESSAGE_COUNT);
        return new EntityInfo(
                r.get(F_ENTITY_TYPE),
                r.get(F_ENTITY_ID),
                r.get(F_FIRST_SEEN).toInstant(),
                r.get(F_LAST_SEEN).toInstant(),
                msgCount != null ? msgCount : 0L,
                topics,
                r.get(F_LAST_MESSAGE_TYPE)
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

    /** Encodes two values separated by NUL into a URL-safe base64 cursor. */
    private static String encodeTupleCursor(String first, String second) {
        String joined = first + '\0' + second;
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(joined.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Decodes a tuple cursor into {@code [first, second]}.
     *
     * @throws com.joxette.api.error.InvalidCursorException if the decoded value has no
     *         {@code \0} separator — a garbage-but-base64-decodable cursor must be
     *         rejected with 400 ERR_INVALID_CURSOR, not silently treated as absent
     *         (which would return page 1 with 200 instead of erroring).
     */
    private static String[] decodeTupleCursor(String encoded) {
        String joined = new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.UTF_8);
        int sep = joined.indexOf('\0');
        if (sep < 0) {
            throw com.joxette.api.error.InvalidCursorException.malformed(
                    new IllegalArgumentException("cursor missing tuple separator"));
        }
        return new String[]{ joined.substring(0, sep), joined.substring(sep + 1) };
    }

    private static String escapeLike(String q) {
        return q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }

    static void validateEntityType(String type) {
        if (type == null || !SAFE_IDENTIFIER.matcher(type).matches()) {
            throw com.joxette.api.error.ValidationException.field("entityType",
                    "must match [a-z][a-z0-9_]* (got '%s')".formatted(type));
        }
    }
}
