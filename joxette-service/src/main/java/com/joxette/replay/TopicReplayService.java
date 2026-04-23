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

import org.duckdb.DuckDBStruct;

import java.sql.Array;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformContext;
import com.joxette.replay.transform.TransformPipeline;
import com.joxette.replay.transform.steps.SqlPushdownAnalyzer;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;


/**
 * Queries the general cassette table ({@code lake.main.general_{topic}}) for a
 * specific topic, with cursor-based pagination and optional column filters.
 *
 * <h2>Table naming</h2>
 * <p>Each topic has its own DuckLake table {@code lake.main.general_{normalized_topic}},
 * created by {@link com.joxette.db.SchemaManager}.  The topic name is normalised
 * to {@code [a-z0-9_]} before constructing the table reference.
 *
 * <h2>Deduplication</h2>
 * <p>DuckDB does not enforce primary-key uniqueness, so the same
 * {@code (kafka_partition, kafka_offset)} triple may appear more than once after
 * concurrent or replayed writes. Every query uses
 * {@code QUALIFY ROW_NUMBER() OVER (PARTITION BY kafka_partition, kafka_offset
 * ORDER BY recorded_at DESC) = 1} to keep only the most-recently recorded copy.
 *
 * <h2>Cursor encoding</h2>
 * <p>The cursor encodes {@code (timestamp, partition, offset)} as URL-safe
 * base64(JSON). Keyset pagination uses jOOQ's {@code seekAfter} which generates
 * the correct tuple comparison on those three columns, matching the
 * {@code ORDER BY} of the query.
 */
@Service
public class TopicReplayService implements CassetteSource {

    private static final int STREAM_PAGE_SIZE = 500;

    /**
     * JSONPath field names eligible for SQL pushdown in the general cassette table.
     * {@code $.topic} is excluded — the topic is encoded in the table name, not a column.
     */
    private static final Set<String> PUSHDOWN_ELIGIBLE = Set.of(
            "$.partition", "$.offset", "$.timestamp", "$.key", "$.recorded_at");

    // -------------------------------------------------------------------------
    // Field references for lake.main.general_{topic}
    // -------------------------------------------------------------------------

    private static final Field<Integer>        F_PARTITION    = DSL.field(DSL.name("kafka_partition"),  Integer.class);
    private static final Field<Long>           F_OFFSET       = DSL.field(DSL.name("kafka_offset"),     Long.class);
    private static final Field<OffsetDateTime> F_TIMESTAMP    = DSL.field(DSL.name("kafka_timestamp"),  OffsetDateTime.class);
    private static final Field<OffsetDateTime> F_RECORDED_AT  = DSL.field(DSL.name("recorded_at"),      OffsetDateTime.class);
    private static final Field<String>         F_KEY          = DSL.field(DSL.name("kafka_key"),        String.class);
    private static final Field<byte[]>         F_VALUE        = DSL.field(DSL.name("kafka_value"),      byte[].class);
    private static final Field<Object>         F_HEADERS      = DSL.field(DSL.name("headers"),          Object.class);
    private static final Field<String>         F_MESSAGE_TYPE = DSL.field(DSL.name("message_type"),     String.class);

    // QUALIFY deduplication: keep the row with the latest recorded_at per
    // (kafka_partition, kafka_offset).
    private static final Condition QUALIFY_DEDUP = DSL.rowNumber().over(
            DSL.partitionBy(F_PARTITION.cast(SQLDataType.BIGINT), F_OFFSET)
               .orderBy(F_RECORDED_AT.desc())
    ).eq(1);

    private final DSLContext dsl;

    public TopicReplayService(DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Returns one page of records from {@code lake.main.general_{topic}}.
     * Convenience overload — equivalent to calling the pipeline-aware variant with
     * {@link TransformPipeline#IDENTITY} (no transformation, no metadata injection).
     *
     * @param limit   max records to return; one extra is fetched internally to
     *                detect whether more pages exist
     * @param cursor  opaque cursor from the previous page, or {@code null} for
     *                the first page
     */
    public PagedResponse<CassetteRecord> query(
            String topic,
            Instant from, Instant to,
            Integer partition,
            Long offsetFrom, Long offsetTo,
            int limit,
            String cursor
    ) throws SQLException {
        return query(topic, from, to, partition, offsetFrom, offsetTo, limit, cursor,
                     TransformPipeline.IDENTITY, "", Order.ASC);
    }

    /**
     * Pipeline-aware overload preserved for callers that do not supply an order.
     * Defaults to {@link Order#ASC} to keep the pre-order-param API behaviour.
     */
    public PagedResponse<CassetteRecord> query(
            String topic,
            Instant from, Instant to,
            Integer partition,
            Long offsetFrom, Long offsetTo,
            int limit,
            String cursor,
            TransformPipeline pipeline,
            String replayId
    ) throws SQLException {
        return query(topic, from, to, partition, offsetFrom, offsetTo, limit, cursor,
                     pipeline, replayId, Order.ASC);
    }

    /**
     * Returns one page of records from {@code lake.main.general_{topic}}, with
     * each record passed through {@code pipeline} before inclusion in the result.
     * Records dropped by a pipeline step are excluded from the page and do not
     * count toward {@code limit}.
     *
     * @param limit    max records to return; one extra is fetched to detect more pages
     * @param cursor   opaque cursor from the previous page, or {@code null} for page 1
     * @param pipeline transform pipeline applied per-record; use
     *                 {@link TransformPipeline#IDENTITY} for no-op
     * @param replayId UUID string for this replay session, forwarded to the pipeline
     *                 for metadata header injection
     */
    public PagedResponse<CassetteRecord> query(
            String topic,
            Instant from, Instant to,
            Integer partition,
            Long offsetFrom, Long offsetTo,
            int limit,
            String cursor,
            TransformPipeline pipeline,
            String replayId,
            Order order
    ) throws SQLException {
        // Push eligible filter_drop steps down to SQL before materialising rows.
        SqlPushdownAnalyzer.PushdownResult pushdown =
                SqlPushdownAnalyzer.analyze(pipeline.steps(), PUSHDOWN_ELIGIBLE);
        TransformPipeline prunedPipeline = pipeline.withSteps(pushdown.remainingSteps());

        Table<?> table = tableFor(topic);
        TopicCursor decoded = cursor != null ? TopicCursor.decode(cursor) : null;

        Condition cond = pushdown.pushdownCondition();
        if (from != null)       cond = cond.and(F_TIMESTAMP.ge(from.atOffset(ZoneOffset.UTC)));
        if (to != null)         cond = cond.and(F_TIMESTAMP.le(to.atOffset(ZoneOffset.UTC)));
        if (partition != null)  cond = cond.and(F_PARTITION.eq(partition));
        if (offsetFrom != null) cond = cond.and(F_OFFSET.ge(offsetFrom));
        if (offsetTo != null)   cond = cond.and(F_OFFSET.le(offsetTo));

        boolean desc = order == Order.DESC;
        var selectBase = dsl
                .select(F_PARTITION, F_OFFSET, F_TIMESTAMP, F_RECORDED_AT, F_KEY, F_VALUE, F_HEADERS, F_MESSAGE_TYPE)
                .from(table)
                .where(cond)
                .qualify(QUALIFY_DEDUP)
                .orderBy(desc ? F_TIMESTAMP.desc() : F_TIMESTAMP.asc(),
                         desc ? F_PARTITION.desc() : F_PARTITION.asc(),
                         desc ? F_OFFSET.desc()    : F_OFFSET.asc());

        final String topicFinal = topic;
        List<CassetteRecord> records;
        if (decoded != null) {
            // jOOQ's seekAfter is direction-aware: it inspects the ORDER BY
            // clause and generates `WHERE (cols) > (vals)` for ASC or
            // `WHERE (cols) < (vals)` for DESC.  So a single code path
            // serves both directions correctly as long as the ORDER BY
            // matches.
            records = selectBase
                    .seekAfter(decoded.timestamp().atOffset(ZoneOffset.UTC),
                               decoded.partition(),
                               decoded.offset())
                    .limit(limit + 1)
                    .fetch(r -> mapRecord(topicFinal, r));
        } else {
            records = selectBase
                    .limit(limit + 1)
                    .fetch(r -> mapRecord(topicFinal, r));
        }

        if (!prunedPipeline.isIdentity()) {
            List<CassetteRecord> transformed = new ArrayList<>();
            for (CassetteRecord r : records) {
                List<ReplayMessage> results = prunedPipeline.apply(new ReplayMessage(r), replayId);
                results.stream().map(ReplayMessage::toCassetteRecord).forEach(transformed::add);
            }
            records = transformed;
        }

        Boolean transformApplied = !pipeline.steps().isEmpty() ? Boolean.TRUE : null;
        return buildPage(records, limit,
                r -> new TopicCursor(r.timestamp(), r.partition(), r.offset()).encode(),
                transformApplied);
    }

    /**
     * Streams all matching records through {@code sink} by internally paginating.
     * Convenience overload — equivalent to calling the pipeline-aware variant with
     * {@link TransformPipeline#IDENTITY} (no transformation, no metadata injection).
     */
    @Override
    public void streamAll(
            String topic,
            Instant from, Instant to,
            Integer partition,
            Long offsetFrom, Long offsetTo,
            Consumer<CassetteRecord> sink
    ) throws SQLException {
        streamAll(topic, from, to, partition, offsetFrom, offsetTo, sink,
                  TransformPipeline.IDENTITY, "", Order.ASC);
    }

    /**
     * Streams all matching records through {@code sink}, applying {@code pipeline}
     * to each record before passing it to the sink. Records dropped by the pipeline
     * are silently skipped. Pages are fetched and released between iterations.
     *
     * <p>A single {@link TransformContext} is shared across all messages in the stream,
     * enabling stateful steps such as {@code time_compress}.  After each
     * {@link TransformPipeline#apply} call, this method sleeps
     * {@link TransformContext#getPendingSleep()} before passing the record to
     * {@code sink} — this is what makes compressed-time replays honour their factor.
     * Paginated paths ({@link #query}) use a throwaway per-message context and never
     * sleep.
     *
     * @param pipeline  transform pipeline applied per-record
     * @param replayId  UUID string for this replay session
     */
    public void streamAll(
            String topic,
            Instant from, Instant to,
            Integer partition,
            Long offsetFrom, Long offsetTo,
            Consumer<CassetteRecord> sink,
            TransformPipeline pipeline,
            String replayId
    ) throws SQLException {
        streamAll(topic, from, to, partition, offsetFrom, offsetTo, sink, pipeline, replayId,
                  Order.ASC, null, null);
    }

    /**
     * ASC-compatible overload used by follow-mode callers that predate the
     * {@link Order} parameter.  New call-sites should pass {@link Order}
     * explicitly via the direction-aware overload below.
     */
    public void streamAll(
            String topic,
            Instant from, Instant to,
            Integer partition,
            Long offsetFrom, Long offsetTo,
            Consumer<CassetteRecord> sink,
            TransformPipeline pipeline,
            String replayId,
            FollowSubscription<CassetteRecord, TopicCursor> follow,
            FollowHooks<CassetteRecord> hooks
    ) throws SQLException {
        streamAll(topic, from, to, partition, offsetFrom, offsetTo, sink, pipeline, replayId,
                  Order.ASC, follow, hooks);
    }

    /**
     * Direction-aware overload.  ASC paginates oldest→newest via
     * {@code seekAfter}; DESC paginates newest→oldest via {@code seekBefore}.
     * Live-tail records (when {@code follow} is non-null) are emitted after the
     * historical drain in arrival order — the client is responsible for
     * positioning them at the head of its list when {@code order==DESC}.
     */
    public void streamAll(
            String topic,
            Instant from, Instant to,
            Integer partition,
            Long offsetFrom, Long offsetTo,
            Consumer<CassetteRecord> sink,
            TransformPipeline pipeline,
            String replayId,
            Order order
    ) throws SQLException {
        streamAll(topic, from, to, partition, offsetFrom, offsetTo, sink, pipeline, replayId,
                  order, null, null);
    }

    /**
     * Streams historical records then, if {@code follow} is non-null, enters a
     * live loop fed by {@code follow}.
     *
     * <p>Each emitted record advances {@code follow.lastEmittedCursor} so the
     * post-drain {@link FollowSubscription#drainBuffered(Consumer)} call can
     * skip any buffered duplicates captured while the drain was still running.
     *
     * @param follow           subscription created before the drain started, or
     *                         {@code null} for the legacy non-follow behavior
     * @param onHistoricalEnd  optional callback invoked exactly once, between
     *                         the last historical record and the first live
     *                         record.  Used by the handler to emit a
     *                         {@code follow} preamble event.  Ignored when
     *                         {@code follow} is {@code null}.
     * @param heartbeat        optional callback invoked when the live loop
     *                         times out waiting for the next record.  Used by
     *                         the handler to emit a heartbeat event.  Ignored
     *                         when {@code follow} is {@code null}.
     */
    public void streamAll(
            String topic,
            Instant from, Instant to,
            Integer partition,
            Long offsetFrom, Long offsetTo,
            Consumer<CassetteRecord> sink,
            TransformPipeline pipeline,
            String replayId,
            Order order,
            FollowSubscription<CassetteRecord, TopicCursor> follow,
            FollowHooks<CassetteRecord> hooks
    ) throws SQLException {
        Consumer<CassetteRecord> tracked = follow == null
                ? sink
                : r -> { sink.accept(r); follow.onEmitted(r); };

        if (pipeline.isIdentity()) {
            // Fast path: no per-record overhead, no context needed
            String pageCursor = null;
            do {
                PagedResponse<CassetteRecord> page =
                        query(topic, from, to, partition, offsetFrom, offsetTo,
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
                PagedResponse<CassetteRecord> rawPage =
                        query(topic, from, to, partition, offsetFrom, offsetTo,
                              STREAM_PAGE_SIZE, pageCursor,
                              TransformPipeline.IDENTITY, "", order);
                for (CassetteRecord r : rawPage.data()) {
                    Optional<ReplayMessage> result =
                            pipeline.apply(new ReplayMessage(r), replayId, ctx);
                    sleepPending(ctx);
                    result.map(ReplayMessage::toCassetteRecord).ifPresent(tracked);
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
            CassetteRecord next;
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
    // Table reference
    // -------------------------------------------------------------------------

    /**
     * Builds a jOOQ {@link Table} reference for the per-topic general cassette:
     * {@code lake.main.general_{normalized_topic}}.
     */
    private static Table<?> tableFor(String topic) {
        String tableName = "general_" + normalizeTopicName(topic);
        return DSL.table(DSL.name("lake", "main", tableName));
    }

    /**
     * Normalises a topic name to {@code [a-z0-9_]}, matching
     * .
     */
    static String normalizeTopicName(String topic) {
        return topic.toLowerCase().replaceAll("[^a-z0-9_]", "_");
    }

    // -------------------------------------------------------------------------
    // Record mapping
    // -------------------------------------------------------------------------

    private static CassetteRecord mapRecord(String topic, Record r) {
        try {
            return new CassetteRecord(
                    topic,
                    r.get(F_PARTITION),
                    r.get(F_OFFSET),
                    r.get(F_TIMESTAMP).toInstant(),
                    r.get(F_RECORDED_AT).toInstant(),
                    r.get(F_KEY),
                    encodeBlob(r.get(F_VALUE)),
                    mapHeaders(r.get(F_HEADERS)),
                    r.get(F_MESSAGE_TYPE)
            );
        } catch (SQLException e) {
            throw new DataAccessException("Failed to map cassette record headers", e);
        }
    }

    // -------------------------------------------------------------------------
    // Package-visible helpers (used by EntityReplayService)
    // -------------------------------------------------------------------------

    static String encodeBlob(byte[] bytes) {
        return bytes == null ? null
                : Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    /**
     * Maps the {@code headers} column ({@code STRUCT(key VARCHAR, value VARCHAR)[]})
     * from a jOOQ record into a list of {@link CassetteRecord.Header}.
     *
     * <p>Both key and value are plain strings — no base64 encoding is applied here.
     * Values that were originally non-UTF-8 binary were base64-encoded on write
     * (see {@code CassetteBatchWriter.decodeHeaderValue}); that encoding is preserved
     * verbatim in the response so callers can detect and decode if needed.
     */
    @SuppressWarnings("unchecked")
    static List<CassetteRecord.Header> mapHeaders(Object headersObj) throws SQLException {
        if (headersObj == null) return List.of();
        Object[] arr;
        if (headersObj instanceof Array sqlArray) {
            arr = (Object[]) sqlArray.getArray();
        } else if (headersObj instanceof Object[] objArr) {
            arr = objArr;
        } else {
            return List.of();
        }
        List<CassetteRecord.Header> headers = new ArrayList<>(arr.length);
        for (Object elem : arr) {
            // DuckDB 1.5.x JDBC returns STRUCT elements as DuckDBStruct (not Map).
            Map<?, ?> struct = switch (elem) {
                case Map<?, ?> m         -> m;
                case DuckDBStruct ds     -> ds.getMap();
                default                  -> null;
            };
            if (struct != null) {
                String key   = (String) struct.get("key");
                // value is VARCHAR — no BLOB cast or base64 encoding needed
                String value = struct.get("value") instanceof String s ? s : "";
                headers.add(new CassetteRecord.Header(key, value));
            }
        }
        return headers;
    }

    static <T> PagedResponse<T> buildPage(
            List<T> fetched, int limit, Function<T, String> cursorOf) {
        return buildPage(fetched, limit, cursorOf, null);
    }

    static <T> PagedResponse<T> buildPage(
            List<T> fetched, int limit, Function<T, String> cursorOf, Boolean transformApplied) {
        boolean hasMore = fetched.size() > limit;
        List<T> data = hasMore ? new ArrayList<>(fetched.subList(0, limit)) : fetched;
        String nextCursor = hasMore ? cursorOf.apply(data.getLast()) : null;
        return new PagedResponse<>(data, nextCursor, hasMore, transformApplied);
    }
}
