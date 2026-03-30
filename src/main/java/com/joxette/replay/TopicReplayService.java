package com.joxette.replay;

import org.springframework.stereotype.Service;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Queries the general cassette ({@code lake.cassette}) with cursor-based
 * pagination and optional column filters.
 *
 * <h2>Deduplication</h2>
 * <p>DuckDB does not enforce primary-key uniqueness, so the same
 * {@code (topic, partition, offset)} triple may appear more than once after
 * concurrent or replayed writes. Every query wraps the base scan in a
 * {@code QUALIFY ROW_NUMBER() OVER (PARTITION BY topic, partition, "offset"
 * ORDER BY recorded_at DESC) = 1} clause to keep only the most-recently
 * recorded copy.
 *
 * <h2>Cursor encoding</h2>
 * <p>The cursor encodes {@code (timestamp, partition, offset)} as URL-safe
 * base64(JSON). Keyset pagination uses a lexicographic tuple comparison on
 * those three columns, matching the {@code ORDER BY} of the query.
 *
 * <h2>Thread safety</h2>
 * <p>All database access is serialised via {@code synchronized(duckDB)}.
 */
@Service
public class TopicReplayService {

    private static final int STREAM_PAGE_SIZE = 500;

    private final Connection duckDB;

    public TopicReplayService(Connection duckDB) {
        this.duckDB = duckDB;
    }

    /**
     * Returns one page of records from {@code lake.cassette} for {@code topic}.
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
        TopicCursor decoded = cursor != null ? TopicCursor.decode(cursor) : null;

        var sql = new StringBuilder("""
                SELECT topic, partition, "offset", timestamp, recorded_at, key, value, headers
                FROM (
                    SELECT topic, partition, "offset", timestamp, recorded_at, key, value, headers
                    FROM lake.cassette
                    WHERE topic = ?
                """);
        List<Object> params = new ArrayList<>();
        params.add(topic);

        if (from != null) {
            sql.append("      AND timestamp >= ?\n");
            params.add(Timestamp.from(from));
        }
        if (to != null) {
            sql.append("      AND timestamp <= ?\n");
            params.add(Timestamp.from(to));
        }
        if (partition != null) {
            sql.append("      AND partition = ?\n");
            params.add(partition);
        }
        if (offsetFrom != null) {
            sql.append("      AND \"offset\" >= ?\n");
            params.add(offsetFrom);
        }
        if (offsetTo != null) {
            sql.append("      AND \"offset\" <= ?\n");
            params.add(offsetTo);
        }

        sql.append("""
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY topic, partition, "offset" ORDER BY recorded_at DESC) = 1
                ) deduped
                """);

        if (decoded != null) {
            sql.append("""
                    WHERE (timestamp > ?)
                       OR (timestamp = ? AND partition > ?)
                       OR (timestamp = ? AND partition = ? AND "offset" > ?)
                    """);
            Timestamp ts = Timestamp.from(decoded.timestamp());
            params.add(ts);
            params.add(ts); params.add(decoded.partition());
            params.add(ts); params.add(decoded.partition()); params.add(decoded.offset());
        }

        sql.append("ORDER BY timestamp ASC, partition ASC, \"offset\" ASC\nLIMIT ?\n");
        params.add(limit + 1);

        List<CassetteRecord> records = executeQuery(sql.toString(), params);
        return buildPage(records, limit,
                r -> new TopicCursor(r.timestamp(), r.partition(), r.offset()).encode());
    }

    /**
     * Streams all matching records from {@code lake.cassette} by internally
     * paginating with {@link #STREAM_PAGE_SIZE} and feeding each record to
     * {@code sink}. Releases the DB lock between pages.
     */
    public void streamAll(
            String topic,
            Instant from, Instant to,
            Integer partition,
            Long offsetFrom, Long offsetTo,
            Consumer<CassetteRecord> sink
    ) throws SQLException {
        String pageCursor = null;
        do {
            PagedResponse<CassetteRecord> page =
                    query(topic, from, to, partition, offsetFrom, offsetTo, STREAM_PAGE_SIZE, pageCursor);
            page.data().forEach(sink);
            pageCursor = page.nextCursor();
            if (!page.hasMore()) break;
        } while (true);
    }

    // -------------------------------------------------------------------------
    // JDBC helpers
    // -------------------------------------------------------------------------

    private List<CassetteRecord> executeQuery(String sql, List<Object> params) throws SQLException {
        List<CassetteRecord> records = new ArrayList<>();
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(sql)) {
                bindParams(ps, params);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        records.add(mapRecord(rs));
                    }
                }
            }
        }
        return records;
    }

    private static CassetteRecord mapRecord(ResultSet rs) throws SQLException {
        return new CassetteRecord(
                rs.getString("topic"),
                rs.getInt("partition"),
                rs.getLong("offset"),
                rs.getTimestamp("timestamp").toInstant(),
                rs.getTimestamp("recorded_at").toInstant(),
                rs.getString("key"),
                encodeBlob(rs.getBytes("value")),
                mapHeaders(rs.getObject("headers"))
        );
    }

    static String encodeBlob(byte[] bytes) {
        return bytes == null ? null
                : Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

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
            if (elem instanceof Map<?, ?> struct) {
                String key = (String) struct.get("key");
                byte[] value = (byte[]) struct.get("value");
                headers.add(new CassetteRecord.Header(key, encodeBlob(value)));
            }
        }
        return headers;
    }

    static void bindParams(PreparedStatement ps, List<Object> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            ps.setObject(i + 1, params.get(i));
        }
    }

    static <T> PagedResponse<T> buildPage(
            List<T> fetched, int limit, Function<T, String> cursorOf) {
        boolean hasMore = fetched.size() > limit;
        List<T> data = hasMore ? new ArrayList<>(fetched.subList(0, limit)) : fetched;
        String nextCursor = hasMore ? cursorOf.apply(data.getLast()) : null;
        return new PagedResponse<>(data, nextCursor, hasMore);
    }
}
