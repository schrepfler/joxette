package com.joxette.replay;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Defines and registers DuckDB scalar macros for working with Kafka-style
 * message headers stored as {@code LIST(STRUCT(key VARCHAR, value BLOB))}.
 *
 * <p>Call {@link #registerMacros(Connection)} once at startup (via
 * {@link SchemaManager}) before executing any query that references these
 * macro names.
 */
public final class HeadersHelper {

    public static final String MACRO_HEADERS_GET     = "headers_get";
    public static final String MACRO_HEADERS_GET_ALL = "headers_get_all";
    public static final String MACRO_HEADERS_PUT     = "headers_put";
    public static final String MACRO_HEADERS_TO_MAP  = "headers_to_map";

    /**
     * Returns the BLOB value of the <em>first</em> header whose key matches.
     * Returns {@code NULL} when the key is absent.
     */
    static final String DDL_HEADERS_GET = """
            CREATE OR REPLACE MACRO headers_get(headers, key) AS (
              list_filter(headers, h -> h.key = key)[1].value
            )
            """;

    /**
     * Returns a {@code LIST(BLOB)} of every header value whose key matches,
     * in insertion order.  Returns an empty list when the key is absent.
     */
    static final String DDL_HEADERS_GET_ALL = """
            CREATE OR REPLACE MACRO headers_get_all(headers, key) AS (
              list_transform(list_filter(headers, h -> h.key = key), h -> h.value)
            )
            """;

    /**
     * Appends a new {@code (key, value)} pair to the end of the header list
     * and returns the extended list.  Duplicates are allowed; existing entries
     * are never modified.
     */
    static final String DDL_HEADERS_PUT = """
            CREATE OR REPLACE MACRO headers_put(headers, key, value) AS (
              list_append(headers, {'key': key, 'value': value})
            )
            """;

    /**
     * Converts headers to {@code MAP(VARCHAR, VARCHAR)}, decoding each BLOB
     * value as UTF-8.  When a key appears more than once the <em>last</em>
     * value wins (last-write-wins).  The conversion is lossy: non-UTF-8 bytes
     * produce undefined results and duplicate keys are collapsed.
     *
     * <p>Implementation notes:
     * <ul>
     *   <li>{@code list_distinct} extracts the ordered set of unique keys.</li>
     *   <li>For each unique key, {@code list_filter(...) [-1]} selects the last
     *       matching entry, implementing last-write-wins semantics.</li>
     *   <li>{@code decode()} converts the BLOB bytes to VARCHAR assuming UTF-8.</li>
     * </ul>
     */
    static final String DDL_HEADERS_TO_MAP = """
            CREATE OR REPLACE MACRO headers_to_map(headers) AS (
              MAP(
                list_distinct(list_transform(headers, h -> h.key)),
                list_transform(
                  list_distinct(list_transform(headers, h -> h.key)),
                  k -> decode(list_filter(headers, h -> h.key = k)[-1].value)
                )
              )
            )
            """;

    private HeadersHelper() {}

    /**
     * Registers (or replaces) all four header macros on the given connection.
     * Safe to call on every startup because each statement uses
     * {@code CREATE OR REPLACE MACRO}.
     */
    public static void registerMacros(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(DDL_HEADERS_GET);
            st.execute(DDL_HEADERS_GET_ALL);
            st.execute(DDL_HEADERS_PUT);
            st.execute(DDL_HEADERS_TO_MAP);
        }
    }
}
