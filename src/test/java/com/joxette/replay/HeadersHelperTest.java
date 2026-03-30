package com.joxette.replay;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests each of the four header macros registered by {@link HeadersHelper}
 * against an in-memory DuckDB instance.
 *
 * <p>Sample data: three entries where {@code content-type} appears twice
 * (first {@code application/json}, then {@code text/plain}) to exercise
 * duplicate-key behaviour.
 */
class HeadersHelperTest {

    private static Connection conn;

    /**
     * DuckDB SQL expression that evaluates to a typed
     * {@code LIST(STRUCT(key VARCHAR, value BLOB))} with three entries.
     */
    private static final String HEADERS = """
            [
              {'key': 'content-type', 'value': 'application/json'::BLOB},
              {'key': 'x-trace-id',   'value': 'abc123'::BLOB},
              {'key': 'content-type', 'value': 'text/plain'::BLOB}
            ]
            """;

    /**
     * Empty header list with correct element type, produced by filtering all
     * entries out of a singleton list — DuckDB 1.5 cannot parse the typed
     * empty-list literal {@code []::LIST(STRUCT(key VARCHAR, value BLOB))} via JDBC.
     */
    private static final String EMPTY_HEADERS =
            "list_filter([{'key': 'k', 'value': 'v'::BLOB}], x -> false)";

    @BeforeAll
    static void openDb() throws Exception {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        HeadersHelper.registerMacros(conn);
    }

    @AfterAll
    static void closeDb() throws Exception {
        conn.close();
    }

    // -----------------------------------------------------------------------
    // headers_get
    // -----------------------------------------------------------------------

    @Test
    void headersGet_returnsFirstMatchingValue() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT headers_get(%s, 'content-type')".formatted(HEADERS))) {
            assertThat(rs.next()).isTrue();
            assertThat(asString(rs.getBytes(1))).isEqualTo("application/json");
        }
    }

    @Test
    void headersGet_returnsNullForAbsentKey() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT headers_get(%s, 'x-missing')".formatted(HEADERS))) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBytes(1)).isNull();
        }
    }

    // -----------------------------------------------------------------------
    // headers_get_all
    // -----------------------------------------------------------------------

    @Test
    void headersGetAll_returnsAllValuesInOrder() throws Exception {
        // unnest expands the returned LIST into individual rows
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT unnest(headers_get_all(%s, 'content-type'))".formatted(HEADERS))) {
            List<String> values = new ArrayList<>();
            while (rs.next()) {
                values.add(asString(rs.getBytes(1)));
            }
            assertThat(values).containsExactly("application/json", "text/plain");
        }
    }

    @Test
    void headersGetAll_returnsEmptyListForAbsentKey() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT len(headers_get_all(%s, 'x-missing'))".formatted(HEADERS))) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isZero();
        }
    }

    // -----------------------------------------------------------------------
    // headers_put
    // -----------------------------------------------------------------------

    @Test
    void headersPut_increasesLengthByOne() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT len(headers_put(%s, 'x-new', 'v'::BLOB))".formatted(HEADERS))) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(4); // 3 original + 1 appended
        }
    }

    @Test
    void headersPut_appendedEntryIsRetrievable() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("""
                     SELECT headers_get(
                       headers_put(%s, 'x-custom', 'hello'::BLOB),
                       'x-custom'
                     )
                     """.formatted(HEADERS))) {
            assertThat(rs.next()).isTrue();
            assertThat(asString(rs.getBytes(1))).isEqualTo("hello");
        }
    }

    @Test
    void headersPut_doesNotModifyExistingEntries() throws Exception {
        // The first headers_get on the augmented list must still return the original value
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("""
                     SELECT headers_get(
                       headers_put(%s, 'x-extra', 'v'::BLOB),
                       'x-trace-id'
                     )
                     """.formatted(HEADERS))) {
            assertThat(rs.next()).isTrue();
            assertThat(asString(rs.getBytes(1))).isEqualTo("abc123");
        }
    }

    // -----------------------------------------------------------------------
    // headers_to_map
    // -----------------------------------------------------------------------

    @Test
    void headersToMap_lastWriteWins_forDuplicateKey() throws Exception {
        // 'content-type' appears twice; last value ('text/plain') must win
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT headers_to_map(%s)['content-type']".formatted(HEADERS))) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("text/plain");
        }
    }

    @Test
    void headersToMap_uniqueKeyIsRetained() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT headers_to_map(%s)['x-trace-id']".formatted(HEADERS))) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("abc123");
        }
    }

    @Test
    void headersToMap_mapCardinalityEqualsDistinctKeyCount() throws Exception {
        // 3 entries but only 2 distinct keys
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT cardinality(headers_to_map(%s))".formatted(HEADERS))) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(2);
        }
    }

    @Test
    void headersToMap_emptyHeadersProducesEmptyMap() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT cardinality(headers_to_map(%s))".formatted(EMPTY_HEADERS))) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isZero();
        }
    }

    // -----------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------

    private static String asString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
