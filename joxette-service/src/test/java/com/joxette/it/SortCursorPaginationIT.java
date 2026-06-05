package com.joxette.it;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.EntityRecord;
import com.joxette.replay.PagedResponse;
import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests verifying that cursor-based pagination preserves correct sort
 * order across multiple page fetches for both general and entity cassettes.
 *
 * <h2>Gap closed</h2>
 * <p>The core edge case: when multiple records share an identical {@code kafka_timestamp}
 * and a page boundary falls mid-timestamp (some, but not all, same-timestamp records are
 * on page N), the cursor encoded from the last record of page N must seek correctly to the
 * first unseen record of page N+1.  Without a tie-breaker in the cursor, the next page
 * would either repeat records from page N (off-by-one) or skip records (off-by-one
 * in the other direction).
 *
 * <h2>Scenarios</h2>
 * <ul>
 *   <li><b>General cassette</b>: 6 records across 3 pages, records 2–5 share the same
 *       {@code kafka_timestamp}.  Page boundary falls between record 3 and 4 (mid-tie).</li>
 *   <li><b>Entity cassette</b>: same data shape, 6 events for one entity, same-timestamp
 *       tie broken by {@code (sourceTopic, sourcePartition, sourceOffset)}.</li>
 * </ul>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
class SortCursorPaginationIT {

    private static final String GENERAL_TOPIC  = "sort-cursor-general";
    private static final String ENTITY_TOPIC   = "sort-cursor-entity-src";
    private static final String ENTITY_TYPE    = "sortcursor";
    private static final String ENTITY_ID      = "ENT-SORT-1";

    // T0 is the unique first record; T1 is shared by records 2–5; T2 is unique last record.
    private static final Instant T0 = Instant.parse("2025-03-01T10:00:00Z");
    private static final Instant T_TIE = Instant.parse("2025-03-01T10:01:00Z");  // shared by 4 records
    private static final Instant T2 = Instant.parse("2025-03-01T10:02:00Z");

    @LocalServerPort int port;
    @Autowired Connection duckDB;

    private final RestTemplate rest = new RestTemplate();

    // -------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------

    @BeforeEach
    void seed() throws Exception {
        // --- General cassette ---
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, GENERAL_TOPIC);
        try (Statement st = duckDB.createStatement()) {
            st.execute("DELETE FROM lake.main.general_sort_cursor_general");
        }

        // 6 rows: offset 0 → T0; offsets 1–4 → T_TIE (same timestamp, different offset); offset 5 → T2
        Instant now = Instant.now();
        DuckDBTestSupport.insertCassetteRow(duckDB, GENERAL_TOPIC, 0, 0L, T0,      now, "k0", bytes("v0"));
        DuckDBTestSupport.insertCassetteRow(duckDB, GENERAL_TOPIC, 0, 1L, T_TIE,   now, "k1", bytes("v1"));
        DuckDBTestSupport.insertCassetteRow(duckDB, GENERAL_TOPIC, 0, 2L, T_TIE,   now, "k2", bytes("v2"));
        DuckDBTestSupport.insertCassetteRow(duckDB, GENERAL_TOPIC, 0, 3L, T_TIE,   now, "k3", bytes("v3"));
        DuckDBTestSupport.insertCassetteRow(duckDB, GENERAL_TOPIC, 0, 4L, T_TIE,   now, "k4", bytes("v4"));
        DuckDBTestSupport.insertCassetteRow(duckDB, GENERAL_TOPIC, 0, 5L, T2,      now, "k5", bytes("v5"));

        // --- Entity cassette ---
        DuckDBTestSupport.createEntityTable(duckDB, ENTITY_TYPE);
        try (Statement st = duckDB.createStatement()) {
            st.execute("DELETE FROM lake.main.entity_" + ENTITY_TYPE);
        }

        // 6 entity rows: same timestamp tie pattern; source_offset 0–5, same-timestamp group is 1–4
        DuckDBTestSupport.insertEntityRow(duckDB, ENTITY_TYPE, ENTITY_ID, 1, "evt",
                ENTITY_TOPIC, 0, 0L, T0,    now, "ek0", bytes("ev0"));
        DuckDBTestSupport.insertEntityRow(duckDB, ENTITY_TYPE, ENTITY_ID, 1, "evt",
                ENTITY_TOPIC, 0, 1L, T_TIE, now, "ek1", bytes("ev1"));
        DuckDBTestSupport.insertEntityRow(duckDB, ENTITY_TYPE, ENTITY_ID, 1, "evt",
                ENTITY_TOPIC, 0, 2L, T_TIE, now, "ek2", bytes("ev2"));
        DuckDBTestSupport.insertEntityRow(duckDB, ENTITY_TYPE, ENTITY_ID, 1, "evt",
                ENTITY_TOPIC, 0, 3L, T_TIE, now, "ek3", bytes("ev3"));
        DuckDBTestSupport.insertEntityRow(duckDB, ENTITY_TYPE, ENTITY_ID, 1, "evt",
                ENTITY_TOPIC, 0, 4L, T_TIE, now, "ek4", bytes("ev4"));
        DuckDBTestSupport.insertEntityRow(duckDB, ENTITY_TYPE, ENTITY_ID, 1, "evt",
                ENTITY_TOPIC, 0, 5L, T2,    now, "ek5", bytes("ev5"));
    }

    // -------------------------------------------------------------------------
    // General cassette — parameterised page-size scenarios
    // -------------------------------------------------------------------------

    record GeneralCase(int pageSize, String label) {
        @Override public String toString() { return label; }
    }

    static Stream<GeneralCase> generalCases() {
        return Stream.of(
                new GeneralCase(2, "pageSize=2 (boundary splits the tie group 1–4)"),
                new GeneralCase(3, "pageSize=3 (boundary falls at end of tie group)"),
                new GeneralCase(1, "pageSize=1 (every boundary is mid-tie)")
        );
    }

    @ParameterizedTest(name = "general cassette: {0}")
    @MethodSource("generalCases")
    void generalCassette_cursorPaginationPreservesSortOrder_acrossTimestampTies(GeneralCase c) {
        List<Long> collectedOffsets = new ArrayList<>();
        String cursor = null;
        int pages = 0;

        do {
            String url = baseUrl() + "/cassettes/topics/" + GENERAL_TOPIC
                    + "?limit=" + c.pageSize()
                    + (cursor != null ? "&cursor=" + cursor : "");
            ResponseEntity<PagedResponse<CassetteRecord>> resp = rest.exchange(
                    url, HttpMethod.GET, null, new ParameterizedTypeReference<>() {});

            assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
            PagedResponse<CassetteRecord> page = resp.getBody();
            assertThat(page).isNotNull();
            assertThat(page.data()).isNotEmpty();

            page.data().forEach(r -> collectedOffsets.add(r.offset()));
            cursor = page.hasMore() ? page.nextCursor() : null;
            pages++;
        } while (cursor != null);

        // All 6 records, in order, no duplicates, no gaps.
        assertThat(collectedOffsets)
                .as("total records collected over %d pages (pageSize=%d)", pages, c.pageSize())
                .containsExactly(0L, 1L, 2L, 3L, 4L, 5L);

        assertThat(collectedOffsets)
                .as("no duplicates")
                .doesNotHaveDuplicates();
    }

    // -------------------------------------------------------------------------
    // Entity cassette — parameterised page-size scenarios
    // -------------------------------------------------------------------------

    record EntityCase(int pageSize, String label) {
        @Override public String toString() { return label; }
    }

    static Stream<EntityCase> entityCases() {
        return Stream.of(
                new EntityCase(2, "pageSize=2 (boundary splits entity tie group)"),
                new EntityCase(3, "pageSize=3 (boundary at end of entity tie group)"),
                new EntityCase(1, "pageSize=1 (every boundary mid-tie)")
        );
    }

    @ParameterizedTest(name = "entity cassette: {0}")
    @MethodSource("entityCases")
    void entityCassette_cursorPaginationPreservesSortOrder_acrossTimestampTies(EntityCase c) {
        List<Long> collectedOffsets = new ArrayList<>();
        String cursor = null;

        do {
            String url = baseUrl() + "/cassettes/entities/" + ENTITY_TYPE + "/" + ENTITY_ID
                    + "?limit=" + c.pageSize()
                    + (cursor != null ? "&cursor=" + cursor : "");
            ResponseEntity<PagedResponse<EntityRecord>> resp = rest.exchange(
                    url, HttpMethod.GET, null, new ParameterizedTypeReference<>() {});

            assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
            PagedResponse<EntityRecord> page = resp.getBody();
            assertThat(page).isNotNull();
            assertThat(page.data()).isNotEmpty();

            page.data().forEach(r -> collectedOffsets.add(r.offset()));
            cursor = page.hasMore() ? page.nextCursor() : null;
        } while (cursor != null);

        assertThat(collectedOffsets)
                .as("all 6 entity events collected in offset order (pageSize=%d)", c.pageSize())
                .containsExactly(0L, 1L, 2L, 3L, 4L, 5L);

        assertThat(collectedOffsets)
                .as("no duplicates")
                .doesNotHaveDuplicates();
    }

    // -------------------------------------------------------------------------
    // Descending order — cursor must seek backwards correctly across ties
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "general cassette DESC: {0}")
    @MethodSource("generalCases")
    void generalCassette_descendingCursorPaginationPreservesSortOrder(GeneralCase c) {
        List<Long> collectedOffsets = new ArrayList<>();
        String cursor = null;

        do {
            String url = baseUrl() + "/cassettes/topics/" + GENERAL_TOPIC
                    + "?limit=" + c.pageSize() + "&order=desc"
                    + (cursor != null ? "&cursor=" + cursor : "");
            ResponseEntity<PagedResponse<CassetteRecord>> resp = rest.exchange(
                    url, HttpMethod.GET, null, new ParameterizedTypeReference<>() {});

            assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
            PagedResponse<CassetteRecord> page = resp.getBody();
            assertThat(page).isNotNull();
            page.data().forEach(r -> collectedOffsets.add(r.offset()));
            cursor = page.hasMore() ? page.nextCursor() : null;
        } while (cursor != null);

        // Descending by (timestamp DESC, partition DESC, offset DESC): 5, 4, 3, 2, 1, 0
        assertThat(collectedOffsets)
                .as("DESC order, all records, no gaps, no duplicates (pageSize=%d)", c.pageSize())
                .containsExactly(5L, 4L, 3L, 2L, 1L, 0L);

        assertThat(collectedOffsets).doesNotHaveDuplicates();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private String baseUrl() {
        return "http://localhost:" + port;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
