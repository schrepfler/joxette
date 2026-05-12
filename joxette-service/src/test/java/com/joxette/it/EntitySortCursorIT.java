package com.joxette.it;

import com.joxette.replay.EntityInfo;
import com.joxette.replay.PagedResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.client.RestTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for entity listing sort orders and compound-cursor pagination.
 *
 * <p>Verifies that {@code GET /cassettes/entities/{type}?sortBy=lastActive|mostMessages}
 * returns entities in the correct order across multiple pages, and that the cursor
 * produced on page N correctly seeks into page N+1.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
class EntitySortCursorIT {

    private static final String ENTITY_TYPE = "sorttest";

    @LocalServerPort int port;
    @Autowired Connection duckDB;

    private final RestTemplate rest = new RestTemplate();

    @BeforeEach
    void seed() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("""
                    INSERT INTO entity_type_configs (entity_type, bucket_count, created_at)
                    VALUES ('sorttest', 256, now())
                    ON CONFLICT (entity_type) DO NOTHING
                    """);
            st.execute("DELETE FROM known_entities WHERE entity_type = 'sorttest'");
        }

        // 5 entities with distinct last_seen and message_count values
        //   id   last_seen               message_count
        //   E1   2024-12-05               50
        //   E2   2024-12-04               200
        //   E3   2024-12-03               10
        //   E4   2024-12-02               150
        //   E5   2024-12-01               75
        insert("E1", "2024-01-01T00:00:00Z", "2024-12-05T00:00:00Z", 50);
        insert("E2", "2024-01-01T00:00:00Z", "2024-12-04T00:00:00Z", 200);
        insert("E3", "2024-01-01T00:00:00Z", "2024-12-03T00:00:00Z", 10);
        insert("E4", "2024-01-01T00:00:00Z", "2024-12-02T00:00:00Z", 150);
        insert("E5", "2024-01-01T00:00:00Z", "2024-12-01T00:00:00Z", 75);
    }

    // -------------------------------------------------------------------------
    // lastActive sort
    // -------------------------------------------------------------------------

    @Test
    void lastActive_returnsAllEntitiesInDescendingLastSeenOrder() {
        List<String> all = fetchAll("lastActive");
        assertThat(all).containsExactly("E1", "E2", "E3", "E4", "E5");
    }

    @Test
    void lastActive_paginatesCorrectlyAcrossPages() {
        // Page 1: limit=2 → E1, E2
        PagedResponse<EntityInfo> page1 = fetch("lastActive", 2, null);
        assertThat(page1.data()).extracting(EntityInfo::entityId).containsExactly("E1", "E2");
        assertThat(page1.hasMore()).isTrue();

        // Page 2: → E3, E4
        PagedResponse<EntityInfo> page2 = fetch("lastActive", 2, page1.nextCursor());
        assertThat(page2.data()).extracting(EntityInfo::entityId).containsExactly("E3", "E4");
        assertThat(page2.hasMore()).isTrue();

        // Page 3: → E5 (last)
        PagedResponse<EntityInfo> page3 = fetch("lastActive", 2, page2.nextCursor());
        assertThat(page3.data()).extracting(EntityInfo::entityId).containsExactly("E5");
        assertThat(page3.hasMore()).isFalse();
    }

    @Test
    void lastActive_noCursorGap_allEntitiesCollectedWithoutDuplicates() {
        List<String> all = fetchAll("lastActive");
        assertThat(all).hasSize(5);
        assertThat(all).doesNotHaveDuplicates();
    }

    // -------------------------------------------------------------------------
    // mostMessages sort
    // -------------------------------------------------------------------------

    @Test
    void mostMessages_returnsAllEntitiesInDescendingCountOrder() {
        // E2(200) > E4(150) > E5(75) > E1(50) > E3(10)
        List<String> all = fetchAll("mostMessages");
        assertThat(all).containsExactly("E2", "E4", "E5", "E1", "E3");
    }

    @Test
    void mostMessages_paginatesCorrectlyAcrossPages() {
        PagedResponse<EntityInfo> page1 = fetch("mostMessages", 2, null);
        assertThat(page1.data()).extracting(EntityInfo::entityId).containsExactly("E2", "E4");
        assertThat(page1.hasMore()).isTrue();

        PagedResponse<EntityInfo> page2 = fetch("mostMessages", 2, page1.nextCursor());
        assertThat(page2.data()).extracting(EntityInfo::entityId).containsExactly("E5", "E1");
        assertThat(page2.hasMore()).isTrue();

        PagedResponse<EntityInfo> page3 = fetch("mostMessages", 2, page2.nextCursor());
        assertThat(page3.data()).extracting(EntityInfo::entityId).containsExactly("E3");
        assertThat(page3.hasMore()).isFalse();
    }

    @Test
    void mostMessages_noCursorGap_allEntitiesCollectedWithoutDuplicates() {
        List<String> all = fetchAll("mostMessages");
        assertThat(all).hasSize(5);
        assertThat(all).doesNotHaveDuplicates();
    }

    // -------------------------------------------------------------------------
    // id sort (baseline)
    // -------------------------------------------------------------------------

    @Test
    void idSort_returnsAlphabeticalOrder() {
        List<String> all = fetchAll("id");
        assertThat(all).containsExactly("E1", "E2", "E3", "E4", "E5");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private List<String> fetchAll(String sortBy) {
        List<String> ids = new ArrayList<>();
        String cursor = null;
        do {
            PagedResponse<EntityInfo> page = fetch(sortBy, 2, cursor);
            page.data().forEach(e -> ids.add(e.entityId()));
            cursor = page.hasMore() ? page.nextCursor() : null;
        } while (cursor != null);
        return ids;
    }

    private PagedResponse<EntityInfo> fetch(String sortBy, int limit, String cursor) {
        String url = "http://localhost:" + port
                + "/cassettes/entities/" + ENTITY_TYPE
                + "?sortBy=" + sortBy
                + "&limit=" + limit
                + (cursor != null ? "&cursor=" + cursor : "");
        ResponseEntity<PagedResponse<EntityInfo>> resp = rest.exchange(
                url, HttpMethod.GET, null,
                new ParameterizedTypeReference<>() {});
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        return resp.getBody();
    }

    private void insert(String entityId, String firstSeen, String lastSeen, long messageCount)
            throws Exception {
        try (PreparedStatement ps = duckDB.prepareStatement("""
                INSERT INTO known_entities
                    (entity_type, entity_id, first_seen, last_seen, message_count)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """)) {
            ps.setString(1, ENTITY_TYPE);
            ps.setString(2, entityId);
            ps.setTimestamp(3, Timestamp.from(Instant.parse(firstSeen)));
            ps.setTimestamp(4, Timestamp.from(Instant.parse(lastSeen)));
            ps.setLong(5, messageCount);
            ps.executeUpdate();
        }
    }
}
