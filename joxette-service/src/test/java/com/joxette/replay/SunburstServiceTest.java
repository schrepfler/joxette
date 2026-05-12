package com.joxette.replay;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link SunburstService}.
 *
 * <p>Uses Mockito to stub {@link EntityReplayService}. Tests focus on:
 * <ul>
 *   <li>Prefix-tree construction from entity sequences</li>
 *   <li>SOL pre-filter — sequences that don't match are excluded</li>
 *   <li>maxEntities cap</li>
 *   <li>Empty-sequence entities are skipped</li>
 * </ul>
 */
class SunburstServiceTest {

    private EntityReplayService entityService;
    private SunburstService     sunburstService;

    @BeforeEach
    void setUp() {
        entityService    = mock(EntityReplayService.class);
        sunburstService  = new SunburstService(entityService);
    }

    // -------------------------------------------------------------------------
    // Basic prefix-tree construction
    // -------------------------------------------------------------------------

    @Test
    void build_noSolFilter_includesAllEntities() throws Exception {
        stubEntities("order", List.of("E1", "E2"));
        stubEvents("order", "E1", List.of("login", "purchase"));
        stubEvents("order", "E2", List.of("login", "logout"));

        var resp = sunburstService.build("order", req(null));

        assertThat(resp.totalSequences()).isEqualTo(2);
        assertThat(resp.root().nodeCount()).isEqualTo(2);
        // Both start with "login"
        assertThat(resp.root().children()).hasSize(1);
        assertThat(resp.root().children().get(0).name()).isEqualTo("login");
        assertThat(resp.root().children().get(0).nodeCount()).isEqualTo(2);
    }

    @Test
    void build_noSolFilter_buildsCorrectHierarchy() throws Exception {
        stubEntities("order", List.of("E1", "E2", "E3"));
        stubEvents("order", "E1", List.of("A", "B", "C"));
        stubEvents("order", "E2", List.of("A", "B", "D"));
        stubEvents("order", "E3", List.of("A", "X"));

        var resp = sunburstService.build("order", req(null));

        assertThat(resp.totalSequences()).isEqualTo(3);
        var aNode = resp.root().children().get(0);
        assertThat(aNode.name()).isEqualTo("A");
        assertThat(aNode.nodeCount()).isEqualTo(3);
        // A→B should have 2, A→X should have 1
        var aChildren = aNode.children();
        assertThat(aChildren).hasSize(2);
        // Most common first
        assertThat(aChildren.get(0).name()).isEqualTo("B");
        assertThat(aChildren.get(0).nodeCount()).isEqualTo(2);
        assertThat(aChildren.get(1).name()).isEqualTo("X");
        assertThat(aChildren.get(1).nodeCount()).isEqualTo(1);
    }

    @Test
    void build_emptySequenceEntities_areSkipped() throws Exception {
        stubEntities("order", List.of("E1", "EMPTY"));
        stubEvents("order", "E1",    List.of("login"));
        stubEvents("order", "EMPTY", List.of());           // no events

        var resp = sunburstService.build("order", req(null));

        assertThat(resp.totalSequences()).isEqualTo(1);
    }

    @Test
    void build_respectsMaxEntities() throws Exception {
        stubEntities("order", List.of("E1", "E2", "E3", "E4", "E5"));
        for (var id : List.of("E1", "E2", "E3", "E4", "E5")) {
            stubEvents("order", id, List.of("event"));
        }

        var req = new SunburstService.SunburstRequest(null, null, 5, null, 3, null);
        var resp = sunburstService.build("order", req);

        assertThat(resp.totalSequences()).isEqualTo(3);
    }

    // -------------------------------------------------------------------------
    // SOL pre-filter
    // -------------------------------------------------------------------------

    @Test
    void build_solFilter_excludesNonMatchingSequences() throws Exception {
        stubEntities("order", List.of("MATCH", "NOMATCH"));
        // MATCH:   login → purchase  — matches "match A(login) >> * >> B(purchase)"
        stubEvents("order", "MATCH",   List.of("login", "browse", "purchase"));
        // NOMATCH: login → logout only — no purchase
        stubEvents("order", "NOMATCH", List.of("login", "logout"));

        var resp = sunburstService.build("order",
                req("match A(login) >> * >> B(purchase)"));

        assertThat(resp.totalSequences()).isEqualTo(1);
        // Only MATCH contributed — tree has login → browse → purchase
        var loginNode = resp.root().children().get(0);
        assertThat(loginNode.name()).isEqualTo("login");
        assertThat(loginNode.nodeCount()).isEqualTo(1);
    }

    @Test
    void build_solFilter_allMatch_includesAll() throws Exception {
        stubEntities("order", List.of("E1", "E2"));
        stubEvents("order", "E1", List.of("login", "purchase"));
        stubEvents("order", "E2", List.of("login", "purchase"));

        var resp = sunburstService.build("order",
                req("match A(login) >> * >> B(purchase)"));

        assertThat(resp.totalSequences()).isEqualTo(2);
    }

    @Test
    void build_solFilter_noneMatch_returnsEmptyTree() throws Exception {
        stubEntities("order", List.of("E1", "E2"));
        stubEvents("order", "E1", List.of("browse", "logout"));
        stubEvents("order", "E2", List.of("browse", "logout"));

        var resp = sunburstService.build("order",
                req("match A(login) >> * >> B(purchase)"));

        assertThat(resp.totalSequences()).isEqualTo(0);
        assertThat(resp.root().nodeCount()).isEqualTo(0);
    }

    @Test
    void build_solFilter_blankQuery_treatedAsNoFilter() throws Exception {
        stubEntities("order", List.of("E1"));
        stubEvents("order", "E1", List.of("login"));

        var resp = sunburstService.build("order",
                req("   "));  // blank, not null

        assertThat(resp.totalSequences()).isEqualTo(1);
    }

    @Test
    void build_eventNames_collectsDistinctNamesFromTree() throws Exception {
        stubEntities("order", List.of("E1", "E2"));
        stubEvents("order", "E1", List.of("login", "purchase"));
        stubEvents("order", "E2", List.of("login", "logout"));

        var resp = sunburstService.build("order", req(null));

        assertThat(resp.eventNames()).containsExactlyInAnyOrder("login", "logout", "purchase");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static SunburstService.SunburstRequest req(String solQuery) {
        return new SunburstService.SunburstRequest(null, null, 10, null, 500, solQuery);
    }

    /** Stubs listEntities to return a single page with the given entity IDs. */
    @SuppressWarnings("unchecked")
    private void stubEntities(String entityType, List<String> ids) throws SQLException {
        List<EntityInfo> infos = ids.stream()
                .map(id -> new EntityInfo(entityType, id,
                        Instant.parse("2024-01-01T00:00:00Z"),
                        Instant.parse("2024-01-01T00:00:00Z"),
                        1L, List.of(), null))
                .toList();
        PagedResponse<EntityInfo> page = new PagedResponse<>(infos, null, false, null);
        when(entityService.listEntities(eq(entityType), anyInt(), isNull(),
                eq(EntityReplayService.EntitySortBy.id)))
                .thenReturn(page);
    }

    /**
     * Stubs streamEntityEvents so that the given event names are delivered to the
     * consumer as minimal {@link EntityRecord}s.
     */
    private void stubEvents(String entityType, String entityId, List<String> eventNames)
            throws SQLException {
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            java.util.function.Consumer<EntityRecord> sink = inv.getArgument(4);
            Instant base = Instant.parse("2024-01-01T10:00:00Z");
            for (int i = 0; i < eventNames.size(); i++) {
                sink.accept(new EntityRecord(entityId, eventNames.get(i),
                        "topic", 0, i, base.plusSeconds(i), base.plusSeconds(i),
                        null, null, null));
            }
            return null;
        }).when(entityService).streamEntityEvents(
                eq(entityType), eq(entityId), isNull(), isNull(),
                any());
    }
}
