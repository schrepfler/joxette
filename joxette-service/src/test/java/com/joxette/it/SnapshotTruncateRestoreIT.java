package com.joxette.it;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.EntityRecord;
import com.joxette.replay.PagedResponse;
import com.joxette.replay.SnapshotInfo;
import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the full <em>snapshot → truncate → restore</em> cycle.
 *
 * <h2>Gap closed</h2>
 * <p>The existing {@code RestoreSnapshotIT} seeds records, creates a snapshot, then inserts
 * additional rows <em>without truncating</em>, and verifies that restore discards the extras.
 * This IT closes the remaining gap by exercising the truncate path explicitly and testing
 * both the general-cassette and entity-cassette variants.
 *
 * <h2>Scenario (one parameterised variant per cassette type)</h2>
 * <ol>
 *   <li>Seed N "pre-snapshot" rows into the cassette.</li>
 *   <li>Create a named snapshot via {@code POST /cassettes/snapshots}.</li>
 *   <li>Add M "post-snapshot" rows.</li>
 *   <li>Truncate the cassette via the REST API ({@code POST /cassettes/topics/{t}/truncate}
 *       or {@code POST /cassettes/entities/{e}/truncate}).  All N+M rows are deleted.</li>
 *   <li>Restore the snapshot via {@code POST /cassettes/snapshots/{name}/restore}.</li>
 *   <li>Assert that the replay endpoint returns exactly the N pre-snapshot rows, identified
 *       by their keys, and that the M post-snapshot rows are absent.</li>
 * </ol>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
class SnapshotTruncateRestoreIT {

    @LocalServerPort int port;
    @Autowired Connection duckDB;

    private final RestTemplate rest = new RestTemplate();

    // -------------------------------------------------------------------------
    // Scenario model
    // -------------------------------------------------------------------------

    sealed interface CassetteCase permits GeneralCassetteCase, EntityCassetteCase {}

    record GeneralCassetteCase(String topic, String snapName) implements CassetteCase {
        @Override public String toString() {
            return "GeneralCassette(topic=" + topic + ")";
        }
    }

    record EntityCassetteCase(String entityType, String entityId, String topic, String snapName)
            implements CassetteCase {
        @Override public String toString() {
            return "EntityCassette(type=" + entityType + ")";
        }
    }

    static Stream<CassetteCase> cases() {
        return Stream.of(
                new GeneralCassetteCase("snap-truncate-restore-gen",   "snap-gen-it"),
                new EntityCassetteCase("snatp_ent", "ENT-SNAP-1",
                        "snap-truncate-restore-ent-src", "snap-ent-it")
        );
    }

    // -------------------------------------------------------------------------
    // Parameterised snapshot → truncate → restore cycle
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void snapshotTruncateRestore_replaysPreSnapshotRowsOnly(CassetteCase c) throws Exception {
        // Determine unique snapshot dir and clean up any remnant from a prior failed run.
        String snapName = snapName(c);
        cleanSnapshotDir(snapName);

        // ----------------------------------------------------------------
        // Step 1: seed pre-snapshot rows
        // ----------------------------------------------------------------
        seedPreSnapshot(c);
        long preCount = countRows(c);
        assertThat(preCount).isEqualTo(2);

        // ----------------------------------------------------------------
        // Step 2: create snapshot
        // ----------------------------------------------------------------
        ResponseEntity<SnapshotInfo> createResp = rest.postForEntity(
                url("/cassettes/snapshots"),
                Map.of("name", snapName),
                SnapshotInfo.class);
        assertThat(createResp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(createResp.getBody()).isNotNull();
        assertThat(createResp.getBody().name()).isEqualTo(snapName);

        // ----------------------------------------------------------------
        // Step 3: add post-snapshot rows
        // ----------------------------------------------------------------
        seedPostSnapshot(c);
        assertThat(countRows(c)).isEqualTo(4);

        // ----------------------------------------------------------------
        // Step 4: truncate via REST API — all rows removed
        // ----------------------------------------------------------------
        ResponseEntity<Map> truncResp = rest.postForEntity(
                truncateUrl(c), null, Map.class);
        assertThat(truncResp.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(countRows(c)).isZero();

        // ----------------------------------------------------------------
        // Step 5: restore snapshot
        // ----------------------------------------------------------------
        ResponseEntity<Void> restoreResp = rest.postForEntity(
                url("/cassettes/snapshots/" + snapName + "/restore"),
                null, Void.class);
        assertThat(restoreResp.getStatusCode()).isEqualTo(HttpStatus.OK);

        // ----------------------------------------------------------------
        // Step 6: verify only pre-snapshot rows are visible via replay API
        // ----------------------------------------------------------------
        verifyPreSnapshotRecords(c);
    }

    // -------------------------------------------------------------------------
    // Per-case helpers
    // -------------------------------------------------------------------------

    private static String snapName(CassetteCase c) {
        return switch (c) {
            case GeneralCassetteCase g -> g.snapName();
            case EntityCassetteCase e -> e.snapName();
        };
    }

    private void seedPreSnapshot(CassetteCase c) throws Exception {
        Instant ts = Instant.parse("2025-02-01T08:00:00Z");
        switch (c) {
            case GeneralCassetteCase g -> {
                DuckDBTestSupport.createGeneralCassetteTable(duckDB, g.topic());
                clearGeneralTable(g.topic());
                DuckDBTestSupport.insertCassetteRow(duckDB, g.topic(), 0, 0L,
                        ts, Instant.now(), "pre-key-1", bytes("pre-v1"));
                DuckDBTestSupport.insertCassetteRow(duckDB, g.topic(), 0, 1L,
                        ts.plusSeconds(1), Instant.now(), "pre-key-2", bytes("pre-v2"));
            }
            case EntityCassetteCase e -> {
                DuckDBTestSupport.createEntityTable(duckDB, e.entityType());
                clearEntityTable(e.entityType());
                DuckDBTestSupport.insertEntityRow(duckDB, e.entityType(), e.entityId(),
                        1, "ev", e.topic(), 0, 0L, ts, Instant.now(), "pre-ek1", bytes("pre-ev1"));
                DuckDBTestSupport.insertEntityRow(duckDB, e.entityType(), e.entityId(),
                        1, "ev", e.topic(), 0, 1L, ts.plusSeconds(1), Instant.now(), "pre-ek2", bytes("pre-ev2"));
            }
        }
    }

    private void seedPostSnapshot(CassetteCase c) throws Exception {
        Instant ts = Instant.parse("2025-02-01T09:00:00Z");
        switch (c) {
            case GeneralCassetteCase g -> {
                DuckDBTestSupport.insertCassetteRow(duckDB, g.topic(), 0, 2L,
                        ts, Instant.now(), "post-key-3", bytes("post-v3"));
                DuckDBTestSupport.insertCassetteRow(duckDB, g.topic(), 0, 3L,
                        ts.plusSeconds(1), Instant.now(), "post-key-4", bytes("post-v4"));
            }
            case EntityCassetteCase e -> {
                DuckDBTestSupport.insertEntityRow(duckDB, e.entityType(), e.entityId(),
                        1, "ev", e.topic(), 0, 2L, ts, Instant.now(), "post-ek3", bytes("post-ev3"));
                DuckDBTestSupport.insertEntityRow(duckDB, e.entityType(), e.entityId(),
                        1, "ev", e.topic(), 0, 3L, ts.plusSeconds(1), Instant.now(), "post-ek4", bytes("post-ev4"));
            }
        }
    }

    private long countRows(CassetteCase c) throws Exception {
        return switch (c) {
            case GeneralCassetteCase g ->
                    DuckDBTestSupport.countRows(duckDB,
                            "lake.main.general_" + normalise(g.topic()));
            case EntityCassetteCase e ->
                    DuckDBTestSupport.countRows(duckDB,
                            "lake.main.entity_" + e.entityType());
        };
    }

    private String truncateUrl(CassetteCase c) {
        return switch (c) {
            case GeneralCassetteCase g ->
                    url("/cassettes/topics/" + g.topic() + "/truncate");
            case EntityCassetteCase e ->
                    url("/cassettes/entities/" + e.entityType() + "/truncate");
        };
    }

    private void verifyPreSnapshotRecords(CassetteCase c) {
        switch (c) {
            case GeneralCassetteCase g -> verifyGeneralPreSnapshot(g);
            case EntityCassetteCase e -> verifyEntityPreSnapshot(e);
        }
    }

    private void verifyGeneralPreSnapshot(GeneralCassetteCase g) {
        ResponseEntity<PagedResponse<CassetteRecord>> resp = rest.exchange(
                url("/cassettes/topics/" + g.topic()),
                HttpMethod.GET, null,
                new ParameterizedTypeReference<>() {});
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        List<CassetteRecord> records = resp.getBody().data();
        assertThat(records).hasSize(2);
        assertThat(records).extracting(CassetteRecord::key)
                .containsExactlyInAnyOrder("pre-key-1", "pre-key-2");
        assertThat(records).extracting(CassetteRecord::offset)
                .doesNotContain(2L, 3L);
    }

    private void verifyEntityPreSnapshot(EntityCassetteCase e) {
        ResponseEntity<PagedResponse<EntityRecord>> resp = rest.exchange(
                url("/cassettes/entities/" + e.entityType() + "/" + e.entityId()),
                HttpMethod.GET, null,
                new ParameterizedTypeReference<>() {});
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        List<EntityRecord> records = resp.getBody().data();
        assertThat(records).hasSize(2);
        assertThat(records).extracting(EntityRecord::key)
                .containsExactlyInAnyOrder("pre-ek1", "pre-ek2");
        assertThat(records).extracting(EntityRecord::offset)
                .doesNotContain(2L, 3L);
    }

    // -------------------------------------------------------------------------
    // Utilities
    // -------------------------------------------------------------------------

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String normalise(String topic) {
        return topic.toLowerCase().replaceAll("[^a-z0-9_]", "_");
    }

    private void clearGeneralTable(String topic) throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("DELETE FROM lake.main.general_" + normalise(topic));
        }
    }

    private void clearEntityTable(String entityType) throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("DELETE FROM lake.main.entity_" + entityType);
        }
    }

    private static void cleanSnapshotDir(String name) throws IOException {
        Path dir = Path.of("snapshots", name);
        if (!Files.exists(dir)) return;
        try (var walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try { Files.delete(p); } catch (IOException e) { throw new UncheckedIOException(e); }
            });
        }
    }
}
