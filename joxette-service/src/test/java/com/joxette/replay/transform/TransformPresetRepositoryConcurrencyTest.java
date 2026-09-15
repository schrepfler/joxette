package com.joxette.replay.transform;

import tools.jackson.databind.ObjectMapper;
import com.joxette.support.DuckDBTestSupport;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers finding I4: {@link TransformPresetRepository} shares the same DuckDB
 * connection/{@code DSLContext} bean as the replay read paths (Task A), but had
 * zero {@code synchronized(duckDB)} blocks — concurrent access (e.g. one request's
 * {@code ?transform_preset=} lookup racing another's) could corrupt or error against
 * the single native handle. Proves real concurrent create/read no longer throws or
 * loses rows.
 */
class TransformPresetRepositoryConcurrencyTest {

    private Connection conn;
    private TransformPresetRepository repo;

    @BeforeEach
    void setUp() throws Exception {
        conn = DuckDBTestSupport.newConnection();
        repo = new TransformPresetRepository(DSL.using(conn, SQLDialect.DUCKDB), new ObjectMapper(), conn);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null && !conn.isClosed()) conn.close();
    }

    @Test
    void concurrentCreatesAndReads_doNotThrowOrCorrupt() throws Exception {
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<TransformPreset>> creates = IntStream.range(0, threads)
                    .<Future<TransformPreset>>mapToObj(i -> pool.submit(() ->
                            repo.create("preset-" + i, "Preset " + i, List.of())))
                    .toList();
            for (Future<TransformPreset> f : creates) {
                assertThat(f.get(10, TimeUnit.SECONDS)).isNotNull();
            }

            List<Future<List<TransformPreset>>> reads = IntStream.range(0, threads)
                    .<Future<List<TransformPreset>>>mapToObj(i -> pool.submit(repo::listAll))
                    .toList();
            for (Future<List<TransformPreset>> f : reads) {
                assertThat(f.get(10, TimeUnit.SECONDS)).hasSize(threads);
            }
        } finally {
            pool.shutdown();
            assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(repo.listAll()).hasSize(threads);
    }
}
