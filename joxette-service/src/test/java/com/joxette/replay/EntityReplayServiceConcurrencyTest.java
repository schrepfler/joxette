package com.joxette.replay;

import com.joxette.support.DuckDBTestSupport;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Connection;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stress test proving that concurrent reads through {@link EntityReplayService} do not
 * corrupt the shared DuckDB {@link Connection}. DuckDB's JDBC driver wraps a single native
 * {@code duckdb_connection} handle that is not thread-safe for concurrent {@code Statement}
 * execution regardless of read/write — see CLAUDE.md "DuckDB as Catalog".
 *
 * <p><b>This is a probabilistic regression test, not a deterministic one.</b> Without
 * {@code synchronized(duckDB)} around every query, concurrent access to the native handle
 * races unpredictably: it may throw a {@link java.sql.SQLException}, return a corrupted
 * row count, or (rarely) pass cleanly on a given run. Run this test 5 times before applying
 * the Step 3 fix — at least one of the five runs is expected to fail. After the fix, every
 * run is deterministic (no lock contention window remains to race on).
 */
@ExtendWith(CursorSigningKeyTestExtension.class)
class EntityReplayServiceConcurrencyTest {

    private static final String ENTITY_TYPE = "order";
    private static final int ROWS_PER_ENTITY = 40;
    private static final int THREAD_COUNT = 16;
    private static final int QUERIES_PER_THREAD = 25;

    private Connection duckDB;
    private EntityReplayService service;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createEntityTable(duckDB, ENTITY_TYPE);
        Instant base = Instant.parse("2025-01-01T00:00:00Z");
        for (int i = 0; i < ROWS_PER_ENTITY; i++) {
            DuckDBTestSupport.insertEntityRow(duckDB, ENTITY_TYPE, "cust-1", 0, "OrderCreated",
                    "orders.events", 0, i, base.plusSeconds(i), base.plusSeconds(i),
                    "cust-1", ("{\"seq\":" + i + "}").getBytes());
        }
        service = new EntityReplayService(DSL.using(duckDB, SQLDialect.DUCKDB), duckDB);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
    }

    @Test
    void concurrentQueryEntityEvents_neverThrowsOrCorrupts() throws Exception {
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Integer> rowCounts = new ConcurrentLinkedQueue<>();

        List<Callable<Void>> tasks = new ArrayList<>();
        for (int t = 0; t < THREAD_COUNT; t++) {
            tasks.add(() -> {
                for (int i = 0; i < QUERIES_PER_THREAD; i++) {
                    try {
                        PagedResponse<EntityRecord> page = service.queryEntityEvents(
                                ENTITY_TYPE, "cust-1", null, null, ROWS_PER_ENTITY, null);
                        rowCounts.add(page.data().size());
                    } catch (Throwable ex) {
                        failures.add(ex);
                    }
                }
                return null;
            });
        }

        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        List<Future<Void>> futures = pool.invokeAll(tasks);
        for (Future<Void> f : futures) {
            f.get();
        }
        pool.shutdown();

        assertThat(failures).as("no thread should observe an exception from the shared DuckDB connection").isEmpty();
        assertThat(rowCounts).as("every concurrent query must see the full, uncorrupted row set")
                .allMatch(count -> count == ROWS_PER_ENTITY);
    }
}
