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
 * Stress test mirroring {@link EntityReplayServiceConcurrencyTest} for the general-cassette
 * read path. See that class's javadoc for why this is a probabilistic (not deterministic)
 * regression test: run 5 times to observe a pre-fix failure.
 */
@ExtendWith(CursorSigningKeyTestExtension.class)
class TopicReplayServiceConcurrencyTest {

    private static final String TOPIC = "orders.events";
    private static final int ROWS = 40;
    private static final int THREAD_COUNT = 16;
    private static final int QUERIES_PER_THREAD = 25;

    private Connection duckDB;
    private TopicReplayService service;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC);
        Instant base = Instant.parse("2025-01-01T00:00:00Z");
        for (int i = 0; i < ROWS; i++) {
            DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, i, base.plusSeconds(i), base.plusSeconds(i),
                    "key-" + i, ("{\"seq\":" + i + "}").getBytes());
        }
        service = new TopicReplayService(DSL.using(duckDB, SQLDialect.DUCKDB), duckDB);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
    }

    @Test
    void concurrentQuery_neverThrowsOrCorrupts() throws Exception {
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Integer> rowCounts = new ConcurrentLinkedQueue<>();

        List<Callable<Void>> tasks = new ArrayList<>();
        for (int t = 0; t < THREAD_COUNT; t++) {
            tasks.add(() -> {
                for (int i = 0; i < QUERIES_PER_THREAD; i++) {
                    try {
                        PagedResponse<CassetteRecord> page = service.query(
                                TOPIC, null, null, null, null, null, ROWS, null);
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
                .allMatch(count -> count == ROWS);
    }
}
