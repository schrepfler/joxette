package com.joxette.replay;

import com.joxette.support.DuckDBTestSupport;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers finding I4: {@link FieldSuggestionsService} shares the same DuckDB
 * connection/{@code DSLContext} bean as the replay read paths (Task A), but had
 * zero {@code synchronized(duckDB)} blocks around its {@code dsl.fetch(sql)} calls
 * (used for both the recursive/shallow field-path CTEs and the message-type
 * query) — concurrent field-suggestion requests could race the single native
 * DuckDB handle. Proves concurrent calls no longer throw.
 */
class FieldSuggestionsServiceConcurrencyTest {

    private Connection conn;
    private FieldSuggestionsService service;

    @BeforeEach
    void setUp() throws Exception {
        conn = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createGeneralCassetteTable(conn, "orders");
        for (int i = 0; i < 20; i++) {
            DuckDBTestSupport.insertCassetteRow(conn, "orders", 0, i,
                    Instant.now(), Instant.now(), "key-" + i,
                    ("{\"order_id\":\"" + i + "\"}").getBytes(), "OrderCreated");
        }
        service = new FieldSuggestionsService(DSL.using(conn, SQLDialect.DUCKDB), conn);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null && !conn.isClosed()) conn.close();
    }

    @Test
    void concurrentFieldAndMessageTypeQueries_doNotThrow() throws Exception {
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<List<String>>> futures = IntStream.range(0, threads)
                    .<Future<List<String>>>mapToObj(i -> pool.submit(() -> i % 2 == 0
                            ? service.forTopic("orders", 50)
                            : service.messageTypesForTopic("orders", 50)))
                    .toList();
            for (Future<List<String>> f : futures) {
                assertThat(f.get(10, TimeUnit.SECONDS)).isNotNull();
            }
        } finally {
            pool.shutdown();
            assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }
}
