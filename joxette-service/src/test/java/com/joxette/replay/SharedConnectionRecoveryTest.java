package com.joxette.replay;

import com.joxette.support.DuckDBTestSupport;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end regression test for the incident where one transient object-store read
 * failure wedged the whole application.
 *
 * <p>The production sequence was:
 * <ol>
 *   <li>a transaction was left open on the shared connection (a swallowed failure in
 *       compaction's {@code BEGIN TRANSACTION} block, or a {@code POST /catalog/query});</li>
 *   <li>an HTTP thread's entity read hit a real
 *       {@code "IO Error: Server returned nothing (no headers, no data)"} from RustFS;</li>
 *   <li>that failure (or the recording pipeline's next {@code executeBatch}, whose
 *       driver-issued {@code BEGIN TRANSACTION} collided with the leaked one) invalidated
 *       the transaction;</li>
 *   <li>from then on <em>every</em> statement on the shared connection failed with
 *       {@code "TransactionContext Error: Current transaction is aborted (please
 *       ROLLBACK)"} — the replay API, and the {@code known_entities} upsert running on a
 *       completely different virtual thread — until the process was restarted.</li>
 * </ol>
 *
 * <p>These tests reproduce steps 1–3 deterministically and assert that step 4 no longer
 * happens: {@code withObjectStoreRetry} rolls the connection back between attempts, so
 * the read recovers on its own and the write pipeline is never affected.
 */
class SharedConnectionRecoveryTest {

    private Connection conn;
    private DSLContext dsl;
    private KnownEntitiesRepository knownEntities;
    private ExecutorService writePipeline;

    @BeforeEach
    void setUp() throws SQLException {
        TopicReplayService.resetObjectStoreCircuitForTests();
        conn = DuckDBTestSupport.newConnection();
        dsl = DSL.using(conn, SQLDialect.DUCKDB);
        // Constructing the service publishes the shared connection to the static retry
        // helper, exactly as the Spring bean does at startup.
        new TopicReplayService(dsl, conn);
        knownEntities = new KnownEntitiesRepository(dsl);
        writePipeline = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown() throws Exception {
        writePipeline.shutdownNow();
        writePipeline.awaitTermination(5, TimeUnit.SECONDS);
        conn.close();
    }

    /**
     * The read recovers by itself: attempt 1 fails with the real transient I/O error,
     * the retry helper rolls back the transaction that failure aborted, and attempt 2
     * runs on a clean connection.
     */
    @Test
    void transientReadFailureInsideALeakedTransaction_recoversOnRetry() throws Exception {
        leakAnAbortedTransaction();

        AtomicInteger attempts = new AtomicInteger();
        int rows = TopicReplayService.withObjectStoreRetry("queryEntityEvents:fixture", () -> {
            attempts.incrementAndGet();
            synchronized (conn) {
                return dsl.fetch("SELECT 42").size();
            }
        }, 0);

        assertThat(rows).isEqualTo(1);
        assertThat(attempts.get())
                .as("attempt 1 hits the aborted transaction, attempt 2 runs on a rolled-back connection")
                .isEqualTo(2);
    }

    /**
     * The cross-thread contamination that made this a whole-application outage: the
     * recording pipeline's {@code known_entities} upsert runs on a different thread and
     * must not inherit the read thread's failure.
     */
    @Test
    void aFailedRead_doesNotWedgeTheWritePipelineOnAnotherThread() throws Exception {
        leakAnAbortedTransaction();

        TopicReplayService.withObjectStoreRetry("queryEntityEvents:fixture",
                () -> {
                    synchronized (conn) {
                        return dsl.fetch("SELECT 42").size();
                    }
                }, 0);

        // The write pipeline, on its own thread, now succeeds.
        writePipeline.submit(() -> knownEntities.upsertBatch(
                List.of(new EntityRoute("fixture", "fix-1", 7, "fixture", "feed.fixture.v1")),
                Instant.now())).get(10, TimeUnit.SECONDS);

        assertThat(knownEntities.countByType("fixture")).isEqualTo(1);
        assertThat(select42()).isEqualTo(42);
    }

    /**
     * The write pipeline heals the connection too, so a read arriving afterwards is fine
     * — the poison travels in both directions and so must the cure.
     */
    @Test
    void aFailedWrite_doesNotWedgeSubsequentReads() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("BEGIN TRANSACTION");
        }

        // upsertBatch swallows its failure (by design — it must not abort the pipeline)
        // but now rolls the connection back before doing so.
        knownEntities.upsertBatch(
                List.of(new EntityRoute("fixture", "fix-1", 7, "fixture", "feed.fixture.v1")),
                Instant.now());

        assertThat(select42()).isEqualTo(42);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Puts the shared connection into exactly the state the incident left it in: a
     * transaction that DuckDB has invalidated, so every subsequent statement is refused.
     *
     * <p>Step 1 is the leaked {@code BEGIN TRANSACTION}; step 2 is any {@code executeBatch}
     * on the connection, whose driver-issued {@code BEGIN TRANSACTION} collides with it
     * and takes the enclosing transaction down with it.
     */
    private void leakAnAbortedTransaction() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("BEGIN TRANSACTION");
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO topic_configs (topic, mode) VALUES (?, ?)")) {
            ps.setString(1, "t1");
            ps.setString(2, "general");
            ps.addBatch();
            ps.setString(1, "t2");
            ps.setString(2, "general");
            ps.addBatch();
            ps.executeBatch();
        } catch (SQLException expected) {
            assertThat(expected.getMessage()).contains("cannot start a transaction within a transaction");
        }
        // Sanity-check the precondition: the connection really is wedged right now.
        try (Statement st = conn.createStatement()) {
            st.executeQuery("SELECT 42");
            throw new AssertionError("expected the connection to be wedged at this point");
        } catch (SQLException expected) {
            assertThat(expected.getMessage()).contains("Current transaction is aborted");
        }
    }

    private int select42() throws SQLException {
        synchronized (conn) {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT 42")) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }
}
