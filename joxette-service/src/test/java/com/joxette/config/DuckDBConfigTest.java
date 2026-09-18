package com.joxette.config;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for compaction's dedicated DuckDB connection.
 *
 * <p>Before this, {@link CompactionService} shared the single {@code Connection} bean
 * used by every other read/write path, all serialised via {@code synchronized(duckDB)}
 * on that same object. A merge call blocked deep in an httpfs retry (verified via a
 * standalone spike: neither {@code Statement.cancel()} nor an interrupt from another
 * thread unblocks it) held that monitor forever, wedging the entire application —
 * recording, replay, and health checks included — until process restart.
 *
 * <p>{@link DuckDBConfig#compactionDuckDbConnection} gives compaction its own
 * connection to the same database instance via {@code DuckDBConnection#duplicate()},
 * so a hang there can no longer hold the monitor everything else depends on.
 */
class DuckDBConfigTest {

    private final DuckDBConfig config = new DuckDBConfig();

    private JoxetteProperties inMemoryProperties() {
        JoxetteProperties props = new JoxetteProperties();
        props.getCatalog().setPath(":memory:");
        return props;
    }

    @Test
    void compactionConnection_isADistinctObject_thatSeesTheSameCatalogState() throws SQLException {
        Connection main = config.duckDbConnection(inMemoryProperties());
        Connection compaction = config.compactionDuckDbConnection(main);
        try {
            assertThat(compaction).isNotSameAs(main);

            try (Statement st = main.createStatement()) {
                st.execute("CREATE TABLE probe (id INTEGER)");
                st.execute("INSERT INTO probe VALUES (1)");
            }
            try (Statement st = compaction.createStatement();
                 ResultSet rs = st.executeQuery("SELECT count(*) FROM probe")) {
                rs.next();
                assertThat(rs.getInt(1)).isEqualTo(1);
            }
        } finally {
            compaction.close();
            main.close();
        }
    }

    /**
     * The actual property that matters: everything outside compaction synchronizes on
     * the main connection object, and {@link com.joxette.compaction.CompactionService}
     * synchronizes on the dedicated one — so holding one monitor for a long time must
     * never delay work guarded by the other.
     */
    @Test
    void holdingTheCompactionConnectionMonitor_doesNotBlockTheMainConnection() throws Exception {
        Connection main = config.duckDbConnection(inMemoryProperties());
        Connection compaction = config.compactionDuckDbConnection(main);
        CountDownLatch holding = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread holder = new Thread(() -> {
            synchronized (compaction) {
                holding.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                    // test teardown
                }
            }
        });
        try {
            holder.start();
            assertThat(holding.await(2, TimeUnit.SECONDS))
                    .as("holder thread should have acquired the compaction connection's monitor")
                    .isTrue();

            long start = System.nanoTime();
            synchronized (main) {
                try (Statement st = main.createStatement()) {
                    st.execute("SELECT 1");
                }
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;

            assertThat(elapsedMs)
                    .as("a query guarded by the main connection's monitor must not wait on "
                            + "the compaction connection's monitor")
                    .isLessThan(500);
        } finally {
            release.countDown();
            holder.join();
            compaction.close();
            main.close();
        }
    }
}
