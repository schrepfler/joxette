package com.joxette.db;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the "one failed statement wedges the whole application"
 * bug.
 *
 * <h2>The mechanism (verified against duckdb_jdbc 1.5.5.1)</h2>
 * <p>Joxette shares a single DuckDB {@link Connection} across every thread.  That
 * connection is in JDBC autocommit mode, and under autocommit a failed statement is
 * harmless: DuckDB rolls its own implicit transaction back and the next statement
 * succeeds (see {@link #failureUnderAutocommit_doesNotWedgeTheConnection()}).
 *
 * <p>The connection is <em>not</em> always under autocommit, though.  An explicit
 * transaction can be open on it — left behind by a raw {@code BEGIN TRANSACTION}
 * whose failure path forgot to roll back, by a {@code POST /catalog/query} that ran
 * a bare {@code BEGIN}, or opened for the duration of a batch by the JDBC driver
 * itself ({@code DuckDBPreparedStatement.executeBatch()} issues
 * {@code BEGIN TRANSACTION} before the first entry and {@code COMMIT} after the
 * last one).  Inside such a transaction DuckDB does not roll a failure back: it
 * <em>invalidates</em> the transaction, and then refuses every subsequent statement —
 * from every thread, reads included — with
 * {@code "TransactionContext Error: Current transaction is aborted (please ROLLBACK)"}
 * until somebody issues an explicit {@code ROLLBACK}.  Nothing in Joxette used to,
 * so a single momentary object-store hiccup wedged the entire database layer until
 * the process was restarted.
 *
 * @see #leakedTransaction_plusBatch_wedgesABareConnection() the raw mechanism
 * @see #oneCallersFailure_doesNotWedgeTheConnectionForAnotherThread() the fix
 */
class DuckDbSessionTest {

    private Connection conn;
    private DuckDbSession session;
    private ExecutorService other;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        session = new DuckDbSession(conn);
        other = Executors.newSingleThreadExecutor();
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE known_entities (entity_type VARCHAR, entity_id VARCHAR PRIMARY KEY)");
            st.execute("CREATE TABLE cfg (id INTEGER)");
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        other.shutdownNow();
        conn.close();
    }

    // -------------------------------------------------------------------------
    // The mechanism, as an executable specification of DuckDB's behaviour
    // -------------------------------------------------------------------------

    /**
     * Baseline: with no explicit transaction open, a failed statement cannot poison
     * the connection.  This is why a plain bad query has never wedged Joxette, and
     * why the bug only showed up under concurrency.
     */
    @Test
    void failureUnderAutocommit_doesNotWedgeTheConnection() throws SQLException {
        assertThatThrownBy(() -> exec("SELECT * FROM does_not_exist"))
                .isInstanceOf(SQLException.class);

        assertThat(selectFortyTwo(conn)).isEqualTo(42);
    }

    /**
     * The actual poisoning: a leaked explicit transaction plus any
     * {@code executeBatch()} anywhere on the connection.  The driver's batch
     * wrapper issues {@code BEGIN TRANSACTION}, which fails with
     * {@code "cannot start a transaction within a transaction"}; that failure
     * invalidates the enclosing transaction, and from then on every statement on
     * the connection fails — including plain reads from unrelated threads.
     */
    @Test
    void leakedTransaction_plusBatch_wedgesABareConnection() throws Exception {
        exec("BEGIN TRANSACTION");          // e.g. CompactionService's swallowed failure path

        assertThatThrownBy(this::upsertKnownEntitiesBatch)
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("cannot start a transaction within a transaction");

        // Every later statement, from any thread, is now refused.
        assertThatThrownBy(() -> selectFortyTwo(conn))
                .hasMessageContaining("Current transaction is aborted");
        assertThatThrownBy(() -> onOtherThread(() -> selectFortyTwo(conn)))
                .hasMessageContaining("Current transaction is aborted");

        // ...and a plain ROLLBACK is all it takes to recover.
        conn.rollback();
        assertThat(selectFortyTwo(conn)).isEqualTo(42);
    }

    // -------------------------------------------------------------------------
    // The fix
    // -------------------------------------------------------------------------

    /**
     * The core guarantee: a failure inside {@link DuckDbSession} can never leave the
     * shared connection unusable for the next caller, whatever thread it is on.
     */
    @Test
    void oneCallersFailure_doesNotWedgeTheConnectionForAnotherThread() throws Exception {
        exec("BEGIN TRANSACTION");

        assertThatThrownBy(() -> session.run("known_entities upsert", c -> upsertKnownEntitiesBatch()))
                .isInstanceOf(SQLException.class);

        assertThat(onOtherThread(() -> session.call("unrelated read", DuckDbSessionTest::selectFortyTwo)))
                .isEqualTo(42);
        assertThat(onOtherThread(() -> selectFortyTwo(conn))).isEqualTo(42);
    }

    /**
     * The read path is where the incident surfaced: the HTTP thread's entity query was
     * the first statement to hit the already-aborted transaction.  A read that fails
     * through the session heals the connection on its way out, so the recording
     * pipeline's next write succeeds.
     */
    @Test
    void failedReadHealsTheConnection_forTheWritePipeline() throws Exception {
        exec("BEGIN TRANSACTION");
        assertThatThrownBy(this::upsertKnownEntitiesBatch).isInstanceOf(SQLException.class);

        assertThatThrownBy(() -> session.call("queryEntityEvents:fixture", DuckDbSessionTest::selectFortyTwo))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("Current transaction is aborted");

        assertThat(session.recoveredTransactions()).isEqualTo(1);
        upsertKnownEntitiesBatch();                       // the write pipeline recovers
        assertThat(selectFortyTwo(conn)).isEqualTo(42);
    }

    /** Success path must not pay for the failure path: no stray ROLLBACK, no lost writes. */
    @Test
    void successfulWorkIsNotRolledBack() throws Exception {
        session.run("insert", c -> {
            try (Statement st = c.createStatement()) {
                st.execute("INSERT INTO cfg VALUES (7)");
            }
        });

        int matching = session.call("read back", c -> {
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT count(*) FROM cfg WHERE id = 7")) {
                rs.next();
                return rs.getInt(1);
            }
        });
        assertThat(matching).isEqualTo(1);
    }

    /**
     * {@link DuckDbSession#inTransaction} is for the one place that genuinely needs a
     * multi-statement transaction; its contract is that a failure always rolls back
     * rather than leaking the open transaction.
     */
    @Test
    void inTransaction_rollsBackOnFailure_leavingTheConnectionClean() throws SQLException {
        assertThatThrownBy(() -> session.inTransaction("migrate", c -> {
            try (Statement st = c.createStatement()) {
                st.execute("INSERT INTO cfg VALUES (1)");
                st.execute("INSERT INTO cfg VALUES (bogus_column)");
            }
        })).isInstanceOf(SQLException.class);

        assertThat(selectFortyTwo(conn)).isEqualTo(42);
        assertThat(countCfg()).isZero();
    }

    @Test
    void inTransaction_commitsOnSuccess() throws Exception {
        session.inTransaction("migrate", c -> {
            try (Statement st = c.createStatement()) {
                st.execute("INSERT INTO cfg VALUES (1)");
                st.execute("INSERT INTO cfg VALUES (2)");
            }
        });

        assertThat(countCfg()).isEqualTo(2);
        assertThat(selectFortyTwo(conn)).isEqualTo(42);
    }

    /** {@code rollbackQuietly} is the escape hatch for not-yet-migrated catch blocks. */
    @Test
    void rollbackQuietly_healsAWedgedConnection_andIsANoOpWhenClean() throws Exception {
        assertThat(session.rollbackQuietly("no-op")).isFalse();

        exec("BEGIN TRANSACTION");
        assertThatThrownBy(this::upsertKnownEntitiesBatch).isInstanceOf(SQLException.class);

        assertThat(session.rollbackQuietly("heal")).isTrue();
        assertThat(selectFortyTwo(conn)).isEqualTo(42);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    /** Mirrors {@code KnownEntitiesRepository.upsertBatch}: a batched PreparedStatement. */
    private void upsertKnownEntitiesBatch() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO known_entities (entity_type, entity_id) VALUES (?, ?)")) {
            ps.setString(1, "fixture");
            ps.setString(2, "e1");
            ps.addBatch();
            ps.setString(1, "fixture");
            ps.setString(2, "e2");
            ps.addBatch();
            ps.executeBatch();
        }
    }

    private static int selectFortyTwo(Connection c) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT 42");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private int countCfg() {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM cfg")) {
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new AssertionError("counting cfg failed", e);
        }
    }

    private <T> T onOtherThread(Callable<T> body) throws Exception {
        Future<T> f = other.submit(body);
        try {
            return f.get();
        } catch (java.util.concurrent.ExecutionException e) {
            if (e.getCause() instanceof Exception cause) throw cause;
            throw e;
        }
    }
}
