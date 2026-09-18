package com.joxette.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * The single serialisation point for the shared DuckDB {@link Connection}.
 *
 * <p>Joxette runs one embedded DuckDB connection for the whole process (see
 * {@code DuckDBConfig} and the "DuckDB Connection Model" section of {@code CLAUDE.md}).
 * Historically every DB-touching class hand-rolled its own
 * {@code synchronized (duckDB) { … }} block; this class is the shared replacement, and
 * it adds the one thing the hand-rolled blocks were all missing: <strong>a failed
 * statement can never leave the connection unusable for the next caller.</strong>
 *
 * <h2>Why that guarantee is needed</h2>
 * <p>Under JDBC autocommit — which is how the connection is opened — a failed
 * statement is harmless: DuckDB rolls its own implicit transaction back and the next
 * statement succeeds.  But the connection is not always under autocommit:
 * <ul>
 *   <li>the JDBC driver itself opens one for the duration of a batch —
 *       {@code DuckDBPreparedStatement.executeBatch()} issues {@code BEGIN TRANSACTION}
 *       before the first entry and {@code COMMIT} after the last;</li>
 *   <li>{@link #inTransaction} (and, historically, raw {@code BEGIN TRANSACTION}
 *       statements whose failure path forgot to roll back) opens one explicitly;</li>
 *   <li>{@code POST /catalog/query} will run whatever SQL it is given, {@code BEGIN}
 *       included.</li>
 * </ul>
 * <p>Inside an explicit transaction DuckDB does <em>not</em> roll a failure back — it
 * invalidates the transaction and then refuses every subsequent statement, from every
 * thread and reads included, with
 * {@code "TransactionContext Error: Current transaction is aborted (please ROLLBACK)"}.
 * A single transient object-store hiccup anywhere therefore used to wedge the entire
 * database layer for the lifetime of the process.  A plain {@code ROLLBACK} clears it
 * completely, which is exactly what this class does on every failure path.
 *
 * <h2>Why {@link Connection#rollback()} and not a raw {@code ROLLBACK} statement</h2>
 * <p>{@code DuckDBConnection} tracks whether it has a batch transaction in flight in a
 * {@code transactionRunning} field.  When its {@code BEGIN TRANSACTION} fails (which is
 * precisely what happens when a transaction was already leaked) that field is left stuck
 * at {@code true}, and every later {@code executeBatch()} then silently skips both its
 * {@code BEGIN} and its {@code COMMIT}.  {@link Connection#rollback()} resets the field;
 * executing {@code "ROLLBACK"} as SQL does not.
 *
 * <h2>Locking</h2>
 * <p>The monitor is the {@link Connection} object itself, so this class interoperates
 * with the {@code synchronized (duckDB)} blocks that have not been migrated yet — both
 * forms exclude each other. All nested uses are reentrant.
 */
@Component
public class DuckDbSession {

    private static final Logger log = LoggerFactory.getLogger(DuckDbSession.class);

    private final Connection connection;

    /** Number of transactions this session has had to roll back after a failure. */
    private final AtomicLong recoveredTransactions = new AtomicLong();

    public DuckDbSession(Connection duckDbConnection) {
        this.connection = duckDbConnection;
    }

    /** The shared connection, for the call sites that still manage their own locking. */
    public Connection connection() {
        return connection;
    }

    /** How many times a failure left a transaction behind that had to be rolled back. */
    public long recoveredTransactions() {
        return recoveredTransactions.get();
    }

    // -------------------------------------------------------------------------
    // Running work
    // -------------------------------------------------------------------------

    /**
     * Runs {@code body} against the shared connection under the shared monitor,
     * rolling back any transaction the failure leaves behind.
     *
     * @param description what the work is, used only in log messages
     */
    public <T> T call(String description, SqlQuery<T> body) throws SQLException {
        synchronized (connection) {
            try {
                return body.run(connection);
            } catch (SQLException | RuntimeException e) {
                rollbackQuietly(description);
                throw e;
            }
        }
    }

    /** Void-returning {@link #call}. */
    public void run(String description, SqlTask body) throws SQLException {
        call(description, conn -> {
            body.run(conn);
            return null;
        });
    }

    /**
     * {@link #call} for the jOOQ call sites, whose bodies throw the unchecked
     * {@code org.jooq.exception.DataAccessException} rather than {@link SQLException}.
     */
    public <T> T supply(String description, Supplier<T> body) {
        synchronized (connection) {
            try {
                return body.get();
            } catch (RuntimeException e) {
                rollbackQuietly(description);
                throw e;
            }
        }
    }

    /**
     * Runs {@code body} inside an explicit {@code BEGIN TRANSACTION} … {@code COMMIT},
     * rolling back if anything throws.
     *
     * <p>Use this only where several statements genuinely have to be atomic.  Everything
     * else belongs in {@link #call}, which leaves DuckDB in autocommit mode where a
     * failure cannot poison the connection in the first place.
     */
    public void inTransaction(String description, SqlTask body) throws SQLException {
        synchronized (connection) {
            try (Statement st = connection.createStatement()) {
                st.execute("BEGIN TRANSACTION");
            } catch (SQLException e) {
                // A transaction was already open — almost certainly leaked by an earlier
                // failure. Clear it and start ours cleanly rather than running our
                // statements inside somebody else's aborted transaction.
                rollbackQuietly(description + " (pre-existing transaction)");
                try (Statement st = connection.createStatement()) {
                    st.execute("BEGIN TRANSACTION");
                }
            }
            try {
                body.run(connection);
            } catch (SQLException | RuntimeException e) {
                rollbackQuietly(description);
                throw e;
            }
            try {
                connection.commit();
            } catch (SQLException e) {
                rollbackQuietly(description + " (commit failed)");
                throw e;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Recovery
    // -------------------------------------------------------------------------

    /**
     * Rolls back whatever transaction is open on the shared connection, if any.
     *
     * <p>Safe and cheap to call speculatively: with no transaction open DuckDB answers
     * {@code "cannot rollback - no transaction is active"}, which is reported here as
     * "nothing to do" rather than as an error.  Call it from any {@code catch} block
     * that swallows a {@link SQLException} on the shared connection but has not been
     * migrated to {@link #call} yet.
     *
     * @return {@code true} if a transaction was actually rolled back
     */
    public boolean rollbackQuietly(String description) {
        boolean rolledBack = rollbackQuietly(connection, description);
        if (rolledBack) recoveredTransactions.incrementAndGet();
        return rolledBack;
    }

    /**
     * Static form of {@link #rollbackQuietly(String)}, for the {@code catch} blocks that
     * still hold the raw shared {@link Connection} and have not been migrated to
     * {@link #call} yet.
     *
     * <p>Every {@code catch (SQLException)} on the shared connection that swallows or
     * translates the error should call this before returning, so the failure cannot be
     * inherited by the next caller on any other thread.
     *
     * @return {@code true} if a transaction was actually rolled back
     */
    public static boolean rollbackQuietly(Connection connection, String description) {
        synchronized (connection) {
            try {
                // Connection.rollback() rather than a "ROLLBACK" statement: it also
                // resets DuckDBConnection.transactionRunning, which a failed
                // driver-issued BEGIN leaves stuck at true (see the class javadoc).
                connection.rollback();
            } catch (SQLException e) {
                if (!isNothingToRollBack(e)) {
                    log.error("Could not roll back the shared DuckDB connection after a failure in {} — "
                            + "it may stay unusable until restart: {}", description, e.getMessage(), e);
                }
                return false;
            }
            log.warn("Rolled back a transaction left open on the shared DuckDB connection after a failure in {}. "
                    + "Without this, every subsequent statement on the connection — from any thread, reads "
                    + "included — would fail with 'Current transaction is aborted (please ROLLBACK)'.", description);
            return true;
        }
    }

    /**
     * Distinguishes "there was nothing to roll back" from "the rollback itself failed".
     * The former is the overwhelmingly common case — the connection is under autocommit,
     * so DuckDB answers {@code "cannot rollback - no transaction is active"} — and must
     * not be logged as a problem. A closed connection (shutdown, or a test's connection
     * outliving a static reference to it) likewise has nothing to recover.
     */
    private static boolean isNothingToRollBack(SQLException e) {
        String msg = e.getMessage();
        if (msg == null) return false;
        return msg.contains("no transaction is active")
            || msg.contains("has been closed")
            || msg.contains("Connection was closed")
            || msg.contains("connection closed");
    }

    // -------------------------------------------------------------------------
    // Functional interfaces
    // -------------------------------------------------------------------------

    /** A unit of work on the shared connection that returns a value. */
    @FunctionalInterface
    public interface SqlQuery<T> {
        T run(Connection connection) throws SQLException;
    }

    /** A unit of work on the shared connection that returns nothing. */
    @FunctionalInterface
    public interface SqlTask {
        void run(Connection connection) throws SQLException;
    }
}
