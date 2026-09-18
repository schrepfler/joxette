package com.joxette.db;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link DuckDbErrors#isTransient} classifies DuckDB {@link SQLException}s as
 * transient (safe to retry) or non-transient (a real bug — retrying wastes time
 * and, for quarantine-tracked batches, delays giving up on a poison record).
 *
 * <p>Covers both message shapes DuckDB actually produces:
 * <ul>
 *   <li>Plain SQL-engine errors: {@code "Catalog Error: ..."}.</li>
 *   <li>{@link org.duckdb.DuckDBAppender} errors, which wrap the real cause in a
 *       fixed preamble: {@code "Appender error, catalog: '...', schema: '...',
 *       table: '...', message: <real cause>"} — verified against real
 *       {@code DuckDBAppender} output (see the class-level rationale in
 *       {@code DuckDbErrors}). A naive {@code startsWith}/{@code contains} check
 *       against the whole message misclassifies every Appender error as
 *       transient, since none of them start with the plain-error prefixes.</li>
 * </ul>
 */
class DuckDbErrorsTest {

    private static SQLException plain(String message) {
        return new SQLException(message);
    }

    private static SQLException appenderWrapped(String innerMessage) {
        return new SQLException("Appender error, catalog: 'lake', schema: 'main', table: 'general_x', message: "
                + innerMessage);
    }

    // -------------------------------------------------------------------------
    // Plain SQL-engine error messages (pre-existing behaviour)
    // -------------------------------------------------------------------------

    @Test
    void catalogError_isNotTransient() {
        assertThat(DuckDbErrors.isTransient(plain("Catalog Error: Table with name x does not exist!"))).isFalse();
    }

    @Test
    void constraintError_isNotTransient() {
        assertThat(DuckDbErrors.isTransient(plain("Constraint Error: NOT NULL constraint failed"))).isFalse();
    }

    @Test
    void ioError_isTransient() {
        assertThat(DuckDbErrors.isTransient(plain("IO Error: Server returned nothing (no headers, no data)")))
                .isTrue();
    }

    @Test
    void connectionRefused_isTransient() {
        assertThat(DuckDbErrors.isTransient(plain("Connection refused"))).isTrue();
    }

    @Test
    void unrecognisedMessage_defaultsToTransient() {
        assertThat(DuckDbErrors.isTransient(plain("something DuckDB has never said before"))).isTrue();
    }

    @Test
    void nonSqlException_isNotTransient() {
        assertThat(DuckDbErrors.isTransient(new RuntimeException("boom"))).isFalse();
    }

    // -------------------------------------------------------------------------
    // DuckDBAppender-wrapped error messages
    // -------------------------------------------------------------------------

    @Test
    void appenderWrapped_catalogError_isNotTransient() {
        // Real message shape from createAppender() against a table that doesn't exist:
        // "Appender error, catalog: 'lake', schema: 'main', table: 'x', message:
        //  duckdb_appender_create_ext error" — no "Catalog Error:" text at all, so this
        // specifically exercises the fallback (see below), not the prefix match.
        assertThat(DuckDbErrors.isTransient(
                appenderWrapped("Catalog Error: Table with name x does not exist!"))).isFalse();
    }

    @Test
    void appenderWrapped_constraintError_isNotTransient() {
        assertThat(DuckDbErrors.isTransient(
                appenderWrapped("Constraint Error: PRIMARY KEY or UNIQUE constraint violated: duplicate key \"k\"")))
                .isFalse();
    }

    @Test
    void appenderWrapped_ioError_isTransient() {
        assertThat(DuckDbErrors.isTransient(
                appenderWrapped("IO Error: Server returned nothing (no headers, no data)"))).isTrue();
    }

    @Test
    void appenderWrapped_invalidColumnType_isNotTransient() {
        // Real message from appending a value of the wrong native type to a column —
        // a programming bug (wrong append(...) overload for the column's DuckDB type),
        // never fixed by retrying.
        assertThat(DuckDbErrors.isTransient(appenderWrapped(
                "invalid column type, expected one of: '[DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_ENUM]', "
                        + "actual: 'DUCKDB_TYPE_BLOB'"))).isFalse();
    }

    @Test
    void appenderCreateFailure_missingTable_isNotTransient() {
        // The exact real message createAppender() throws for a nonexistent table —
        // no recognisable inner "message:" cause to extract, just a bare marker.
        // Missing tables are deterministic (retrying can't create the table), so this
        // must not be classified transient even though it matches no known pattern.
        assertThat(DuckDbErrors.isTransient(plain(
                "Appender error, catalog: 'lake', schema: 'main', table: 'does_not_exist', "
                        + "message: duckdb_appender_create_ext error"))).isFalse();
    }

    // -------------------------------------------------------------------------
    // Aborted-transaction detection
    // -------------------------------------------------------------------------

    @Test
    void abortedTransaction_isRecognised() {
        assertThat(DuckDbErrors.isAbortedTransaction(plain(
                "TransactionContext Error: Current transaction is aborted (please ROLLBACK)"))).isTrue();
    }

    @Test
    void transactionWithinTransaction_isRecognised() {
        // The failure that *causes* the abort: the JDBC driver's executeBatch() wrapper
        // issuing BEGIN TRANSACTION while a transaction is already open.
        assertThat(DuckDbErrors.isAbortedTransaction(plain(
                "TransactionContext Error: cannot start a transaction within a transaction"))).isTrue();
    }

    @Test
    void pendingQueryResidue_isRecognised() {
        assertThat(DuckDbErrors.isAbortedTransaction(plain(
                "Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result")))
                .isTrue();
    }

    @Test
    void abortedTransaction_isRecognisedThroughTheCauseChain() {
        assertThat(DuckDbErrors.isAbortedTransaction(new RuntimeException("wrapper",
                plain("TransactionContext Error: Current transaction is aborted (please ROLLBACK)")))).isTrue();
    }

    @Test
    void ordinaryErrors_areNotAbortedTransactions() {
        assertThat(DuckDbErrors.isAbortedTransaction(plain("IO Error: Timeout was reached"))).isFalse();
        assertThat(DuckDbErrors.isAbortedTransaction(plain("Catalog Error: no such table"))).isFalse();
        assertThat(DuckDbErrors.isAbortedTransaction(null)).isFalse();
    }

    @Test
    void abortedTransaction_isTransient_becauseARollbackClearsIt() {
        // Must stay retryable: DuckDbSession rolls the aborted transaction back on the
        // way out of the failing call, so the next attempt runs on a clean connection.
        // Classifying it non-transient would make DuckLakeWriteChannel quarantine a
        // perfectly good batch (record loss) instead of simply retrying it.
        assertThat(DuckDbErrors.isTransient(plain(
                "TransactionContext Error: Current transaction is aborted (please ROLLBACK)"))).isTrue();
        assertThat(DuckDbErrors.isTransient(appenderWrapped(
                "TransactionContext Error: Current transaction is aborted (please ROLLBACK)"))).isTrue();
    }
}
