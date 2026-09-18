package com.joxette.db;

import java.sql.SQLException;

/**
 * Classifies DuckDB {@link SQLException}s as transient or non-transient.
 *
 * <h2>Why message matching?</h2>
 * <p>DuckDB JDBC always returns {@code null} for {@link SQLException#getSQLState()} and
 * {@code 0} for {@link SQLException#getErrorCode()} regardless of the error type — there
 * is no exception subclass hierarchy to discriminate on.  Message prefix matching against
 * DuckDB's own error-type labels (e.g. {@code "IO Error:"}, {@code "Catalog Error:"}) is
 * therefore the only reliable signal available from the JDBC layer.
 *
 * <h2>Denylist-first strategy</h2>
 * <p>Rather than allowlisting transient patterns, we denylist known non-transient ones.
 * Any {@code SQLException} whose message does not match a non-transient prefix is treated
 * as transient and retried.  This is safe because all write paths in Joxette are
 * idempotent (at-least-once from Kafka; DuckLake deduplicates on read), so a spurious
 * retry is harmless whereas a missed retry drops data permanently.
 *
 * <h2>Updating this list</h2>
 * <p>If new DuckDB error prefixes need to be classified, add them here — this is the
 * single source of truth.  Previously the same logic was duplicated across
 * {@code DuckLakeWriteChannel}, {@code ExportService}, and {@code CompactionService}.
 */
public final class DuckDbErrors {

    private DuckDbErrors() {}

    /**
     * Returns {@code true} when {@code t} (or any cause in its chain) represents a
     * transient storage / network failure that is safe to retry.
     *
     * <p>Non-transient errors are identified by known DuckDB error-type prefixes:
     * <ul>
     *   <li>{@code Catalog Error} — missing table/schema; retrying won't help</li>
     *   <li>{@code Binder Error} — type/name resolution failure</li>
     *   <li>{@code Parser Error} — malformed SQL</li>
     *   <li>{@code Constraint Error} — PK/FK/unique violation</li>
     *   <li>{@code Invalid Input Error} — bad argument to a function</li>
     *   <li>{@code Not implemented Error} — feature not supported by this build</li>
     *   <li>{@code Permission Error} — access control denial</li>
     *   <li>{@code Out of Range Error} — value overflow</li>
     * </ul>
     * Everything else (IO Error, HTTP errors, connection failures, unknown errors) is
     * treated as potentially transient.
     */
    public static boolean isTransient(Throwable t) {
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof SQLException) {
                String msg = cur.getMessage();
                if (msg != null) {
                    String inner = appenderInnerMessage(msg);
                    if (inner != null) {
                        // A DuckDBAppender error whose real cause we could extract — classify
                        // that inner cause, not the "Appender error, catalog: ..." wrapper.
                        if (isDefinitelyNonTransient(inner)) return false;
                        if (isDefinitelyTransient(inner))    return true;
                        // Recognisable wrapper but unrecognised inner cause (e.g. Appender's
                        // own internal errors like "duckdb_appender_create_ext error" for a
                        // missing table, which carry no further classifiable text) — these are
                        // always deterministic failures in the Appender/schema setup itself,
                        // never a transient network condition, so default to non-transient
                        // instead of falling through to the generic transient-by-default rule.
                        return false;
                    }
                    if (isDefinitelyNonTransient(msg)) return false;
                    if (isDefinitelyTransient(msg))    return true;
                }
            }
            cur = cur.getCause();
        }
        // No SQLException found, or no recognisable prefix — treat as transient
        // (conservative: retry is safe, silent drop is not)
        return t instanceof SQLException;
    }

    /**
     * DuckDB's JDBC {@link org.duckdb.DuckDBAppender} wraps every error in a fixed
     * preamble — {@code "Appender error, catalog: '...', schema: '...', table: '...',
     * message: <real cause>"} — instead of surfacing the real cause's own message
     * (e.g. {@code "Catalog Error: ..."}, {@code "IO Error: ..."}) directly. Matching
     * prefixes against the whole message therefore misses every Appender error,
     * silently misclassifying all of them as transient (the denylist-first default).
     *
     * <p>Returns the substring after {@code "message: "}, or {@code null} if
     * {@code msg} is not an Appender-wrapped error at all.
     */
    private static String appenderInnerMessage(String msg) {
        if (!msg.startsWith("Appender error,")) return null;
        int idx = msg.indexOf("message: ");
        return idx < 0 ? "" : msg.substring(idx + "message: ".length());
    }

    private static boolean isDefinitelyNonTransient(String msg) {
        return msg.startsWith("Catalog Error")
            || msg.startsWith("Binder Error")
            || msg.startsWith("Parser Error")
            || msg.startsWith("Constraint Error")
            || msg.startsWith("Invalid Input Error")
            || msg.startsWith("Not implemented Error")
            || msg.startsWith("Permission Error")
            || msg.startsWith("Out of Range Error")
            || msg.contains("invalid column type");
    }

    private static boolean isDefinitelyTransient(String msg) {
        return msg.startsWith("IO Error")
            || msg.contains("HTTP PUT")
            || msg.contains("HTTP GET")
            || msg.contains("HTTP Error")
            || msg.contains("Could not connect")
            || msg.contains("Connection refused")
            || msg.contains("Connection timed out")
            // Recoverable, not a bug: DuckDbSession rolls the aborted transaction back
            // on the way out of the failing call, so the next attempt runs on a clean
            // connection. Listed explicitly (rather than relying on the
            // transient-by-default fallback) so it also survives the Appender-wrapper
            // path, whose unrecognised-inner-cause branch defaults to non-transient.
            || isAbortedTransactionMessage(msg);
    }

    // -------------------------------------------------------------------------
    // Aborted-transaction detection
    // -------------------------------------------------------------------------

    /**
     * Returns {@code true} when {@code t} (or any cause in its chain) indicates that the
     * shared connection is carrying a transaction that DuckDB has invalidated, or the
     * residue of a failed statement, and therefore needs an explicit {@code ROLLBACK}
     * before anything else will run on it.
     *
     * <p>Three shapes, all seen in production:
     * <ul>
     *   <li>{@code "TransactionContext Error: Current transaction is aborted (please
     *       ROLLBACK)"} — every statement on the connection after the transaction was
     *       invalidated, from every thread, reads included.</li>
     *   <li>{@code "TransactionContext Error: cannot start a transaction within a
     *       transaction"} — the failure that <em>causes</em> the invalidation: the JDBC
     *       driver's {@code executeBatch()} wrapper issuing its own
     *       {@code BEGIN TRANSACTION} while a transaction was already leaked on the
     *       connection.</li>
     *   <li>{@code "Attempting to execute an unsuccessful or closed pending query
     *       result"} — a failed statement's pending result still attached to the
     *       connection, which breaks the next {@code execute()} the same way.</li>
     * </ul>
     *
     * @see DuckDbSession#rollbackQuietly(String)
     */
    public static boolean isAbortedTransaction(Throwable t) {
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            String msg = cur.getMessage();
            if (msg != null && isAbortedTransactionMessage(msg)) return true;
        }
        return false;
    }

    private static boolean isAbortedTransactionMessage(String msg) {
        return msg.contains("Current transaction is aborted")
            || msg.contains("cannot start a transaction within a transaction")
            || msg.contains("closed pending query result");
    }
}
