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

    private static boolean isDefinitelyNonTransient(String msg) {
        return msg.startsWith("Catalog Error")
            || msg.startsWith("Binder Error")
            || msg.startsWith("Parser Error")
            || msg.startsWith("Constraint Error")
            || msg.startsWith("Invalid Input Error")
            || msg.startsWith("Not implemented Error")
            || msg.startsWith("Permission Error")
            || msg.startsWith("Out of Range Error");
    }

    private static boolean isDefinitelyTransient(String msg) {
        return msg.startsWith("IO Error")
            || msg.contains("HTTP PUT")
            || msg.contains("HTTP GET")
            || msg.contains("HTTP Error")
            || msg.contains("Could not connect")
            || msg.contains("Connection refused")
            || msg.contains("Connection timed out");
    }
}
