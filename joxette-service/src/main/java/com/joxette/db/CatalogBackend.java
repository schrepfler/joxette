package com.joxette.db;

import java.util.Locale;

/**
 * Identifies the DuckLake catalog backend in use, detected purely from the
 * URI scheme of {@code joxette.catalog.path} at startup.
 *
 * <p>The DuckLake catalog schema is <em>identical across all three backends</em> —
 * only the {@code ATTACH} URI changes.  Moving between stages is a one-line YAML
 * change; no Java code, no schema migration, no Parquet data movement is required.
 * See {@code docs/catalog-scaling.md} for the full migration runbook.
 *
 * <h2>URI detection rules</h2>
 * <ol>
 *   <li>{@code :memory:}, blank, or {@code null} → {@link #EMBEDDED_DUCKDB} (in-memory, tests only)</li>
 *   <li>No {@code ://} in the value, or {@code file:} prefix → {@link #EMBEDDED_DUCKDB} (local file)</li>
 *   <li>{@code quack://} prefix → {@link #QUACK}</li>
 *   <li>{@code postgresql://} or {@code postgres://} prefix → {@link #POSTGRESQL}</li>
 *   <li>Any other URI scheme → {@link IllegalArgumentException}</li>
 * </ol>
 *
 * <h2>Supported URI forms</h2>
 * <pre>
 *   # Stage 1 — embedded DuckDB file (default)
 *   joxette.catalog.path: ./data/joxette.ducklake
 *
 *   # Stage 2 — Quack client–server DuckDB (DuckDB 1.5.3+, beta)
 *   joxette.catalog.path: quack://quack-host:9999/joxette-catalog
 *
 *   # Stage 3 — PostgreSQL
 *   joxette.catalog.path: postgresql://pg-host:5432/joxette_catalog?user=joxette&amp;password=secret
 * </pre>
 */
public enum CatalogBackend {

    /**
     * Embedded DuckDB file — default for single-process deployments.
     *
     * <p>The catalog is a local {@code .ducklake} DuckDB file (or {@code :memory:}
     * in tests).  A single JVM process owns the file; running a second Joxette
     * instance against the same file will corrupt it.
     *
     * <p>Example: {@code joxette.catalog.path: ./data/joxette.ducklake}
     */
    EMBEDDED_DUCKDB,

    /**
     * Quack client–server DuckDB (DuckDB 1.5.3+, <b>beta</b>).
     *
     * <p>A single DuckDB process serves multiple Joxette instances over TCP.
     * Evaluate at DuckDB 2.0 GA; the Quack protocol may change before then.
     *
     * <p>Example: {@code joxette.catalog.path: quack://quack-host:9999/joxette-catalog}
     */
    QUACK,

    /**
     * PostgreSQL catalog backend.
     *
     * <p>Full HA and streaming replication available.  DuckDB-native types
     * (VARIANT, LIST, STRUCT) are stored as opaque bytes in PostgreSQL and are
     * only readable via DuckDB; they cannot be queried directly from PostgreSQL.
     *
     * <p>Example: {@code joxette.catalog.path: postgresql://pg-host:5432/joxette_catalog?user=joxette&password=secret}
     */
    POSTGRESQL;

    /**
     * Detects the catalog backend from the raw {@code joxette.catalog.path} value.
     *
     * @param catalogPath raw value of {@code joxette.catalog.path}; may be {@code null}
     * @return the matching {@link CatalogBackend}
     * @throws IllegalArgumentException if the URI scheme is present but not recognised
     */
    public static CatalogBackend detect(String catalogPath) {
        if (catalogPath == null || catalogPath.isBlank() || ":memory:".equals(catalogPath)) {
            return EMBEDDED_DUCKDB;
        }
        String lower = catalogPath.toLowerCase(Locale.ROOT);
        if (lower.startsWith("quack://")) {
            return QUACK;
        }
        if (lower.startsWith("postgresql://") || lower.startsWith("postgres://")) {
            return POSTGRESQL;
        }
        if (lower.startsWith("file:")) {
            return EMBEDDED_DUCKDB;
        }
        // No scheme present → plain file path → embedded DuckDB.
        if (!lower.contains("://")) {
            return EMBEDDED_DUCKDB;
        }
        throw new IllegalArgumentException(
            "Unrecognised joxette.catalog.path URI scheme: '" + catalogPath + "'. " +
            "Supported prefixes: none or file: (embedded DuckDB file), " +
            "quack:// (Quack server, DuckDB 1.5.3+), " +
            "postgresql:// or postgres:// (PostgreSQL catalog).");
    }
}
