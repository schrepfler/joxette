package com.joxette.db;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link CatalogBackend#detect(String)}.
 *
 * <p>Each URI form (embedded file, Quack, PostgreSQL) is verified through the full
 * detection switch so that any future URI scheme addition must explicitly update this
 * test class.
 */
class CatalogBackendTest {

    // -------------------------------------------------------------------------
    // EMBEDDED_DUCKDB — no scheme or file: prefix
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {":memory:", " "})
    void detect_nullBlankOrMemory_returnsEmbeddedDuckDb(String path) {
        assertThat(CatalogBackend.detect(path)).isEqualTo(CatalogBackend.EMBEDDED_DUCKDB);
    }

    @ParameterizedTest
    @CsvSource({
        "./data/joxette.ducklake",
        "./data/joxette.db",
        "/opt/joxette/catalog.ducklake",
        "data/catalog",
        "relative/path/to/catalog.ducklake"
    })
    void detect_plainFilePath_returnsEmbeddedDuckDb(String path) {
        assertThat(CatalogBackend.detect(path)).isEqualTo(CatalogBackend.EMBEDDED_DUCKDB);
    }

    @ParameterizedTest
    @CsvSource({
        "file:./data/joxette.ducklake",
        "file:/opt/joxette/catalog.ducklake",
        "FILE:./data/joxette.ducklake"
    })
    void detect_fileScheme_returnsEmbeddedDuckDb(String path) {
        assertThat(CatalogBackend.detect(path)).isEqualTo(CatalogBackend.EMBEDDED_DUCKDB);
    }

    // -------------------------------------------------------------------------
    // QUACK
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @CsvSource({
        "quack://quack-host:9999/joxette-catalog",
        "quack://localhost:9999/catalog",
        "QUACK://quack-host:9999/catalog",
        "quack://quack-host:9999/joxette-catalog?timeout=30"
    })
    void detect_quackScheme_returnsQuack(String path) {
        assertThat(CatalogBackend.detect(path)).isEqualTo(CatalogBackend.QUACK);
    }

    // -------------------------------------------------------------------------
    // POSTGRESQL
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @CsvSource({
        "postgresql://pg-host:5432/joxette_catalog",
        "postgresql://pg-host:5432/joxette_catalog?user=joxette&password=secret",
        "postgres://pg-host:5432/joxette_catalog",
        "POSTGRESQL://pg-host:5432/joxette_catalog",
        "POSTGRES://pg-host:5432/joxette_catalog"
    })
    void detect_postgresScheme_returnsPostgresql(String path) {
        assertThat(CatalogBackend.detect(path)).isEqualTo(CatalogBackend.POSTGRESQL);
    }

    // -------------------------------------------------------------------------
    // Unknown schemes → IllegalArgumentException
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(strings = {
        "mysql://host:3306/db",
        "mongodb://host:27017/catalog",
        "jdbc:postgresql://host:5432/db",  // JDBC URL form not supported; use bare URI
        "http://host/catalog",
        "unknown://host/catalog"
    })
    void detect_unknownScheme_throwsIllegalArgumentException(String path) {
        assertThatThrownBy(() -> CatalogBackend.detect(path))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unrecognised joxette.catalog.path URI scheme")
            .hasMessageContaining(path);
    }

    // -------------------------------------------------------------------------
    // Enum names (used in logs and health details)
    // -------------------------------------------------------------------------

    @Test
    void enumNames_matchExpectedStrings() {
        assertThat(CatalogBackend.EMBEDDED_DUCKDB.name()).isEqualTo("EMBEDDED_DUCKDB");
        assertThat(CatalogBackend.QUACK.name()).isEqualTo("QUACK");
        assertThat(CatalogBackend.POSTGRESQL.name()).isEqualTo("POSTGRESQL");
    }

    // -------------------------------------------------------------------------
    // Stage-1 default path sanity check
    // -------------------------------------------------------------------------

    @Test
    void detect_defaultStage1Path_returnsEmbeddedDuckDb() {
        // The default configured in JoxetteProperties.Catalog
        assertThat(CatalogBackend.detect("./data/joxette.ducklake"))
            .isEqualTo(CatalogBackend.EMBEDDED_DUCKDB);
    }

    @Test
    void detect_exampleStage2Path_returnsQuack() {
        assertThat(CatalogBackend.detect("quack://quack-host:9999/joxette-catalog"))
            .isEqualTo(CatalogBackend.QUACK);
    }

    @Test
    void detect_exampleStage3Path_returnsPostgresql() {
        assertThat(CatalogBackend.detect(
            "postgresql://pg-host:5432/joxette_catalog?user=joxette&password=secret"))
            .isEqualTo(CatalogBackend.POSTGRESQL);
    }
}
