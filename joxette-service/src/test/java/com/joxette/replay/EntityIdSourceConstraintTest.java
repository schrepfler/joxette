package com.joxette.replay;

import com.joxette.management.IdSource;
import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Guards against future drift between the {@code entity_source_matchers.id_source}
 * DB CHECK constraint and {@link EntityIdExtractor}'s switch cases.
 *
 * <h2>Invariant</h2>
 * <p>Every value accepted by the DB constraint must be handled by
 * {@code EntityIdExtractor.extract()} without falling through to the default
 * (which returns empty). Conversely, any value rejected by the constraint must
 * never appear in application code as a valid source.
 */
class EntityIdSourceConstraintTest {

    private Connection conn;
    private EntityIdExtractor extractor;

    @BeforeEach
    void setUp() throws SQLException {
        conn      = DuckDBTestSupport.newConnection();
        extractor = new EntityIdExtractor();
        // Seed minimal prerequisite rows so FK-like checks pass (DuckDB
        // doesn't enforce FKs, but the INSERT still needs a mapping row to
        // satisfy the UNIQUE constraint on entity_source_matchers).
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO entity_type_configs (entity_type, bucket_count) VALUES ('_test', 1)")) {
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO entity_source_mappings (entity_type, topic, mode) VALUES ('_test', '_topic', 'entity_only')")) {
            ps.executeUpdate();
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        conn.close();
    }

    // -------------------------------------------------------------------------
    // DB constraint: valid values are accepted
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "id_source=''{0}'' is accepted by DB constraint")
    @ValueSource(strings = {"key", "value", "header"})
    void validIdSource_isAcceptedByDbConstraint(String idSource) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO entity_source_matchers
                    (entity_type, topic, message_type, id_source, id_expression)
                VALUES ('_test', '_topic', ?, ?, '$.id')
                """)) {
            ps.setString(1, idSource);   // unique message_type per test run
            ps.setString(2, idSource);
            ps.executeUpdate();          // must not throw
        }
    }

    // -------------------------------------------------------------------------
    // DB constraint: invalid values are rejected
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "id_source=''{0}'' is rejected by DB constraint")
    @ValueSource(strings = {"headers", "HEADER", "Header", "payload", "", "KEY"})
    void invalidIdSource_isRejectedByDbConstraint(String idSource) {
        assertThatThrownBy(() -> {
            try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO entity_source_matchers
                        (entity_type, topic, message_type, id_source, id_expression)
                    VALUES ('_test', '_topic', ?, ?, '$.id')
                    """)) {
                ps.setString(1, idSource + "_msg");
                ps.setString(2, idSource);
                ps.executeUpdate();
            }
        }).isInstanceOf(SQLException.class);
    }

    // -------------------------------------------------------------------------
    // Extractor handles every value the DB accepts
    // -------------------------------------------------------------------------

    @Test
    void extractor_handlesAllDbAcceptedSources_withoutFallingThroughToDefault() {
        KafkaMessage keyMsg = new KafkaMessage(
                "t", 0, 0L, System.currentTimeMillis(), "key-val", null, List.of());
        KafkaMessage valueMsg = new KafkaMessage(
                "t", 0, 0L, System.currentTimeMillis(), null,
                "{\"id\":\"v\"}".getBytes(StandardCharsets.UTF_8), List.of());
        KafkaMessage headerMsg = new KafkaMessage(
                "t", 0, 0L, System.currentTimeMillis(), null, null,
                List.of(new KafkaMessage.Header("x-id", "h-val".getBytes(StandardCharsets.UTF_8))));

        // IdSource.KEY source — should return the message key
        assertThat(extractor.extract(keyMsg, IdSource.KEY, null)).hasValue("key-val");

        // IdSource.VALUE source — should apply JSONPath to message value
        assertThat(extractor.extract(valueMsg, IdSource.VALUE, "$.id")).hasValue("v");

        // IdSource.HEADER source — should extract the named header
        assertThat(extractor.extract(headerMsg, IdSource.HEADER, "x-id")).hasValue("h-val");
    }

    @Test
    void idSource_fromValue_rejectsObsoletePluralSpelling() {
        // "headers" (plural) is not a valid IdSource — IdSource.fromValue must throw.
        assertThatThrownBy(() -> IdSource.fromValue("headers"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
