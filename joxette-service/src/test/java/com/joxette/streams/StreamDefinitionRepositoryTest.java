package com.joxette.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.joxette.api.error.ConflictException;
import com.joxette.api.error.ResourceNotFoundException;
import com.joxette.replay.DedupPolicy;
import com.joxette.replay.ReplayOutputMode;
import com.joxette.replay.SolOutput;
import com.joxette.replay.StateFoldStrategy;
import com.joxette.support.DuckDBTestSupport;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for {@link StreamDefinitionRepository} against an in-memory DuckDB.
 */
class StreamDefinitionRepositoryTest {

    private Connection                    conn;
    private StreamDefinitionRepository   repo;

    @BeforeEach
    void setUp() throws Exception {
        conn = DuckDBTestSupport.newConnection();
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        repo = new StreamDefinitionRepository(DSL.using(conn, SQLDialect.DUCKDB), mapper);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null && !conn.isClosed()) conn.close();
    }

    // -------------------------------------------------------------------------
    // create + findById
    // -------------------------------------------------------------------------

    @Test
    void create_and_findById_roundTrips() {
        StreamDefinition created = repo.create(
                "order-lifecycle", "Order Lifecycle", "order", null,
                null, null, null, List.of(), null, null);

        assertThat(created.id()).isEqualTo("order-lifecycle");
        assertThat(created.name()).isEqualTo("Order Lifecycle");
        assertThat(created.entityType()).isEqualTo("order");
        assertThat(created.entityId()).isNull();
        assertThat(created.createdAt()).isNotNull();
        assertThat(created.updatedAt()).isNotNull();
    }

    @Test
    void create_withAllFields_roundTrips() {
        StreamDefinition.SourceOptions source = new StreamDefinition.SourceOptions(
                List.of("OrderCreated", "OrderPaid"),
                Instant.parse("2025-01-01T00:00:00Z"),
                Instant.parse("2025-12-31T23:59:59Z"),
                50, DedupPolicy.VALUE);

        StreamDefinition created = repo.create(
                "order-paid-stream", "Order Paid Stream", "order", "order-789",
                source, "MATCH created THEN paid WITHIN 24h",
                SolOutput.ANNOTATED, List.of(),
                ReplayOutputMode.STATE, StateFoldStrategy.MERGE_PATCH);

        StreamDefinition found = repo.findById("order-paid-stream").orElseThrow();
        assertThat(found.entityId()).isEqualTo("order-789");
        assertThat(found.sol()).isEqualTo("MATCH created THEN paid WITHIN 24h");
        assertThat(found.solOutput()).isEqualTo(SolOutput.ANNOTATED);
        assertThat(found.output()).isEqualTo(ReplayOutputMode.STATE);
        assertThat(found.stateFold()).isEqualTo(StateFoldStrategy.MERGE_PATCH);
        assertThat(found.source().messageTypes()).containsExactly("OrderCreated", "OrderPaid");
        assertThat(found.source().lastN()).isEqualTo(50);
        assertThat(found.source().dedup()).isEqualTo(DedupPolicy.VALUE);
    }

    @Test
    void create_duplicate_throwsConflict() {
        repo.create("my-stream", "My Stream", "order", null, null, null, null, List.of(), null, null);

        assertThatThrownBy(() ->
                repo.create("my-stream", "Duplicate", "order", null, null, null, null, List.of(), null, null))
                .isInstanceOf(ConflictException.class)
                .hasMessageContaining("my-stream");
    }

    @Test
    void findById_notFound_returnsEmpty() {
        assertThat(repo.findById("does-not-exist")).isEmpty();
    }

    // -------------------------------------------------------------------------
    // listAll + listByEntityType
    // -------------------------------------------------------------------------

    @Test
    void listAll_returnsAllOrderedById() {
        repo.create("b-stream", "B", "order",  null, null, null, null, List.of(), null, null);
        repo.create("a-stream", "A", "invoice", null, null, null, null, List.of(), null, null);

        List<StreamDefinition> all = repo.listAll();
        assertThat(all).hasSize(2);
        assertThat(all.get(0).id()).isEqualTo("a-stream");
        assertThat(all.get(1).id()).isEqualTo("b-stream");
    }

    @Test
    void listByEntityType_filtersCorrectly() {
        repo.create("order-stream",   "Order",   "order",   null, null, null, null, List.of(), null, null);
        repo.create("invoice-stream", "Invoice", "invoice", null, null, null, null, List.of(), null, null);

        List<StreamDefinition> orders = repo.listByEntityType("order");
        assertThat(orders).hasSize(1);
        assertThat(orders.get(0).id()).isEqualTo("order-stream");
    }

    @Test
    void listAll_emptyRepository_returnsEmptyList() {
        assertThat(repo.listAll()).isEmpty();
    }

    // -------------------------------------------------------------------------
    // update
    // -------------------------------------------------------------------------

    @Test
    void update_modifiesDefinition() {
        repo.create("my-stream", "Original Name", "order", null, null, null, null, List.of(), null, null);

        StreamDefinition updated = repo.update(
                "my-stream", "Updated Name", "order", "order-42",
                null, "NEW SOL", SolOutput.SUMMARY, List.of(),
                ReplayOutputMode.STATE, StateFoldStrategy.LAST_VALUE);

        assertThat(updated.name()).isEqualTo("Updated Name");
        assertThat(updated.entityId()).isEqualTo("order-42");
        assertThat(updated.sol()).isEqualTo("NEW SOL");
        assertThat(updated.solOutput()).isEqualTo(SolOutput.SUMMARY);
        assertThat(updated.output()).isEqualTo(ReplayOutputMode.STATE);
        assertThat(updated.stateFold()).isEqualTo(StateFoldStrategy.LAST_VALUE);
    }

    @Test
    void update_preservesCreatedAt() {
        StreamDefinition created = repo.create(
                "my-stream", "Name", "order", null, null, null, null, List.of(), null, null);
        Instant originalCreatedAt = created.createdAt();

        StreamDefinition updated = repo.update(
                "my-stream", "New Name", "order", null, null, null, null, List.of(), null, null);

        assertThat(updated.createdAt()).isEqualTo(originalCreatedAt);
        assertThat(updated.updatedAt()).isAfterOrEqualTo(originalCreatedAt);
    }

    @Test
    void update_notFound_throwsResourceNotFound() {
        assertThatThrownBy(() ->
                repo.update("ghost", "Name", "order", null, null, null, null, List.of(), null, null))
                .isInstanceOf(ResourceNotFoundException.class)
                .hasMessageContaining("ghost");
    }

    // -------------------------------------------------------------------------
    // delete
    // -------------------------------------------------------------------------

    @Test
    void delete_existingId_returnsTrue() {
        repo.create("to-delete", "Name", "order", null, null, null, null, List.of(), null, null);
        assertThat(repo.delete("to-delete")).isTrue();
        assertThat(repo.findById("to-delete")).isEmpty();
    }

    @Test
    void delete_notFound_returnsFalse() {
        assertThat(repo.delete("nonexistent")).isFalse();
    }
}
