package com.joxette.exports;

import com.joxette.api.error.ResourceNotFoundException;
import com.joxette.support.DuckDBTestSupport;
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

class ExportJobRepositoryTest {

    private Connection           conn;
    private ExportJobRepository  repo;

    @BeforeEach
    void setUp() throws Exception {
        conn = DuckDBTestSupport.newConnection();
        repo = new ExportJobRepository(DSL.using(conn, SQLDialect.DUCKDB));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null && !conn.isClosed()) conn.close();
    }

    // -------------------------------------------------------------------------
    // Create
    // -------------------------------------------------------------------------

    @Test
    void create_returnsJobInPendingStatus() {
        ExportJob job = repo.create("order", List.of("ORD-1", "ORD-2"),
                null, null, null, ExportOutputFormat.PARQUET);

        assertThat(job.id()).isNotBlank();
        assertThat(job.entityType()).isEqualTo("order");
        assertThat(job.entityIds()).containsExactly("ORD-1", "ORD-2");
        assertThat(job.outputFormat()).isEqualTo(ExportOutputFormat.PARQUET);
        assertThat(job.status()).isEqualTo(ExportStatus.PENDING);
        assertThat(job.createdAt()).isNotNull();
        assertThat(job.startedAt()).isNull();
        assertThat(job.completedAt()).isNull();
        assertThat(job.outputPath()).isNull();
        assertThat(job.rowCount()).isNull();
    }

    @Test
    void create_withAllOptionalFields_roundtrips() {
        Instant from = Instant.parse("2025-01-01T00:00:00Z");
        Instant to   = Instant.parse("2025-06-01T00:00:00Z");
        ExportJob job = repo.create("order", List.of("ORD-X"),
                from, to, List.of("OrderCreated", "OrderPaid"), ExportOutputFormat.NDJSON);

        assertThat(job.from()).isEqualTo(from);
        assertThat(job.to()).isEqualTo(to);
        assertThat(job.messageTypes()).containsExactly("OrderCreated", "OrderPaid");
        assertThat(job.outputFormat()).isEqualTo(ExportOutputFormat.NDJSON);
    }

    // -------------------------------------------------------------------------
    // FindById
    // -------------------------------------------------------------------------

    @Test
    void findById_existingJob_returnsIt() {
        ExportJob created = repo.create("order", List.of("A"), null, null, null, ExportOutputFormat.PARQUET);
        assertThat(repo.findById(created.id())).isPresent();
    }

    @Test
    void findById_unknownId_returnsEmpty() {
        assertThat(repo.findById("does-not-exist")).isEmpty();
    }

    // -------------------------------------------------------------------------
    // List
    // -------------------------------------------------------------------------

    @Test
    void listAll_multipleJobs_orderedNewestFirst() throws Exception {
        ExportJob j1 = repo.create("order", List.of("A"), null, null, null, ExportOutputFormat.PARQUET);
        Thread.sleep(5);
        ExportJob j2 = repo.create("order", List.of("B"), null, null, null, ExportOutputFormat.PARQUET);

        List<ExportJob> all = repo.listAll();
        assertThat(all).hasSizeGreaterThanOrEqualTo(2);
        // newest first
        assertThat(all.get(0).id()).isEqualTo(j2.id());
        assertThat(all.get(1).id()).isEqualTo(j1.id());
    }

    @Test
    void listByEntityType_filtersCorrectly() {
        repo.create("order",   List.of("A"), null, null, null, ExportOutputFormat.PARQUET);
        repo.create("invoice", List.of("B"), null, null, null, ExportOutputFormat.PARQUET);

        List<ExportJob> orders = repo.listByEntityType("order");
        assertThat(orders).hasSize(1);
        assertThat(orders.get(0).entityType()).isEqualTo("order");
    }

    // -------------------------------------------------------------------------
    // Status transitions
    // -------------------------------------------------------------------------

    @Test
    void markRunning_setsStatusAndStartedAt() {
        ExportJob job = repo.create("order", List.of("A"), null, null, null, ExportOutputFormat.PARQUET);
        repo.markRunning(job.id());

        ExportJob updated = repo.findById(job.id()).orElseThrow();
        assertThat(updated.status()).isEqualTo(ExportStatus.RUNNING);
        assertThat(updated.startedAt()).isNotNull();
    }

    @Test
    void markCompleted_setsStatusOutputPathRowCountAndCompletedAt() {
        ExportJob job = repo.create("order", List.of("A"), null, null, null, ExportOutputFormat.PARQUET);
        repo.markRunning(job.id());
        repo.markCompleted(job.id(), "s3://bucket/exports/job.parquet", 42L);

        ExportJob updated = repo.findById(job.id()).orElseThrow();
        assertThat(updated.status()).isEqualTo(ExportStatus.COMPLETED);
        assertThat(updated.outputPath()).isEqualTo("s3://bucket/exports/job.parquet");
        assertThat(updated.rowCount()).isEqualTo(42L);
        assertThat(updated.completedAt()).isNotNull();
    }

    @Test
    void markFailed_setsStatusErrorMessageAndCompletedAt() {
        ExportJob job = repo.create("order", List.of("A"), null, null, null, ExportOutputFormat.PARQUET);
        repo.markFailed(job.id(), "connection refused");

        ExportJob updated = repo.findById(job.id()).orElseThrow();
        assertThat(updated.status()).isEqualTo(ExportStatus.FAILED);
        assertThat(updated.errorMessage()).isEqualTo("connection refused");
        assertThat(updated.completedAt()).isNotNull();
    }

    // -------------------------------------------------------------------------
    // Delete
    // -------------------------------------------------------------------------

    @Test
    void delete_existingJob_returnsTrue() {
        ExportJob job = repo.create("order", List.of("A"), null, null, null, ExportOutputFormat.PARQUET);
        assertThat(repo.delete(job.id())).isTrue();
        assertThat(repo.findById(job.id())).isEmpty();
    }

    @Test
    void delete_missingJob_returnsFalse() {
        assertThat(repo.delete("ghost-id")).isFalse();
    }
}
