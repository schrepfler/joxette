package com.joxette.reconciliation;

import com.joxette.cluster.InstanceRegistry;
import com.joxette.compaction.CompactionLockManager;
import com.joxette.compaction.RunStatus;
import com.joxette.compaction.TriggerSource;
import com.joxette.config.JoxetteProperties;
import com.joxette.metrics.JoxetteMetrics;
import com.joxette.support.DuckDBTestSupport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReconciliationServiceLifecycleTest {

    private Connection duckDB;
    private InstanceRegistry instanceRegistry;
    private ReconciliationService service;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, "orders.events");
        DuckDBTestSupport.createEntityTable(duckDB, "order");

        instanceRegistry = DuckDBTestSupport.newInstanceRegistry(duckDB);
        JoxetteProperties props = new JoxetteProperties();
        // Public constructor (the package-private one used by CompactionServiceTest is only
        // accessible from com.joxette.compaction) — instanceId comes from instanceRegistry
        // itself; no joxette_instances row is needed since ReconciliationService never calls
        // cleanLocksForDeadInstances().
        CompactionLockManager lockManager = new CompactionLockManager(duckDB, props, instanceRegistry);

        service = new ReconciliationService(duckDB, props, lockManager,
                new JoxetteMetrics(new SimpleMeterRegistry()), new ObjectMapper());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
    }

    @Test
    void beginRun_insertsRunningRow() throws Exception {
        ReconciliationRun run = service.beginRun(TriggerSource.MANUAL, null, false);

        assertThat(run.id()).isPositive();
        assertThat(run.status()).isEqualTo(RunStatus.RUNNING);
        assertThat(run.triggeredBy()).isEqualTo(TriggerSource.MANUAL);
        assertThat(service.isRunning()).isTrue();
    }

    @Test
    void beginRun_whileAlreadyRunning_throwsConflict() throws Exception {
        service.beginRun(TriggerSource.MANUAL, null, false);

        assertThatThrownBy(() -> service.beginRun(TriggerSource.MANUAL, null, false))
                .isInstanceOf(com.joxette.api.error.ConflictException.class)
                .hasMessageContaining("Reconciliation");
    }

    /**
     * {@code scanOrphanedFiles()} calls real DuckLake-only functions
     * ({@code ducklake_delete_orphaned_files}), which do not exist against
     * {@link DuckDBTestSupport}'s fake {@code ATTACH ':memory:' AS lake} (not a real
     * {@code ducklake:} catalog) — only {@code @SpringBootTest} + Testcontainers-MinIO
     * IT tests (e.g. {@code ReconciliationOrphanedFilesIT}) can exercise a real scan.
     * This test instead verifies the error-handling contract that IS reachable here:
     * a scan failure marks the run FAILED (not left RUNNING forever) and releases
     * both the local guard and the distributed lock.
     */
    @Test
    void executeRun_whenScanFails_marksRunFailedAndReleasesGuards() throws Exception {
        ReconciliationRun started = service.beginRun(TriggerSource.MANUAL, null, false);

        service.executeRun(started.id(), null, false);

        assertThat(service.isRunning()).isFalse();
        ReconciliationStatus status = service.getStatus();
        assertThat(status.running()).isFalse();
        assertThat(status.lastRun().id()).isEqualTo(started.id());
        assertThat(status.lastRun().status()).isEqualTo(RunStatus.FAILED);
        assertThat(status.lastRun().errorMessage()).isNotBlank();
        // tablesScanned is computed before the scan runs, so it survives the failure.
        assertThat(status.lastRun().tablesScanned()).isEqualTo(2); // general_orders_events + entity_order

        // The lock/guard release must have actually happened, not just been attempted —
        // a fresh beginRun() must succeed rather than throw ConflictException.
        ReconciliationRun next = service.beginRun(TriggerSource.MANUAL, null, false);
        assertThat(next.id()).isNotEqualTo(started.id());
    }

    @Test
    void updateRunRecord_writesAllBookkeepingFieldsBackCorrectly() throws Exception {
        ReconciliationRun started = service.beginRun(TriggerSource.MANUAL, List.of("orders.events"), true);

        service.updateRunRecord(started.id(), RunStatus.COMPLETED, 2, 3, 12345L, 1, 678L, 3, null, null);

        ReconciliationRun updated = service.getRunById(started.id());
        assertThat(updated.status()).isEqualTo(RunStatus.COMPLETED);
        assertThat(updated.targets()).containsExactly("orders.events");
        assertThat(updated.tablesScanned()).isEqualTo(2);
        assertThat(updated.orphanedFiles()).isEqualTo(3);
        assertThat(updated.orphanedBytes()).isEqualTo(12345L);
        assertThat(updated.missingFiles()).isEqualTo(1);
        assertThat(updated.missingBytes()).isEqualTo(678L);
        assertThat(updated.recoveredFiles()).isEqualTo(3);
        assertThat(updated.recoveryRequested()).isTrue();
        assertThat(updated.completedAt()).isNotNull();
    }

    @Test
    void getHistory_returnsRunsNewestFirst() throws Exception {
        // insertRunRecord/updateRunRecord bypass the running-flag/distributed-lock guard
        // entirely (that guard is beginRun/executeRun's concern, already covered by the
        // tests above) — appropriate here since this test only cares about ORDER BY
        // started_at DESC, not concurrency control.
        long firstId = service.insertRunRecord(TriggerSource.MANUAL, null, false);
        service.updateRunRecord(firstId, RunStatus.COMPLETED, 1, 0, 0, 0, 0, 0, null, null);
        long secondId = service.insertRunRecord(TriggerSource.SCHEDULED, List.of("orders.events"), false);
        service.updateRunRecord(secondId, RunStatus.COMPLETED, 1, 0, 0, 0, 0, 0, null, null);

        List<ReconciliationRun> history = service.getHistory(20);

        assertThat(history).hasSize(2);
        assertThat(history.get(0).id()).isEqualTo(secondId);
        assertThat(history.get(1).id()).isEqualTo(firstId);
    }
}
