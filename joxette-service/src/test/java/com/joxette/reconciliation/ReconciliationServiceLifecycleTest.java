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

    @Test
    void executeRun_withStubbedScans_completesAndCountsTables() throws Exception {
        ReconciliationRun started = service.beginRun(TriggerSource.MANUAL, null, false);

        service.executeRun(started.id(), null, false);

        assertThat(service.isRunning()).isFalse();
        ReconciliationStatus status = service.getStatus();
        assertThat(status.running()).isFalse();
        assertThat(status.lastRun().id()).isEqualTo(started.id());
        assertThat(status.lastRun().status()).isEqualTo(RunStatus.COMPLETED);
        assertThat(status.lastRun().tablesScanned()).isEqualTo(2); // general_orders_events + entity_order
        assertThat(status.lastRun().orphanedFiles()).isZero();
        assertThat(status.lastRun().missingFiles()).isZero();
    }

    @Test
    void getHistory_returnsRunsNewestFirst() throws Exception {
        ReconciliationRun first = service.beginRun(TriggerSource.MANUAL, null, false);
        service.executeRun(first.id(), null, false);
        ReconciliationRun second = service.beginRun(TriggerSource.SCHEDULED, List.of("orders.events"), false);
        service.executeRun(second.id(), List.of("orders.events"), false);

        List<ReconciliationRun> history = service.getHistory(20);

        assertThat(history).hasSize(2);
        assertThat(history.get(0).id()).isEqualTo(second.id());
        assertThat(history.get(1).id()).isEqualTo(first.id());
    }
}
