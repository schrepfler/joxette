package com.joxette.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JoxetteMetricsReconciliationGaugeTest {

    @Test
    void reconciliationOrphanedFilesGauge_reflectsSupplierValue() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        JoxetteMetrics metrics = new JoxetteMetrics(registry);
        java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger(0);

        metrics.registerReconciliationOrphanedFilesGauge(count::get);
        count.set(7);

        assertThat(registry.get("joxette.reconciliation.orphaned.files").gauge().value()).isEqualTo(7.0);
    }

    @Test
    void reconciliationMissingFilesGauge_reflectsSupplierValue() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        JoxetteMetrics metrics = new JoxetteMetrics(registry);
        java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger(0);

        metrics.registerReconciliationMissingFilesGauge(count::get);
        count.set(3);

        assertThat(registry.get("joxette.reconciliation.missing.files").gauge().value()).isEqualTo(3.0);
    }
}
