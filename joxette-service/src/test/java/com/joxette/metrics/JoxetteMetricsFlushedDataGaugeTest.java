package com.joxette.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JoxetteMetricsFlushedDataGaugeTest {

    @Test
    void flushedDataGauge_reflectsSupplierValue() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        JoxetteMetrics metrics = new JoxetteMetrics(registry);
        java.util.concurrent.atomic.AtomicLong bytes = new java.util.concurrent.atomic.AtomicLong(0);

        metrics.registerFlushedDataGauge(bytes::get);
        bytes.set(123456789L);

        assertThat(registry.get("joxette.catalog.flushed.bytes").gauge().value()).isEqualTo(123456789.0);
    }
}
