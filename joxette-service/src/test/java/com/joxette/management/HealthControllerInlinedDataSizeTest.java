package com.joxette.management;

import com.joxette.cluster.InstanceRegistry;
import com.joxette.config.BrokerConnectionFactory;
import com.joxette.config.JoxetteProperties;
import com.joxette.lifecycle.BackgroundTaskRegistry;
import com.joxette.metrics.JoxetteMetrics;
import com.joxette.recording.RecordingCoordinator;
import com.joxette.support.DuckDBTestSupport;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.sql.Connection;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Reproduces the reported symptom: GET /health always reports
 * {@code inlinedDataSizeBytes = 0}, even when the DuckLake catalog holds
 * rows that have not been flushed to Parquet.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class HealthControllerInlinedDataSizeTest {

    @Mock RecordingCoordinator coordinator;
    @Mock BrokerConnectionFactory brokerConnectionFactory;
    @Mock InstanceRegistry instanceRegistry;

    private Connection duckDB;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, "orders.events");
        DuckDBTestSupport.insertCassetteRow(duckDB, "orders.events", 0, 0L,
                Instant.now(), Instant.now(), "key-1", "value-1".getBytes());
    }

    @AfterEach
    void tearDown() throws Exception {
        duckDB.close();
    }

    @Test
    void health_reportsNonZeroInlinedDataSize_whenCassetteTableHasRows() {
        when(coordinator.listRunning()).thenReturn(Map.of());
        when(instanceRegistry.listAll()).thenReturn(List.of());
        when(instanceRegistry.getInstanceId()).thenReturn("test-instance");

        BackgroundTaskRegistry taskRegistry = new BackgroundTaskRegistry();
        taskRegistry.start();

        HealthController controller = new HealthController(
                coordinator, new JoxetteProperties(), brokerConnectionFactory, duckDB,
                new PrometheusMeterRegistry(PrometheusConfig.DEFAULT), instanceRegistry, taskRegistry,
                new JoxetteMetrics(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT)));

        HealthController.HealthStatus status = controller.health();

        assertThat(status.inlinedDataSizeBytes())
                .as("lake.main.general_orders_events has rows not yet flushed to Parquet")
                .isGreaterThan(0);
    }
}
