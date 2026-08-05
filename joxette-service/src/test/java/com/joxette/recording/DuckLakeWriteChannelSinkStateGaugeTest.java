package com.joxette.recording;

import com.joxette.config.JoxetteProperties;
import com.joxette.metrics.JoxetteMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@code joxette.sink.state} gauge (Task C, ops-deployment-readiness plan)
 * tracks {@link DuckLakeWriteChannel}'s internal {@code SinkState} through all three
 * values, using the exact retained-supplier pattern documented in
 * {@link JoxetteMetrics#registerSinkStateGauge} (see also the class javadoc's note on
 * the past weak-reference NaN bug this pattern exists to prevent).
 *
 * <p>{@code SinkState} only transitions to DEGRADED/FAILED from inside the drain loop
 * on a real (retryable) DuckDB write failure — reaching those states deterministically
 * from a unit test would require fault-injecting the JDBC layer. Since this test's
 * purpose is to verify the *gauge wiring*, not the retry state machine (which is
 * exercised indirectly wherever {@code DuckLakeWriteChannel} is used end-to-end), it
 * drives the transitions directly via the private {@code sinkState} field — reflection
 * is used here only because {@code SinkState} intentionally has no production setter
 * beyond {@link DuckLakeWriteChannel#resetSinkState()} (which only ever sets HEALTHY).
 *
 * <p>No {@code Awaitility} is needed: the gauge is a synchronous pull — reading it calls
 * the supplier immediately on the calling (test) thread, with no cross-thread handoff.
 */
class DuckLakeWriteChannelSinkStateGaugeTest {

    private SimpleMeterRegistry registry;
    private JoxetteMetrics metrics;
    private Connection duckDB;
    private DuckLakeWriteChannel writeChannel;

    @BeforeEach
    void setUp() throws Exception {
        registry = new SimpleMeterRegistry();
        metrics = new JoxetteMetrics(registry);
        duckDB = DriverManager.getConnection("jdbc:duckdb:");
        JoxetteProperties props = new JoxetteProperties();
        writeChannel = new DuckLakeWriteChannel(duckDB, props, new CassetteRecordingBus(props), metrics);
        writeChannel.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        writeChannel.stop();
        duckDB.close();
    }

    @Test
    void gaugeTracksSinkStateTransitions() throws Exception {
        assertThat(gaugeValue()).as("initial state is HEALTHY").isEqualTo(0.0);

        setSinkState(DuckLakeWriteChannel.SinkState.DEGRADED);
        assertThat(gaugeValue()).as("DEGRADED").isEqualTo(1.0);

        setSinkState(DuckLakeWriteChannel.SinkState.FAILED);
        assertThat(gaugeValue()).as("FAILED").isEqualTo(2.0);

        writeChannel.resetSinkState();
        assertThat(gaugeValue()).as("reset back to HEALTHY").isEqualTo(0.0);
    }

    private double gaugeValue() {
        return registry.get("joxette.sink.state").gauge().value();
    }

    @SuppressWarnings("unchecked")
    private void setSinkState(DuckLakeWriteChannel.SinkState state) throws Exception {
        Field field = DuckLakeWriteChannel.class.getDeclaredField("sinkState");
        field.setAccessible(true);
        ((AtomicReference<DuckLakeWriteChannel.SinkState>) field.get(writeChannel)).set(state);
    }
}
