package com.joxette.recording;

import com.joxette.config.JoxetteProperties;
import com.joxette.metrics.JoxetteMetrics;
import com.joxette.support.DuckDBTestSupport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that a slow/blocked {@link CassetteRecordingBus#publish} call for one
 * topic's batch does not delay {@link DuckLakeWriteChannel#submit} completing
 * for an UNRELATED batch. Uses a test-double bus subclass that blocks on a
 * latch inside publish() — {@code CassetteRecordingBus} is public and
 * non-final specifically so this kind of test double can be built without any
 * production-code changes to the bus itself.
 */
class DuckLakeWriteChannelBusDecouplingTest {

    private static final String TOPIC_SLOW = "bus.decoupling.slow.topic";
    private static final String TOPIC_FAST = "bus.decoupling.fast.topic";

    private Connection duckDB;
    private DuckLakeWriteChannel writeChannel;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC_SLOW);
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC_FAST);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (writeChannel != null) writeChannel.stop();
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
    }

    @Test
    void slowSubscriberFanout_doesNotDelaySubmitForUnrelatedBatch() throws Exception {
        JoxetteProperties props = new JoxetteProperties();
        CountDownLatch releaseLatch = new CountDownLatch(1);
        CountDownLatch publishStarted = new CountDownLatch(1);
        SlowRecordingBus bus = new SlowRecordingBus(props, releaseLatch, publishStarted);

        writeChannel = new DuckLakeWriteChannel(duckDB, props, bus, new JoxetteMetrics(new SimpleMeterRegistry()));
        writeChannel.start();

        // This batch's write succeeds and its publish() call (on TOPIC_SLOW) blocks
        // on releaseLatch. submit() itself returns once the DB write completes —
        // completion happens before publish() is even dispatched — so wait for
        // publish() to have actually started before measuring the second submit.
        writeChannel.submit(generalBatch(TOPIC_SLOW, 1));
        assertThat(publishStarted.await(5, TimeUnit.SECONDS))
                .as("the slow bus.publish() call must have started")
                .isTrue();

        // A second, unrelated batch on a different topic must complete quickly
        // even though the first publish() call is still blocked on releaseLatch —
        // this is only true if publish() runs off the drain VT.
        long startNanos = System.nanoTime();
        writeChannel.submit(generalBatch(TOPIC_FAST, 1));
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

        assertThat(elapsedMs)
                .as("submit() for an unrelated topic must not wait on a blocked bus.publish() call")
                .isLessThan(2_000);

        releaseLatch.countDown();
    }

    private static WriteBatch generalBatch(String topic, int count) {
        List<ConsumerRecord<String, byte[]>> records = new ArrayList<>(count);
        List<String> types = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ConsumerRecord<String, byte[]> r = new ConsumerRecord<>(
                    topic, 0, i,
                    1_700_000_000_000L + i, TimestampType.CREATE_TIME,
                    -1, -1,
                    "k" + i,
                    ("payload-" + i).getBytes(StandardCharsets.UTF_8),
                    new RecordHeaders(),
                    Optional.empty());
            records.add(r);
            types.add(null);
        }
        return WriteBatch.of(topic, records, records, types, List.of());
    }

    /** Test double: blocks inside publish() until released, to simulate a slow subscriber fanout. */
    private static final class SlowRecordingBus extends CassetteRecordingBus {
        private final CountDownLatch releaseLatch;
        private final CountDownLatch publishStarted;

        SlowRecordingBus(JoxetteProperties props, CountDownLatch releaseLatch, CountDownLatch publishStarted) {
            super(props);
            this.releaseLatch = releaseLatch;
            this.publishStarted = publishStarted;
        }

        @Override
        public void publish(WriteBatch batch) {
            publishStarted.countDown();
            try {
                if (!releaseLatch.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("releaseLatch not released within 10s");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            super.publish(batch);
        }
    }
}
