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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves two properties of {@link DuckLakeWriteChannel}'s dispatch of
 * {@link CassetteRecordingBus#publish}: (1) a slow/blocked publish() call for one
 * topic's batch does not delay {@link DuckLakeWriteChannel#submit} completing for
 * an UNRELATED batch, and (2) consecutive same-topic batches are still delivered
 * to publish() in strict submission order (a dedicated single-threaded worker is
 * required for this — a per-task executor would not guarantee it). Uses a
 * test-double bus subclass in each case — {@code CassetteRecordingBus} is public
 * and non-final specifically so this kind of test double can be built without any
 * production-code changes to the bus itself.
 */
class DuckLakeWriteChannelBusDecouplingTest {

    private static final String TOPIC_SLOW = "bus.decoupling.slow.topic";
    private static final String TOPIC_FAST = "bus.decoupling.fast.topic";
    private static final String TOPIC_ORDER = "bus.decoupling.order.topic";

    private Connection duckDB;
    private DuckLakeWriteChannel writeChannel;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC_SLOW);
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC_FAST);
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC_ORDER);
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

    /**
     * Submits many consecutive same-topic batches and asserts {@code publish()} is
     * invoked in exactly the order they were submitted. This is the scenario a
     * per-task executor (e.g. {@code Executors.newVirtualThreadPerTaskExecutor()})
     * cannot guarantee: independently-scheduled virtual threads give no relative
     * ordering, so two consecutive same-topic batches could be delivered to a
     * {@code follow=true} subscriber out of cursor order. A large batch count is
     * used to make a reordering regression reliably detectable rather than
     * relying on a single narrow race window.
     */
    @Test
    void sameTopicBatches_publishedInSubmissionOrder() throws Exception {
        JoxetteProperties props = new JoxetteProperties();
        OrderRecordingBus bus = new OrderRecordingBus(props);

        writeChannel = new DuckLakeWriteChannel(duckDB, props, bus, new JoxetteMetrics(new SimpleMeterRegistry()));
        writeChannel.start();

        int batchCount = 200;
        List<Long> expectedOrder = new ArrayList<>(batchCount);
        for (long offset = 0; offset < batchCount; offset++) {
            expectedOrder.add(offset);
            writeChannel.submit(generalBatch(TOPIC_ORDER, offset, 1));
        }

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(bus.publishedOffsets()).hasSize(batchCount));

        assertThat(bus.publishedOffsets())
                .as("publish() must be invoked in exactly submission order for consecutive same-topic batches")
                .containsExactlyElementsOf(expectedOrder);
    }

    private static WriteBatch generalBatch(String topic, int count) {
        return generalBatch(topic, 0, count);
    }

    private static WriteBatch generalBatch(String topic, long baseOffset, int count) {
        List<ConsumerRecord<String, byte[]>> records = new ArrayList<>(count);
        List<String> types = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long offset = baseOffset + i;
            ConsumerRecord<String, byte[]> r = new ConsumerRecord<>(
                    topic, 0, offset,
                    1_700_000_000_000L + offset, TimestampType.CREATE_TIME,
                    -1, -1,
                    "k" + offset,
                    ("payload-" + offset).getBytes(StandardCharsets.UTF_8),
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

    /**
     * Test double: records the offset of the first general record in each batch,
     * in the order {@code publish()} is actually invoked, so a test can compare
     * that order against submission order.
     */
    private static final class OrderRecordingBus extends CassetteRecordingBus {
        private final List<Long> publishedOffsets = Collections.synchronizedList(new ArrayList<>());

        OrderRecordingBus(JoxetteProperties props) {
            super(props);
        }

        @Override
        public void publish(WriteBatch batch) {
            publishedOffsets.add(batch.generalRecords().get(0).offset());
            super.publish(batch);
        }

        List<Long> publishedOffsets() {
            synchronized (publishedOffsets) {
                return new ArrayList<>(publishedOffsets);
            }
        }
    }
}
