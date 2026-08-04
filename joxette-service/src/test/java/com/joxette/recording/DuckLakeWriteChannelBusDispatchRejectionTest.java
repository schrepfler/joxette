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

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces the race described in the write-resilience review finding: the drain VT
 * can still be mid-{@code processBatch} — past the point where the write has already
 * succeeded and {@code batch.result()} has already been completed — when
 * {@code busPublishExecutor.execute(...)} is called, but the executor has ALREADY been
 * shut down by a concurrent {@link DuckLakeWriteChannel#stop()} call. Before the fix,
 * the resulting {@link java.util.concurrent.RejectedExecutionException} fell into
 * {@code processBatch}'s outer {@code catch (Exception e)} block and re-entered the
 * non-retryable-failure path for a batch that had already succeeded. For a
 * MULTI-partition batch (the only shape where the symptom is externally observable —
 * {@code batch.result()} is already complete so re-completing it is a silent no-op),
 * that path is {@code splitAndProcessByPartition}, which re-writes every partition's
 * records a SECOND time — duplicate rows in the general cassette table, even though
 * {@code submit()} itself returns successfully and never surfaces the problem to the
 * caller.
 *
 * <p>Shuts down {@code busPublishExecutor} directly via reflection (rather than calling
 * the full {@link DuckLakeWriteChannel#stop()}, which would also tear down the drain
 * thread) to isolate exactly this race without needing real concurrent timing.
 */
class DuckLakeWriteChannelBusDispatchRejectionTest {

    private static final String TOPIC = "bus.dispatch.rejection.test.events";

    private Connection duckDB;
    private DuckLakeWriteChannel writeChannel;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC);

        JoxetteProperties props = new JoxetteProperties();
        writeChannel = new DuckLakeWriteChannel(duckDB, props, new CassetteRecordingBus(props),
                new JoxetteMetrics(new SimpleMeterRegistry()));
        writeChannel.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (writeChannel != null) writeChannel.stop();
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
    }

    @Test
    void rejectedBusDispatch_doesNotDuplicateAnAlreadySuccessfulMultiPartitionWrite() throws Exception {
        Field f = DuckLakeWriteChannel.class.getDeclaredField("busPublishExecutor");
        f.setAccessible(true);
        ExecutorService busPublishExecutor = (ExecutorService) f.get(writeChannel);
        busPublishExecutor.shutdownNow(); // simulate stop()'s shutdown racing ahead of processBatch

        // Two partitions so the write is already durable by the time execute() is
        // called on a dead executor — this is what makes splitAndProcessByPartition's
        // re-write observable as DUPLICATE rows rather than a silently-swallowed
        // completeExceptionally() no-op on an already-successful single-partition batch.
        WriteBatch batch = multiPartitionBatch(TOPIC, 2, 3);

        WriteResult result = writeChannel.submit(batch);

        assertThat(result.sourceRecordsWritten()).isEqualTo(6);
        assertThat(DuckDBTestSupport.countRows(duckDB, "lake.main.general_" + normalize(TOPIC)))
                .as("a rejected bus-dispatch execute() on an already-successful write must not " +
                        "trigger a duplicate re-write via splitAndProcessByPartition")
                .isEqualTo(6L);
    }

    private static String normalize(String topic) {
        return topic.toLowerCase().replaceAll("[^a-z0-9_]", "_");
    }

    private static WriteBatch multiPartitionBatch(String topic, int partitionCount, int recordsPerPartition) {
        List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
        List<String> types = new ArrayList<>();
        for (int p = 0; p < partitionCount; p++) {
            for (int i = 0; i < recordsPerPartition; i++) {
                records.add(new ConsumerRecord<>(
                        topic, p, i,
                        1_700_000_000_000L + i, TimestampType.CREATE_TIME,
                        -1, -1,
                        "k" + p + "-" + i, ("payload-" + p + "-" + i).getBytes(StandardCharsets.UTF_8),
                        new RecordHeaders(),
                        Optional.empty()));
                types.add(null);
            }
        }
        return WriteBatch.of(topic, records, records, types, List.of());
    }
}
