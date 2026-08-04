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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves that a batch which deterministically fails with a non-retryable DuckDB
 * error is skipped ("quarantined") after {@code quarantineAfterAttempts}
 * re-submissions of the SAME underlying offsets — mirroring what happens across
 * repeated actor restarts when a poison message can never be written — instead
 * of failing forever with zero progress.
 */
class DuckLakeWriteChannelQuarantineTest {

    private static final String TOPIC = "quarantine.test.events";
    private static final int QUARANTINE_AFTER_ATTEMPTS = 3;

    private Connection duckDB;
    private DuckLakeWriteChannel writeChannel;
    private SimpleMeterRegistry registry;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        // Deliberately never create lake.main.general_quarantine_test_events —
        // every write attempt fails with a non-retryable "Catalog Error".

        JoxetteProperties props = new JoxetteProperties();
        props.getThreading().setQuarantineAfterAttempts(QUARANTINE_AFTER_ATTEMPTS);

        registry = new SimpleMeterRegistry();
        writeChannel = new DuckLakeWriteChannel(duckDB, props, new CassetteRecordingBus(props),
                new JoxetteMetrics(registry));
        writeChannel.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (writeChannel != null) writeChannel.stop();
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
    }

    @Test
    void samePoisonBatch_isQuarantinedAfterConfiguredAttempts_thenLetsTopicProgress() {
        // Attempts 1 and 2 (below the threshold): submit() must still throw — this
        // is today's existing fail-fast-but-never-committed behaviour, unchanged.
        for (int attempt = 1; attempt <= QUARANTINE_AFTER_ATTEMPTS - 1; attempt++) {
            WriteBatch batch = poisonBatch(0L);
            assertThatThrownBy(() -> writeChannel.submit(batch));
        }
        assertThat(extractionFailureFreeCounter()).isZero(); // sanity: nothing quarantined yet

        // Attempt == threshold: the channel gives up retrying this exact identity,
        // logs it, and completes the batch successfully with zero records written
        // so the recorder can commit past it instead of restarting forever.
        WriteBatch quarantined = poisonBatch(0L);
        WriteResult result;
        try {
            result = writeChannel.submit(quarantined);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        assertThat(result.recordsWritten()).isZero();

        assertThat(quarantinedCount(TOPIC)).isEqualTo(1.0);

        // A batch with a DIFFERENT offset identity must NOT be quarantined by the
        // previous batch's failures — only the specific poison identity is skipped.
        WriteBatch differentIdentity = poisonBatch(100L);
        assertThatThrownBy(() -> writeChannel.submit(differentIdentity));
    }

    private double quarantinedCount(String topic) {
        var counter = registry.find("joxette.recording.batches_quarantined").tag("topic", topic).counter();
        return counter == null ? 0.0 : counter.count();
    }

    private double extractionFailureFreeCounter() {
        var counter = registry.find("joxette.recording.batches_quarantined").tag("topic", TOPIC).counter();
        return counter == null ? 0.0 : counter.count();
    }

    private WriteBatch poisonBatch(long baseOffset) {
        List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
        List<String> types = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            ConsumerRecord<String, byte[]> r = new ConsumerRecord<>(
                    TOPIC, 0, baseOffset + i,
                    1_700_000_000_000L + i, TimestampType.CREATE_TIME,
                    -1, -1,
                    "k" + i,
                    ("payload-" + i).getBytes(StandardCharsets.UTF_8),
                    new RecordHeaders(),
                    Optional.empty());
            records.add(r);
            types.add(null);
        }
        return WriteBatch.of(TOPIC, records, records, types, List.of());
    }
}
