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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
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
 *
 * <p>Also proves that on a multi-partition topic, a poison partition never holds
 * healthy co-batched partitions hostage: {@link DuckLakeWriteChannel} splits a
 * multi-partition batch by partition on non-retryable failure
 * ({@link WriteBatch#splitByPartition()}), so quarantine identity is always
 * {@code (topic, single partition, min-offset)} — stable across restarts
 * regardless of which other partitions happen to get coalesced alongside it by
 * upstream batching.
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

    @Test
    void mixedPartitionBatch_isolatesPoisonPartitionAndNeverLosesHealthyPartitionRecords() throws SQLException {
        // The general cassette table for this topic exists (unlike the single-
        // partition test above) with a CHECK constraint that always rejects the
        // POISON_KEY partition-0 records — a genuinely poison record tied to ONE
        // partition, while partition 1's differently-keyed records always write
        // cleanly. This is the multi-partition scenario the whole-batch identity
        // got wrong: which partitions land in the same WriteBatch varies by poll
        // timing, so we deliberately vary the bundling across "restarts" below.
        try (Statement st = duckDB.createStatement()) {
            st.execute(String.format("""
                    CREATE TABLE lake.main.general_%s (
                        recorded_at     TIMESTAMPTZ NOT NULL,
                        kafka_offset    BIGINT      NOT NULL,
                        kafka_partition INTEGER     NOT NULL,
                        kafka_timestamp TIMESTAMPTZ NOT NULL,
                        kafka_key       VARCHAR,
                        kafka_value     BLOB,
                        metadata        VARCHAR,
                        headers         STRUCT(key VARCHAR, value VARCHAR)[],
                        message_type    VARCHAR,
                        CHECK (kafka_key != '%s')
                    )""", MULTI_SANITIZED, POISON_KEY));
        }

        // Attempt 1/3 for the poison identity: a MIXED batch (partitions 0 and 1
        // coalesced together, as batchWeighted might produce on a fresh start).
        // The whole submit() must still throw (poison partition unresolved), but
        // partition 1's records must land durably despite that — proving a
        // genuinely poison partition is isolated from a healthy co-batched one
        // within the SAME submit() call, not just across restarts.
        WriteBatch mixed1 = mixedBatch(poisonRecords(0L, 1), healthyRecords(0L, 3));
        assertThatThrownBy(() -> writeChannel.submit(mixed1));
        assertThat(rowCount(MULTI_SANITIZED)).isEqualTo(3L);
        assertThat(quarantinedCount(MULTI_TOPIC)).isZero();

        // Attempt 2/3: simulate the realistic restart case — partition 1's offset
        // committed after attempt 1 (its records are durable), so this "restart"
        // only re-reads partition 0's still-uncommitted poison offset ALONE, with
        // no partition 1 records bundled at all.
        WriteBatch poisonOnly = mixedBatch(poisonRecords(0L, 1), List.of());
        assertThatThrownBy(() -> writeChannel.submit(poisonOnly));
        assertThat(quarantinedCount(MULTI_TOPIC)).isZero();

        // Attempt 3/3 (threshold): a MIXED batch again, but bundled with a
        // DIFFERENT set of partition-1 records than attempt 1. Under the old
        // whole-batch composite identity this would have looked like a brand new
        // "poison batch" and reset the failure counter — the exact bug this
        // scenario regression-tests. With per-partition split, partition 0's
        // identity (topic, partition 0, offset 0) is unaffected by what
        // partition 1 contributes, so it must quarantine here.
        WriteBatch mixed3 = mixedBatch(poisonRecords(0L, 1), healthyRecords(3L, 3));
        WriteResult result;
        try {
            result = writeChannel.submit(mixed3);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // The parent batch completes successfully (no exception): the quarantined
        // poison sub-batch resolves to recordsWritten=0 rather than a failure, so
        // it never blocks the healthy sub-batch's contribution or the parent's
        // offset-commit eligibility.
        assertThat(result.recordsWritten()).isEqualTo(3); // only partition 1's 3 new records
        assertThat(quarantinedCount(MULTI_TOPIC)).isEqualTo(1.0);

        // Partition 1 was NEVER quarantined and never lost a record to partition
        // 0's failures: all 6 of its records (3 from attempt 1 + 3 from attempt 3)
        // are durably present; partition 0 contributed zero rows across all three
        // attempts.
        assertThat(rowCount(MULTI_SANITIZED)).isEqualTo(6L);
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT COUNT(*) FROM lake.main.general_" + MULTI_SANITIZED
                             + " WHERE kafka_partition = " + POISON_PARTITION)) {
            rs.next();
            assertThat(rs.getLong(1)).isZero();
        }
    }

    private static final String MULTI_TOPIC = "quarantine.multi.partition.events";
    private static final String MULTI_SANITIZED = "quarantine_multi_partition_events";
    private static final int POISON_PARTITION = 0;
    private static final int HEALTHY_PARTITION = 1;
    private static final String POISON_KEY = "POISON";

    private long rowCount(String sanitizedTable) throws SQLException {
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM lake.main.general_" + sanitizedTable)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private WriteBatch mixedBatch(List<ConsumerRecord<String, byte[]>> poison,
                                   List<ConsumerRecord<String, byte[]>> healthy) {
        List<ConsumerRecord<String, byte[]>> all = new ArrayList<>(poison.size() + healthy.size());
        all.addAll(poison);
        all.addAll(healthy);
        List<String> types = new ArrayList<>(Collections.nCopies(all.size(), (String) null));
        return WriteBatch.of(MULTI_TOPIC, all, all, types, List.of());
    }

    private List<ConsumerRecord<String, byte[]>> poisonRecords(long baseOffset, int count) {
        List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(new ConsumerRecord<>(
                    MULTI_TOPIC, POISON_PARTITION, baseOffset + i,
                    1_700_000_000_000L + i, TimestampType.CREATE_TIME,
                    -1, -1,
                    POISON_KEY,
                    ("poison-" + i).getBytes(StandardCharsets.UTF_8),
                    new RecordHeaders(),
                    Optional.empty()));
        }
        return records;
    }

    private List<ConsumerRecord<String, byte[]>> healthyRecords(long baseOffset, int count) {
        List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(new ConsumerRecord<>(
                    MULTI_TOPIC, HEALTHY_PARTITION, baseOffset + i,
                    1_700_000_000_000L + i, TimestampType.CREATE_TIME,
                    -1, -1,
                    "healthy-key-" + (baseOffset + i),
                    ("payload-" + i).getBytes(StandardCharsets.UTF_8),
                    new RecordHeaders(),
                    Optional.empty()));
        }
        return records;
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
