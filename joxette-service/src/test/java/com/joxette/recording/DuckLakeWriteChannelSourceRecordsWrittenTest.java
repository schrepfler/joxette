package com.joxette.recording;

import com.joxette.config.JoxetteProperties;
import com.joxette.metrics.JoxetteMetrics;
import com.joxette.replay.EntityRoute;
import com.joxette.replay.KafkaMessage;
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

/**
 * Proves {@link WriteResult#sourceRecordsWritten()} tracks distinct SOURCE Kafka
 * messages, not rows written — the two diverge for a {@code both}/{@code entity_only}
 * mode topic where a single source message fans out into one general row PLUS one or
 * more entity routes. Before this fix, {@code TopicRecorder} used
 * {@code recordsWritten()} (the row count) for {@code messagesWritten}/
 * {@code joxette.messages.written}, which over-counts relative to
 * {@code messagesConsumed} on exactly this kind of batch.
 */
class DuckLakeWriteChannelSourceRecordsWrittenTest {

    private static final String TOPIC = "source.records.written.test.events";
    private static final String ENTITY_TYPE = "order";

    private Connection duckDB;
    private DuckLakeWriteChannel writeChannel;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC);
        DuckDBTestSupport.createEntityTable(duckDB, ENTITY_TYPE);

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
    void bothModeBatch_sourceRecordsWrittenCountsMessagesNotRows() throws Exception {
        // 2 source messages; each is written as ONE general row AND routed to TWO
        // entity routes (e.g. two entity_source_mappings matching the same message).
        // recordsWritten (rows) must be 2 general + 4 entity = 6.
        // sourceRecordsWritten (messages) must be 2 — matching messagesConsumed.
        int sourceMessageCount = 2;
        int routesPerMessage = 2;

        List<ConsumerRecord<String, byte[]>> sourceRecords = new ArrayList<>();
        List<WriteBatch.EntityWriteItem> entityItems = new ArrayList<>();
        for (int i = 0; i < sourceMessageCount; i++) {
            ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                    TOPIC, 0, i,
                    1_700_000_000_000L + i, TimestampType.CREATE_TIME,
                    -1, -1,
                    "order-" + i,
                    ("payload-" + i).getBytes(StandardCharsets.UTF_8),
                    new RecordHeaders(),
                    Optional.empty());
            sourceRecords.add(record);

            KafkaMessage msg = new KafkaMessage(
                    TOPIC, 0, i, record.timestamp(),
                    "order-" + i, record.value().clone(), List.of());
            List<EntityRoute> routes = new ArrayList<>(routesPerMessage);
            for (int r = 0; r < routesPerMessage; r++) {
                routes.add(new EntityRoute(ENTITY_TYPE, "order-" + i, 0, "type" + r, TOPIC));
            }
            entityItems.add(new WriteBatch.EntityWriteItem(routes, msg));
        }
        List<String> messageTypes = new ArrayList<>();
        for (int i = 0; i < sourceMessageCount; i++) messageTypes.add(null);

        WriteBatch batch = WriteBatch.of(TOPIC, sourceRecords, sourceRecords, messageTypes, entityItems);

        WriteResult result = writeChannel.submit(batch);

        assertThat(result.sourceRecordsWritten())
                .as("sourceRecordsWritten must equal distinct source Kafka messages, not fan-out rows")
                .isEqualTo(sourceMessageCount);
        assertThat(result.recordsWritten())
                .as("recordsWritten (rows) = general rows + entity routes, and must stay the row count, unchanged")
                .isEqualTo(sourceMessageCount + sourceMessageCount * routesPerMessage);
        assertThat(result.recordsWritten())
                .as("this batch shape is exactly where the two must diverge")
                .isNotEqualTo(result.sourceRecordsWritten());
    }
}
