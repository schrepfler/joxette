package com.joxette.replay;

import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * Orchestrates replay-to-topic operations: reads records from a DuckLake
 * cassette and produces them back onto a Kafka topic.
 *
 * <h2>General cassette replay</h2>
 * <p>Records are streamed from {@code lake.main.general_{topic}} in
 * {@code kafka_timestamp ASC} order (the natural query order), so the target
 * topic receives them in the same wall-clock order as the original topic.
 *
 * <h2>Entity cassette replay</h2>
 * <p>Records are streamed from {@code lake.main.entity_{type}} filtered by
 * {@code entity_id} and ordered by
 * {@code (kafka_timestamp, recorded_at, topic, partition, offset)}.
 * Because all source topics for an entity share a single cassette table and
 * the ORDER BY covers all source topics, this constitutes an implicit
 * merge-sort across source topics by {@code kafka_timestamp}.
 *
 * <h2>Progress reporting</h2>
 * <p>Both methods accept a {@code Consumer<ReplayProgress> progressSink} that
 * is called:
 * <ul>
 *   <li>every {@value #PROGRESS_INTERVAL} successfully sent records
 *       ({@code status = "in_progress"}),</li>
 *   <li>once on successful completion ({@code status = "completed"}),</li>
 *   <li>once on failure ({@code status = "failed"} with {@code errorMessage}).</li>
 * </ul>
 * Callers can wire this sink to an SSE emitter (via
 * {@link SseReplayHandler#streamSse}) or collect the final event synchronously.
 *
 * <h2>Timestamp preservation</h2>
 * <p>Original Kafka timestamps are forwarded to the target topic via the
 * {@link KafkaProducerService} so that downstream consumers see event-time
 * ordering identical to the source topic.
 */
@Service
public class ReplayToTopicService {

    /** Emit a progress event after every N successfully sent records. */
    private static final int PROGRESS_INTERVAL = 100;

    private final TopicReplayService    topicReplayService;
    private final EntityReplayService   entityReplayService;
    private final KafkaProducerService  kafkaProducerService;

    public ReplayToTopicService(
            TopicReplayService   topicReplayService,
            EntityReplayService  entityReplayService,
            KafkaProducerService kafkaProducerService) {
        this.topicReplayService   = topicReplayService;
        this.entityReplayService  = entityReplayService;
        this.kafkaProducerService = kafkaProducerService;
    }

    // =========================================================================
    // General cassette → Kafka
    // =========================================================================

    /**
     * Streams all matching records from the general cassette for {@code sourceTopic}
     * and produces each one to {@code req.targetTopic()}, preserving
     * {@code kafka_timestamp} ordering.
     *
     * <p>Inter-message delays are scaled by {@code speedMultiplier}: a value of
     * {@code 1.0} replays in real-time, {@code 2.0} replays at double speed
     * (half the delays), {@code 0.5} replays at half speed (double the delays).
     * Pass {@code Double.MAX_VALUE} or any very large value to replay with no
     * intentional delay.
     *
     * <p>The {@code progressSink} is invoked with an {@code "in_progress"}
     * snapshot every {@value #PROGRESS_INTERVAL} records and with a final
     * {@code "completed"} or {@code "failed"} event when the replay ends.
     *
     * @param speedMultiplier replay speed factor; must be &gt; 0
     * @throws SQLException if the DuckDB read fails before any Kafka sends occur
     */
    public void replayTopicToKafka(
            String sourceTopic,
            ReplayToTopicRequest req,
            double speedMultiplier,
            Consumer<ReplayProgress> progressSink
    ) throws SQLException {
        long[]    counts  = {0L, 0L};   // [0]=sent [1]=errors
        Instant[] lastTs  = {null};
        Instant[] prevTs  = {null};

        try {
            topicReplayService.streamAll(
                    sourceTopic, req.from(), req.to(),
                    req.partition(), req.offsetFrom(), req.offsetTo(),
                    record -> {
                        applyDelay(prevTs[0], record.timestamp(), speedMultiplier);
                        prevTs[0] = record.timestamp();
                        lastTs[0] = record.timestamp();
                        doSend(() -> kafkaProducerService.send(req.targetTopic(), record), counts);
                        if (counts[0] % PROGRESS_INTERVAL == 0) {
                            progressSink.accept(inProgress(req.targetTopic(), counts, lastTs[0]));
                        }
                    }
            );
        } catch (RuntimeException ex) {
            progressSink.accept(failed(req.targetTopic(), counts, lastTs[0], ex.getMessage()));
            return;
        }

        progressSink.accept(completed(req.targetTopic(), counts, lastTs[0]));
    }

    // =========================================================================
    // Entity cassette → Kafka
    // =========================================================================

    /**
     * Streams all matching events for {@code entityId} from the entity cassette
     * {@code lake.main.entity_{entityType}} and produces each one to
     * {@code req.targetTopic()}.
     *
     * <p>Events are ordered by
     * {@code (kafka_timestamp, recorded_at, topic, partition, offset)},
     * which constitutes a merge-sort across all source topics by
     * {@code kafka_timestamp}.
     *
     * <p>Inter-message delays are scaled by {@code speedMultiplier} identically
     * to {@link #replayTopicToKafka}.
     *
     * <p>The {@code progressSink} contract is identical to
     * {@link #replayTopicToKafka}.
     *
     * @param speedMultiplier replay speed factor; must be &gt; 0
     * @throws SQLException if the DuckDB read fails before any Kafka sends occur
     */
    public void replayEntityToKafka(
            String entityType,
            String entityId,
            ReplayToTopicRequest req,
            double speedMultiplier,
            Consumer<ReplayProgress> progressSink
    ) throws SQLException {
        long[]    counts = {0L, 0L};
        Instant[] lastTs = {null};
        Instant[] prevTs = {null};

        try {
            entityReplayService.streamEntityEvents(
                    entityType, entityId, req.from(), req.to(),
                    record -> {
                        applyDelay(prevTs[0], record.timestamp(), speedMultiplier);
                        prevTs[0] = record.timestamp();
                        lastTs[0] = record.timestamp();
                        doSend(() -> kafkaProducerService.send(req.targetTopic(), record), counts);
                        if (counts[0] % PROGRESS_INTERVAL == 0) {
                            progressSink.accept(inProgress(req.targetTopic(), counts, lastTs[0]));
                        }
                    }
            );
        } catch (RuntimeException ex) {
            progressSink.accept(failed(req.targetTopic(), counts, lastTs[0], ex.getMessage()));
            return;
        }

        progressSink.accept(completed(req.targetTopic(), counts, lastTs[0]));
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /**
     * Sleeps for the scaled inter-message delay if {@code prev} is not null and
     * {@code current} is strictly after {@code prev}.
     *
     * <pre>
     * delay = (current − prev).toMillis() / speedMultiplier
     * </pre>
     *
     * <p>A {@code speedMultiplier} of {@code 1.0} replays in real-time;
     * {@code 2.0} halves the gaps; {@code 0.5} doubles them.
     */
    private static void applyDelay(Instant prev, Instant current, double speedMultiplier) {
        if (prev == null || !current.isAfter(prev)) {
            return;
        }
        long delayMs = (long) ((current.toEpochMilli() - prev.toEpochMilli()) / speedMultiplier);
        if (delayMs <= 0) {
            return;
        }
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Replay interrupted during inter-message delay", e);
        }
    }

    @FunctionalInterface
    private interface SendAction {
        java.util.concurrent.Future<?> execute() throws Exception;
    }

    /**
     * Executes a send action synchronously.  On success increments {@code counts[0]}.
     * On failure increments {@code counts[1]} and rethrows as {@link RuntimeException}
     * to abort the streaming pipeline.
     */
    private static void doSend(SendAction action, long[] counts) {
        try {
            action.execute().get();
            counts[0]++;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Replay interrupted during Kafka send", e);
        } catch (ExecutionException e) {
            counts[1]++;
            throw new RuntimeException(
                    "Kafka send failed: " + e.getCause().getMessage(), e.getCause());
        } catch (Exception e) {
            counts[1]++;
            throw new RuntimeException("Kafka send failed: " + e.getMessage(), e);
        }
    }

    private static ReplayProgress inProgress(String target, long[] counts, Instant ts) {
        return new ReplayProgress("in_progress", target, counts[0], counts[1], ts, null);
    }

    private static ReplayProgress completed(String target, long[] counts, Instant ts) {
        return new ReplayProgress("completed", target, counts[0], counts[1], ts, null);
    }

    private static ReplayProgress failed(String target, long[] counts, Instant ts, String msg) {
        return new ReplayProgress("failed", target, counts[0], counts[1], ts, msg);
    }
}
