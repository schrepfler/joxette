package com.joxette.replay;

import com.joxette.replay.sink.RecordSink;
import com.joxette.replay.sink.SinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Orchestrates replay-to-topic operations: pulls records from a
 * {@link CassetteSource} or {@link EntityCassetteSource} and hands them to a
 * {@link RecordSink} that writes to the destination (typically Kafka).
 *
 * <h2>General cassette replay</h2>
 * <p>Records are streamed in {@code kafka_timestamp ASC} order (the natural
 * source-ordering contract), so the target receives them in the same
 * wall-clock order as the original topic.
 *
 * <h2>Entity cassette replay</h2>
 * <p>Records are streamed ordered by
 * {@code (kafka_timestamp, recorded_at, topic, partition, offset)}, which
 * constitutes an implicit merge-sort across all source topics that fed the
 * entity.
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
 *
 * <h2>Reuse</h2>
 * <p>The engine is pure (no Spring annotations, no Kafka imports) and is
 * intended to be callable from the Joxette service and from test code alike.
 * Production code wires it with a Kafka-backed {@link RecordSink}; tests can
 * wire it with a capturing sink, a Testcontainers-backed sink, or any other
 * implementation.
 */
public class ReplayEngine {

    private static final Logger log = LoggerFactory.getLogger(ReplayEngine.class);

    /** Emit a progress event after every N successfully sent records. */
    private static final int PROGRESS_INTERVAL = 100;

    private final CassetteSource          cassetteSource;
    private final EntityCassetteSource    entitySource;
    private final RecordSink              sink;
    private final Function<String, Integer> partitionCountLookup;

    public ReplayEngine(CassetteSource       cassetteSource,
                        EntityCassetteSource entitySource,
                        RecordSink           sink) {
        this(cassetteSource, entitySource, sink, topic -> 1);
    }

    public ReplayEngine(CassetteSource          cassetteSource,
                        EntityCassetteSource    entitySource,
                        RecordSink              sink,
                        Function<String, Integer> partitionCountLookup) {
        this.cassetteSource       = cassetteSource;
        this.entitySource         = entitySource;
        this.sink                 = sink;
        this.partitionCountLookup = partitionCountLookup;
    }

    // =========================================================================
    // General cassette → sink
    // =========================================================================

    public void replayTopic(
            String sourceTopic,
            ReplayToTopicRequest req,
            double speedMultiplier,
            Consumer<ReplayProgress> progressSink
    ) throws SQLException {
        long[]    counts = {0L, 0L};
        Instant[] lastTs = {null};
        Instant[] prevTs = {null};

        String effectiveTopic = resolveTargetTopic(req, sourceTopic);
        int targetCount = partitionCountLookup.apply(effectiveTopic);
        if (req.partitionStrategy() == PartitionStrategy.PRESERVE) {
            int sourceCount = partitionCountLookup.apply(sourceTopic);
            if (sourceCount != targetCount) {
                throw new IllegalArgumentException(
                        "partitionStrategy=PRESERVE requires equal partition counts: source="
                        + sourceCount + " target=" + targetCount + " topic=" + effectiveTopic);
            }
        }

        MessageTransformer transformer = transformerFor(req);
        log.info("Starting topic replay: source='{}' target='{}' from={} to={} partition={} speed={}x transforms={} partitionStrategy={}",
                sourceTopic, effectiveTopic, req.from(), req.to(), req.partition(),
                speedMultiplier, transformer != null, req.partitionStrategy());

        try {
            cassetteSource.streamAll(
                    sourceTopic, req.from(), req.to(),
                    req.partition(), req.offsetFrom(), req.offsetTo(),
                    record -> {
                        applyDelay(prevTs[0], record.timestamp(), speedMultiplier);
                        prevTs[0] = record.timestamp();
                        CassetteRecord r = transformer != null ? transformer.transform(record) : record;
                        lastTs[0] = r.timestamp();
                        Integer partition = resolvePartition(req.partitionStrategy(), r.partition(), targetCount);
                        doSend(() -> sink.send(effectiveTopic, partition, r), counts);
                        if (counts[0] % PROGRESS_INTERVAL == 0) {
                            progressSink.accept(inProgress(effectiveTopic, counts, lastTs[0]));
                        }
                    }
            );
        } catch (RuntimeException ex) {
            log.warn("Topic replay failed: source='{}' target='{}' sent={} errors={} reason={}",
                    sourceTopic, effectiveTopic, counts[0], counts[1], ex.getMessage());
            progressSink.accept(failed(effectiveTopic, counts, lastTs[0], ex.getMessage()));
            return;
        }

        log.info("Topic replay completed: source='{}' target='{}' sent={} errors={}",
                sourceTopic, effectiveTopic, counts[0], counts[1]);
        progressSink.accept(completed(effectiveTopic, counts, lastTs[0]));
    }

    // =========================================================================
    // Entity cassette → sink
    // =========================================================================

    public void replayEntity(
            String entityType,
            String entityId,
            ReplayToTopicRequest req,
            double speedMultiplier,
            Consumer<ReplayProgress> progressSink
    ) throws SQLException {
        long[]    counts = {0L, 0L};
        Instant[] lastTs = {null};
        Instant[] prevTs = {null};

        // Cache partition counts per effective target to avoid repeated admin lookups.
        // Also tracks which source→target pairs have passed PRESERVE validation.
        Map<String, Integer> targetPartitionCounts = new HashMap<>();
        Map<String, Boolean> preserveValidated = new HashMap<>();

        MessageTransformer transformer = transformerFor(req);
        log.info("Starting entity replay: type='{}' id='{}' target='{}' from={} to={} speed={}x transforms={} partitionStrategy={}",
                entityType, entityId, req.targetTopic(), req.from(), req.to(),
                speedMultiplier, transformer != null, req.partitionStrategy());

        try {
            entitySource.streamEntityEvents(
                    entityType, entityId, req.from(), req.to(),
                    record -> {
                        applyDelay(prevTs[0], record.timestamp(), speedMultiplier);
                        prevTs[0] = record.timestamp();
                        EntityRecord r = transformer != null ? transformer.transform(record) : record;
                        lastTs[0] = r.timestamp();

                        String effectiveTopic = resolveTargetTopic(req, r.topic());
                        int targetCount = targetPartitionCounts.computeIfAbsent(
                                effectiveTopic, partitionCountLookup::apply);

                        if (req.partitionStrategy() == PartitionStrategy.PRESERVE) {
                            String cacheKey = r.topic() + "->" + effectiveTopic;
                            if (!preserveValidated.containsKey(cacheKey)) {
                                int sourceCount = partitionCountLookup.apply(r.topic());
                                if (sourceCount != targetCount) {
                                    throw new IllegalArgumentException(
                                            "partitionStrategy=PRESERVE requires equal partition counts: source="
                                            + sourceCount + " target=" + targetCount
                                            + " sourceTopic=" + r.topic()
                                            + " targetTopic=" + effectiveTopic);
                                }
                                preserveValidated.put(cacheKey, Boolean.TRUE);
                            }
                        }

                        Integer partition = resolvePartition(req.partitionStrategy(), r.partition(), targetCount);
                        doSend(() -> sink.send(effectiveTopic, partition, r), counts);
                        if (counts[0] % PROGRESS_INTERVAL == 0) {
                            progressSink.accept(inProgress(effectiveTopic, counts, lastTs[0]));
                        }
                    }
            );
        } catch (RuntimeException ex) {
            String fallbackTarget = resolveTargetTopic(req, entityType);
            log.warn("Entity replay failed: type='{}' id='{}' target='{}' sent={} errors={} reason={}",
                    entityType, entityId, fallbackTarget, counts[0], counts[1], ex.getMessage());
            progressSink.accept(failed(fallbackTarget, counts, lastTs[0], ex.getMessage()));
            return;
        }

        String fallbackTarget = resolveTargetTopic(req, entityType);
        log.info("Entity replay completed: type='{}' id='{}' target='{}' sent={} errors={}",
                entityType, entityId, fallbackTarget, counts[0], counts[1]);
        progressSink.accept(completed(fallbackTarget, counts, lastTs[0]));
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /**
     * Sleeps for the scaled inter-message delay if {@code prev} is not null and
     * {@code current} is strictly after {@code prev}.
     */
    public static void applyDelay(Instant prev, Instant current, double speedMultiplier) {
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

    /**
     * Resolves the effective target topic for a record from a given source topic.
     * Checks {@code topicMappings} first; falls back to {@code targetTopic};
     * falls back to the source topic name itself (identity routing, useful as
     * the default for entity replays with no explicit target).
     */
    public static String resolveTargetTopic(ReplayToTopicRequest req, String sourceTopic) {
        if (req.topicMappings() != null) {
            String mapped = req.topicMappings().get(sourceTopic);
            if (mapped != null) return mapped;
        }
        return req.targetTopic() != null ? req.targetTopic() : sourceTopic;
    }

    /**
     * Resolves the effective target partition given the chosen strategy.
     *
     * @param strategy           the partition routing strategy
     * @param sourcePartition    the partition number from the cassette record
     * @param targetPartitionCount the partition count of the target topic
     * @return the partition to pass to the sink, or {@code null} to let Kafka choose
     */
    public static Integer resolvePartition(PartitionStrategy strategy,
                                           int sourcePartition,
                                           int targetPartitionCount) {
        return switch (strategy) {
            case DEFAULT  -> null;
            case PRESERVE -> sourcePartition;
            case MODULO   -> sourcePartition % targetPartitionCount;
        };
    }

    private static MessageTransformer transformerFor(ReplayToTopicRequest req) {
        ReplayTransformConfig cfg = req.transforms();
        if (cfg == null || cfg.isIdentity()) {
            return null;
        }
        return new MessageTransformer(cfg);
    }

    @FunctionalInterface
    private interface SendAction {
        RecordSink.SendResult execute();
    }

    /**
     * Executes a send. On success increments {@code counts[0]}.
     * On {@link SinkException} increments {@code counts[1]} and rethrows so
     * the streaming pipeline aborts.
     */
    private static void doSend(SendAction action, long[] counts) {
        try {
            action.execute();
            counts[0]++;
        } catch (SinkException e) {
            counts[1]++;
            throw e;
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
