package com.joxette.replay;

import com.joxette.replay.sink.RecordSink;
import com.joxette.replay.sink.SinkException;
import com.softwaremill.jox.flows.Flows;
import com.softwaremill.jox.structured.Scopes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Flow-based variant of {@link ReplayEngine} for SSE streaming endpoints.
 *
 * <p>Each replay runs inside a Jox {@code supervised} scope. The source is
 * wrapped in {@code Flows.usingEmit} so that when the downstream
 * {@code Consumer<ReplayProgress>} throws (broken pipe, client disconnect),
 * the scope is cancelled and the DuckDB read is interrupted immediately —
 * no phantom writes, no {@code IllegalStateException} on a dead emitter.
 *
 * <p>All business logic (transform, partition resolution, inter-message delay,
 * progress framing) delegates to the static helpers on {@link ReplayEngine} so
 * there is no duplication.
 *
 * <p>{@link ReplayEngine} is kept as-is for the blocking JSON endpoints and for
 * use in tests.
 */
public class FlowReplayEngine {

    private static final Logger log = LoggerFactory.getLogger(FlowReplayEngine.class);

    private static final int PROGRESS_INTERVAL = 100;

    private final CassetteSource         cassetteSource;
    private final EntityCassetteSource   entitySource;
    private final RecordSink             sink;
    private final Function<String, Integer> partitionCountLookup;

    public FlowReplayEngine(
            CassetteSource cassetteSource,
            EntityCassetteSource entitySource,
            RecordSink sink,
            Function<String, Integer> partitionCountLookup) {
        this.cassetteSource       = cassetteSource;
        this.entitySource         = entitySource;
        this.sink                 = sink;
        this.partitionCountLookup = partitionCountLookup;
    }

    // =========================================================================
    // General cassette replay
    // =========================================================================

    /**
     * Streams general-cassette records through the sink, reporting progress to
     * {@code progressSink}. Runs inside a supervised scope: if {@code progressSink}
     * throws (e.g. broken pipe), the scope interrupts the source immediately.
     */
    public void replayTopic(
            String sourceTopic,
            ReplayToTopicRequest req,
            double speedMultiplier,
            Consumer<ReplayProgress> progressSink
    ) throws Exception {
        long[]    counts = {0L, 0L};
        Instant[] lastTs = {null};
        Instant[] prevTs = {null};

        String effectiveTopic = ReplayEngine.resolveTargetTopic(req, sourceTopic);
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
        log.info("Starting topic replay (flow): source='{}' target='{}' speed={}x", sourceTopic, effectiveTopic, speedMultiplier);

        try {
            Scopes.<Void>supervised(scope -> {
                Flows.<CassetteRecord>usingEmit(emit -> {
                    try {
                        cassetteSource.streamAll(
                                sourceTopic, req.from(), req.to(),
                                req.partition(), req.offsetFrom(), req.offsetTo(),
                                record -> {
                                    try { emit.apply(record); }
                                    catch (Exception e) {
                                        if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                                        throw new RuntimeException(e);
                                    }
                                });
                    } catch (java.sql.SQLException e) {
                        throw new RuntimeException(e);
                    }
                })
                .runForeach(record -> {
                    ReplayEngine.applyDelay(prevTs[0], record.timestamp(), speedMultiplier);
                    prevTs[0] = record.timestamp();
                    CassetteRecord r = transformer != null ? transformer.transform(record) : record;
                    lastTs[0] = r.timestamp();
                    Integer partition = ReplayEngine.resolvePartition(req.partitionStrategy(), r.partition(), targetCount);
                    doSend(() -> sink.send(effectiveTopic, partition, r), counts);
                    if (counts[0] % PROGRESS_INTERVAL == 0) {
                        progressSink.accept(progress("in_progress", effectiveTopic, counts, lastTs[0]));
                    }
                });
                return null;
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("Topic replay interrupted (client disconnected?): source='{}'", sourceTopic);
            return;
        }

        log.info("Topic replay completed (flow): source='{}' target='{}' sent={} errors={}",
                sourceTopic, effectiveTopic, counts[0], counts[1]);
        progressSink.accept(progress("completed", effectiveTopic, counts, lastTs[0]));
    }

    // =========================================================================
    // Entity cassette replay
    // =========================================================================

    /**
     * Streams entity-cassette records through the sink, reporting progress to
     * {@code progressSink}. Runs inside a supervised scope: if {@code progressSink}
     * throws (e.g. broken pipe), the scope interrupts the source immediately.
     */
    public void replayEntity(
            String entityType,
            String entityId,
            ReplayToTopicRequest req,
            double speedMultiplier,
            Consumer<ReplayProgress> progressSink
    ) throws Exception {
        long[]    counts = {0L, 0L};
        Instant[] lastTs = {null};
        Instant[] prevTs = {null};

        Map<String, Integer> targetPartitionCounts = new HashMap<>();
        Map<String, Boolean> preserveValidated     = new HashMap<>();

        MessageTransformer transformer = transformerFor(req);
        log.info("Starting entity replay (flow): type='{}' id='{}' speed={}x", entityType, entityId, speedMultiplier);

        try {
            Scopes.<Void>supervised(scope -> {
                Flows.<EntityRecord>usingEmit(emit -> {
                    try {
                        entitySource.streamEntityEvents(
                                entityType, entityId, req.from(), req.to(),
                                record -> {
                                    try { emit.apply(record); }
                                    catch (Exception e) {
                                        if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                                        throw new RuntimeException(e);
                                    }
                                });
                    } catch (java.sql.SQLException e) {
                        throw new RuntimeException(e);
                    }
                })
                .runForeach(record -> {
                    ReplayEngine.applyDelay(prevTs[0], record.timestamp(), speedMultiplier);
                    prevTs[0] = record.timestamp();
                    EntityRecord r = transformer != null ? transformer.transform(record) : record;
                    lastTs[0] = r.timestamp();

                    String effectiveTopic = ReplayEngine.resolveTargetTopic(req, r.topic());
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

                    Integer partition = ReplayEngine.resolvePartition(req.partitionStrategy(), r.partition(), targetCount);
                    doSend(() -> sink.send(effectiveTopic, partition, r), counts);
                    if (counts[0] % PROGRESS_INTERVAL == 0) {
                        String et = ReplayEngine.resolveTargetTopic(req, entityType);
                        progressSink.accept(progress("in_progress", et, counts, lastTs[0]));
                    }
                });
                return null;
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("Entity replay interrupted (client disconnected?): type='{}' id='{}'", entityType, entityId);
            return;
        }

        String fallbackTarget = ReplayEngine.resolveTargetTopic(req, entityType);
        log.info("Entity replay completed (flow): type='{}' id='{}' target='{}' sent={} errors={}",
                entityType, entityId, fallbackTarget, counts[0], counts[1]);
        progressSink.accept(progress("completed", fallbackTarget, counts, lastTs[0]));
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    @FunctionalInterface
    private interface SendAction { RecordSink.SendResult execute(); }

    private static void doSend(SendAction action, long[] counts) {
        try {
            action.execute();
            counts[0]++;
        } catch (SinkException e) {
            counts[1]++;
            throw e;
        }
    }

    private static ReplayProgress progress(String status, String target, long[] counts, Instant ts) {
        return new ReplayProgress(status, target, counts[0], counts[1], ts, null);
    }

    private static MessageTransformer transformerFor(ReplayToTopicRequest req) {
        ReplayTransformConfig cfg = req.transforms();
        if (cfg == null || cfg.isIdentity()) return null;
        return new MessageTransformer(cfg);
    }
}
