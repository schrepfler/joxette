package com.joxette.recording;

import com.joxette.config.BrokerConnectionFactory;
import com.joxette.config.JoxetteProperties;
import com.joxette.management.ConfigRepository;
import com.joxette.management.TopicConfig;
import com.joxette.replay.KnownEntitiesRepository;
import com.joxette.replay.MessageRouter;
import com.softwaremill.jox.kafka.ConsumerSettings;
import com.softwaremill.jox.structured.Scopes;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the lifecycle of per-topic {@link TopicRecorder} instances.
 *
 * <p>Each topic runs inside a {@link RecorderScope}: one or more virtual threads
 * (depending on {@code joxette.threading.topic-parallelism}) each running a
 * supervised Jox scope around a {@link TopicRecorder}.  All recorders share the
 * single {@link DuckLakeWriteChannel}.
 *
 * <p>On failure a scope removes itself from the active map and records the error;
 * it does NOT auto-restart (retry policy is left for a future task).
 */
@Lazy
@Component
public class RecordingCoordinator {

    private static final Logger log = LoggerFactory.getLogger(RecordingCoordinator.class);

    private final JoxetteProperties properties;
    private final BrokerConnectionFactory brokerConnectionFactory;
    private final ConfigRepository configRepository;
    private final DuckLakeWriteChannel writeChannel;
    private final MessageRouter messageRouter;
    private final KnownEntitiesRepository knownEntities;

    private final ConcurrentHashMap<String, RecorderScope> activeScopes = new ConcurrentHashMap<>();

    public RecordingCoordinator(
            JoxetteProperties properties,
            BrokerConnectionFactory brokerConnectionFactory,
            @Lazy ConfigRepository configRepository,
            DuckLakeWriteChannel writeChannel,
            MessageRouter messageRouter,
            KnownEntitiesRepository knownEntities) {
        this.properties = properties;
        this.brokerConnectionFactory = brokerConnectionFactory;
        this.configRepository = configRepository;
        this.writeChannel = writeChannel;
        this.messageRouter = messageRouter;
        this.knownEntities = knownEntities;
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Starts recording {@code topic} if it is not already active.
     *
     * @param startFrom "earliest" | "latest" | ISO-8601 timestamp
     * @return {@code true} if a new scope was started; {@code false} if already running.
     */
    public boolean startTopic(String topic, String startFrom) {
        RecorderScope[] created = {null};
        activeScopes.computeIfAbsent(topic, t -> {
            created[0] = launchScope(t, startFrom);
            return created[0];
        });
        if (created[0] != null) {
            log.info("Started recorder scope for topic '{}' (startFrom={}, parallelism={})",
                    topic, startFrom, created[0].recorders().size());
            return true;
        }
        log.debug("Recorder for topic '{}' already running", topic);
        return false;
    }

    /**
     * Starts recording {@code topic} with the default offset strategy ("latest").
     */
    public boolean startTopic(String topic) {
        return startTopic(topic, "latest");
    }

    /**
     * Stops the recorder scope for {@code topic} and waits for all its threads to terminate.
     *
     * @return {@code true} if a scope was stopped; {@code false} if none was running.
     */
    public boolean stopTopic(String topic) {
        RecorderScope scope = activeScopes.remove(topic);
        if (scope == null) {
            log.debug("No active recorder scope for topic '{}'", topic);
            return false;
        }
        shutdownScope(topic, scope);
        return true;
    }

    /**
     * Stops then restarts the recorder for {@code topic} using the same startFrom
     * value that was used when it was first started.
     */
    public boolean restartTopic(String topic) {
        RecorderScope existing = activeScopes.remove(topic);
        String startFrom = existing != null ? existing.startFrom() : "latest";
        if (existing != null) {
            shutdownScope(topic, existing);
        }
        return startTopic(topic, startFrom);
    }

    /** Returns the set of currently active topic names. */
    public Set<String> activeTopics() {
        return Set.copyOf(activeScopes.keySet());
    }

    /**
     * Returns a live status snapshot for every active recorder scope.
     * Topics that failed and self-removed are not included (their last error
     * was already logged at ERROR level).
     */
    public Map<String, RecorderStatus> listRunning() {
        Map<String, RecorderStatus> result = new java.util.LinkedHashMap<>();
        activeScopes.forEach((topic, scope) -> result.put(topic, scope.toStatus()));
        return Map.copyOf(result);
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    @PreDestroy
    public void stopAll() {
        log.info("Stopping all {} active recorder scope(s)", activeScopes.size());
        // Snapshot keys before iteration to avoid ConcurrentModificationException
        List.copyOf(activeScopes.keySet()).forEach(this::stopTopic);
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    private RecorderScope launchScope(String topic, String startFrom) {
        JoxetteProperties.Recording cfg = properties.getRecording();
        int parallelism = resolveParallelism(topic);

        String brokerId = lookupBrokerId(topic);
        ConsumerSettings<String, byte[]> baseSettings = brokerConnectionFactory.consumerSettings(brokerId);

        List<RecorderHandle> handles = new ArrayList<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            TopicRecorder recorder = new TopicRecorder(
                    topic,
                    baseSettings,
                    writeChannel,
                    cfg.getBatchSize(),
                    cfg.getBatchTimeoutMs(),
                    messageRouter,
                    knownEntities,
                    startFrom);

            int idx = i;
            Thread thread = Thread.ofVirtual()
                    .name("joxette-recorder-" + topic + (parallelism > 1 ? "-" + idx : ""))
                    .start(() -> runInSupervisedScope(topic, recorder));

            handles.add(new RecorderHandle(recorder, thread));
        }

        return new RecorderScope(topic, startFrom, Instant.now(), handles);
    }

    private void runInSupervisedScope(String topic, TopicRecorder recorder) {
        JoxetteProperties.Recording cfg = properties.getRecording();
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(Integer.MAX_VALUE)
                .intervalFunction(io.github.resilience4j.core.IntervalFunction
                        .ofExponentialRandomBackoff(
                                Duration.ofMillis(cfg.getRetryInitialIntervalMs()),
                                cfg.getRetryMultiplier(),
                                Duration.ofMillis(cfg.getRetryMaxIntervalMs())))
                // Don't retry if the recorder was stopped deliberately or interrupted
                .ignoreExceptions(InterruptedException.class)
                .retryOnException(e -> !recorder.isStopped())
                .build();

        Retry retry = Retry.of("recorder-" + topic, retryConfig);
        retry.getEventPublisher()
                .onRetry(e -> log.warn(
                        "Recorder for topic '{}' will retry (attempt #{}) after backoff — last error: {}",
                        topic, e.getNumberOfRetryAttempts(), e.getLastThrowable().getMessage()))
                .onError(e -> log.error(
                        "Recorder for topic '{}' exhausted retries", topic, e.getLastThrowable()));

        try {
            Retry.decorateCheckedRunnable(retry, () ->
                    Scopes.supervised(scope -> {
                        scope.forkUser(() -> {
                            recorder.run();
                            return null;
                        });
                        return null;
                    })
            ).run();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Recorder for topic '{}' interrupted; stopping cleanly", topic);
        } catch (Throwable e) {
            log.error("Recorder for topic '{}' failed permanently", topic, e);
            RecorderScope scope = activeScopes.remove(topic);
            if (scope != null) scope.recordError(e.getMessage());
        } finally {
            activeScopes.remove(topic);
        }
    }

    private void shutdownScope(String topic, RecorderScope scope) {
        for (RecorderHandle handle : scope.recorders()) {
            handle.recorder().stop();
        }
        for (RecorderHandle handle : scope.recorders()) {
            try {
                handle.thread().join(5_000);
                if (handle.thread().isAlive()) {
                    log.warn("Recorder thread for topic '{}' did not stop within 5 s; interrupting", topic);
                    handle.thread().interrupt();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("Stopped recorder scope for topic '{}'", topic);
    }

    private int resolveParallelism(String topic) {
        Map<String, Integer> overrides = properties.getThreading().getTopicParallelism();
        if (overrides != null && overrides.containsKey(topic)) {
            return Math.max(1, overrides.get(topic));
        }
        return Math.max(1, properties.getThreading().getDefaultSourceParallelism());
    }

    private String lookupBrokerId(String topic) {
        try {
            return configRepository.findTopic(topic)
                    .map(TopicConfig::brokerId)
                    .orElse(null);
        } catch (SQLException e) {
            log.warn("Could not look up brokerId for topic '{}'; using default broker: {}", topic, e.getMessage());
            return null;
        }
    }

    // -----------------------------------------------------------------------
    // Inner types
    // -----------------------------------------------------------------------

    /**
     * Internal handle for one recorder virtual thread.
     */
    private record RecorderHandle(TopicRecorder recorder, Thread thread) {}

    /**
     * Groups one or more {@link RecorderHandle}s for a single topic.
     *
     * <p>For topics with parallelism > 1, multiple handles are created — each
     * consuming a disjoint partition range.  All handles feed into the shared
     * {@link DuckLakeWriteChannel}.
     */
    private static final class RecorderScope {

        private final String topic;
        private final String startFrom;
        private final Instant startedAt;
        private final List<RecorderHandle> recorders;
        private volatile String lastError;

        RecorderScope(String topic, String startFrom, Instant startedAt, List<RecorderHandle> recorders) {
            this.topic = topic;
            this.startFrom = startFrom;
            this.startedAt = startedAt;
            this.recorders = List.copyOf(recorders);
        }

        String startFrom() { return startFrom; }
        List<RecorderHandle> recorders() { return recorders; }

        void recordError(String error) {
            this.lastError = error;
        }

        RecorderStatus toStatus() {
            boolean running = recorders.stream().anyMatch(h -> h.thread().isAlive());

            // Most-recent lastBatchAt across all handles
            Instant lastBatch = recorders.stream()
                    .map(h -> h.recorder().lastBatchAt())
                    .filter(t -> t != null)
                    .max(Instant::compareTo)
                    .orElse(null);

            // Sum lag across all parallel consumers
            long totalLag = recorders.stream()
                    .mapToLong(h -> {
                        long l = h.recorder().consumerLag();
                        return l < 0 ? 0 : l;
                    })
                    .sum();
            // Report -1 when no consumer has reported lag yet
            if (recorders.stream().allMatch(h -> h.recorder().consumerLag() < 0)) {
                totalLag = -1;
            }

            return new RecorderStatus(topic, running, startedAt, lastBatch, totalLag, lastError);
        }
    }
}
