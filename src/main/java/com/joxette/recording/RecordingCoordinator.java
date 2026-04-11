package com.joxette.recording;

import com.joxette.config.JoxetteProperties;
import com.joxette.kafka.ConsumerSettings;
import com.joxette.replay.KnownEntitiesRepository;
import com.joxette.replay.MessageRouter;
import com.softwaremill.jox.structured.Scopes;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the lifecycle of per-topic {@link TopicRecorder} instances.
 *
 * <p>Each recorder is started on a dedicated virtual thread inside a Jox
 * supervised scope.  The {@link MessageRouter} and {@link KnownEntitiesRepository}
 * are injected so that entity routing and known-entity registration happen inside
 * every recorder's batch-write loop.
 */
@Lazy
@Component
public class RecordingCoordinator {

    private static final Logger log = LoggerFactory.getLogger(RecordingCoordinator.class);

    private final JoxetteProperties properties;
    private final ConsumerSettings<String, byte[]> baseKafkaConsumerSettings;
    private final Connection duckDbConnection;
    private final MessageRouter messageRouter;
    private final KnownEntitiesRepository knownEntities;

    /** Live recorders, keyed by topic name. */
    private final ConcurrentHashMap<String, RecorderHandle> activeRecorders =
            new ConcurrentHashMap<>();

    public RecordingCoordinator(
            JoxetteProperties properties,
            @Qualifier("baseKafkaConsumerSettings") ConsumerSettings<String, byte[]> baseKafkaConsumerSettings,
            Connection duckDbConnection,
            MessageRouter messageRouter,
            KnownEntitiesRepository knownEntities) {
        this.properties = properties;
        this.baseKafkaConsumerSettings = baseKafkaConsumerSettings;
        this.duckDbConnection = duckDbConnection;
        this.messageRouter = messageRouter;
        this.knownEntities = knownEntities;
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Starts recording {@code topic} if it is not already active.
     *
     * @param startFrom "earliest" to seek all partitions to the beginning on first assignment;
     *                  "latest" (default) to start from the current end of the topic.
     * @return {@code true} if a new recorder was started; {@code false} if one
     *         was already running for this topic.
     */
    public boolean startTopic(String topic, String startFrom) {
        RecorderHandle[] created = {null};
        activeRecorders.computeIfAbsent(topic, t -> {
            created[0] = launchRecorder(t, startFrom);
            return created[0];
        });
        if (created[0] != null) {
            log.info("Started recorder for topic '{}' (startFrom={})", topic, startFrom);
            return true;
        }
        log.debug("Recorder for topic '{}' already running", topic);
        return false;
    }

    /**
     * Starts recording {@code topic} with the default offset strategy ("latest").
     * Use {@link #startTopic(String, String)} to specify "earliest" for backfill.
     */
    public boolean startTopic(String topic) {
        return startTopic(topic, "latest");
    }

    /**
     * Stops the recorder for {@code topic} and waits for its virtual thread to terminate.
     *
     * @return {@code true} if a recorder was stopped; {@code false} if none was running.
     */
    public boolean stopTopic(String topic) {
        RecorderHandle handle = activeRecorders.remove(topic);
        if (handle == null) {
            log.debug("No active recorder for topic '{}'", topic);
            return false;
        }
        shutdownHandle(topic, handle);
        return true;
    }

    /** Returns the set of currently active topic names. */
    public Set<String> activeTopics() {
        return Set.copyOf(activeRecorders.keySet());
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    @PreDestroy
    public void stopAll() {
        log.info("Stopping all {} active recorder(s)", activeRecorders.size());
        activeRecorders.keySet().forEach(this::stopTopic);
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    private RecorderHandle launchRecorder(String topic, String startFrom) {
        JoxetteProperties.Recording cfg = properties.getRecording();
        TopicRecorder recorder = new TopicRecorder(
                topic,
                baseKafkaConsumerSettings,
                duckDbConnection,
                cfg.getBatchSize(),
                cfg.getBatchTimeoutMs(),
                messageRouter,
                knownEntities,
                startFrom);

        Thread thread = Thread.ofVirtual()
                .name("joxette-recorder-" + topic)
                .start(() -> runInSupervisedScope(topic, recorder));

        return new RecorderHandle(recorder, thread);
    }

    private void runInSupervisedScope(String topic, TopicRecorder recorder) {
        try {
            Scopes.supervised(scope -> {
                scope.forkUser(() -> {
                    recorder.run();
                    return null;
                });
                return null;
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Recorder for topic '{}' interrupted", topic);
        } catch (Exception e) {
            log.error("Recorder for topic '{}' failed; removing from active set", topic, e);
        } finally {
            activeRecorders.remove(topic);
        }
    }

    private void shutdownHandle(String topic, RecorderHandle handle) {
        handle.recorder().stop();
        try {
            handle.thread().join(5_000);
            if (handle.thread().isAlive()) {
                log.warn("Recorder thread for topic '{}' did not stop within 5 s; interrupting", topic);
                handle.thread().interrupt();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("Stopped recorder for topic '{}'", topic);
    }

    // -----------------------------------------------------------------------
    // Inner types
    // -----------------------------------------------------------------------

    private record RecorderHandle(TopicRecorder recorder, Thread thread) {}
}
