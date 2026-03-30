package com.joxette.recording;

import com.joxette.config.JoxetteProperties;
import com.softwaremill.jox.structured.Scopes;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the lifecycle of per-topic {@link TopicRecorder} instances.
 *
 * <h2>Structured concurrency model</h2>
 * Each active topic recorder runs inside its own Jox
 * {@link Scopes#supervised supervised scope}, which is in turn hosted on a
 * dedicated virtual thread.  Within that scope the supervised design means:
 * <ul>
 *   <li>If the {@link com.joxette.recording.CassetteBatchWriter DuckLake writer}
 *       throws, the exception exits {@link TopicRecorder#run()}, the scope
 *       fails, and the scope's cleanup cancels the Kafka source (via
 *       {@link TopicRecorder#stop()}).</li>
 *   <li>If Kafka disconnects unexpectedly, the poll loop propagates the
 *       exception through the Jox flow, the scope fails, and the virtual
 *       thread terminates — the coordinator marks the topic as no longer
 *       active.</li>
 * </ul>
 *
 * <h2>Dynamic start / stop</h2>
 * {@link #startTopic(String)} and {@link #stopTopic(String)} may be called
 * concurrently (e.g. from a REST controller).  State is kept in a
 * {@link ConcurrentHashMap} of {@link RecorderHandle} records.
 */
@Component
public class RecordingCoordinator {

    private static final Logger log = LoggerFactory.getLogger(RecordingCoordinator.class);

    private final JoxetteProperties properties;
    private final Map<String, Object> baseKafkaConsumerProperties;
    private final Connection duckDbConnection;

    /** Live recorders, keyed by topic name. */
    private final ConcurrentHashMap<String, RecorderHandle> activeRecorders =
            new ConcurrentHashMap<>();

    public RecordingCoordinator(
            JoxetteProperties properties,
            Map<String, Object> baseKafkaConsumerProperties,
            Connection duckDbConnection) {
        this.properties = properties;
        this.baseKafkaConsumerProperties = baseKafkaConsumerProperties;
        this.duckDbConnection = duckDbConnection;
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Starts recording {@code topic} if it is not already active.
     *
     * @return {@code true} if a new recorder was started; {@code false} if one
     *         was already running for this topic.
     */
    public boolean startTopic(String topic) {
        // Atomically insert; if a handle already exists, do nothing.
        RecorderHandle[] created = {null};
        activeRecorders.computeIfAbsent(topic, t -> {
            created[0] = launchRecorder(t);
            return created[0];
        });
        if (created[0] != null) {
            log.info("Started recorder for topic '{}'", topic);
            return true;
        }
        log.debug("Recorder for topic '{}' already running", topic);
        return false;
    }

    /**
     * Stops the recorder for {@code topic} and waits for its virtual thread to
     * terminate.
     *
     * @return {@code true} if a recorder was stopped; {@code false} if none was
     *         running.
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
        // Collect keys first to avoid ConcurrentModificationException.
        activeRecorders.keySet().forEach(this::stopTopic);
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    private RecorderHandle launchRecorder(String topic) {
        JoxetteProperties.Recording cfg = properties.getRecording();
        TopicRecorder recorder = new TopicRecorder(
                topic,
                baseKafkaConsumerProperties,
                duckDbConnection,
                cfg.getBatchSize(),
                cfg.getBatchTimeoutMs());

        Thread thread = Thread.ofVirtual()
                .name("joxette-recorder-" + topic)
                .start(() -> runInSupervisedScope(topic, recorder));

        return new RecorderHandle(recorder, thread);
    }

    /**
     * Wraps the recorder in a Jox supervised scope so that any failure inside
     * the pipeline (writer or Kafka source) is contained to this topic's scope.
     * When the scope exits (normally or exceptionally), the recorder's resources
     * are closed and the entry is removed from {@link #activeRecorders}.
     */
    private void runInSupervisedScope(String topic, TopicRecorder recorder) {
        try {
            Scopes.supervised(scope -> {
                // Fork the recorder so the scope owns its lifecycle.
                // If run() throws, the scope propagates the failure; the
                // scope's structured-concurrency guarantee then cancels any
                // other forks (none here, but this future-proofs the design).
                scope.fork(() -> {
                    recorder.run();
                    return null;
                });
                // Scope blocks here until the fork completes or fails.
                return null;
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Recorder for topic '{}' interrupted", topic);
        } catch (Exception e) {
            log.error("Recorder for topic '{}' failed; removing from active set", topic, e);
        } finally {
            // Ensure the map entry is cleaned up even on unexpected exits.
            activeRecorders.remove(topic);
        }
    }

    private void shutdownHandle(String topic, RecorderHandle handle) {
        handle.recorder().stop();          // signal poll loop to exit
        handle.thread().interrupt();       // wake any park/sleep inside the VT
        try {
            handle.thread().join(5_000);   // wait up to 5 s for clean exit
            if (handle.thread().isAlive()) {
                log.warn("Recorder thread for topic '{}' did not stop within 5 s", topic);
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
