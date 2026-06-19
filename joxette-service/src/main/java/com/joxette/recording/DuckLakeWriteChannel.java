package com.joxette.recording;

import com.joxette.config.JoxetteProperties;
import com.joxette.metrics.JoxetteMetrics;
import com.softwaremill.jox.Channel;
import com.softwaremill.jox.ChannelClosedException;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Serializes all DuckDB writes through a single virtual thread.
 *
 * <p>DuckDB allows only one writer at a time on a single connection. This component
 * owns the entire write path: every {@link WriteBatch} from every per-topic
 * {@link TopicRecorder} is enqueued on a bounded Jox channel and drained by a
 * dedicated virtual thread that holds the shared writer instances.
 *
 * <p>The bounded channel capacity is the backpressure valve: when full,
 * {@link #submit(WriteBatch)} blocks the calling Jox flow step, which slows Kafka
 * consumption and increases consumer lag rather than growing in-process buffers.
 *
 * <p>Upstream coalescing is handled by {@code Flow.batchWeighted} in
 * {@link TopicRecorder} before batches reach this channel, so the drain VT
 * already receives pre-merged batches when the writer is the bottleneck.
 *
 * <p>Reads (replay queries) are NOT routed here — they use separate
 * {@code Statement} objects on the shared connection and proceed concurrently.
 */
@Component
public class DuckLakeWriteChannel {

    private static final Logger log = LoggerFactory.getLogger(DuckLakeWriteChannel.class);

    enum SinkState { HEALTHY, DEGRADED, FAILED }

    private final Connection duckDbConnection;
    private final int capacity;
    private final CassetteRecordingBus bus;
    private final long writeRetryInitialMs;
    private final double writeRetryMultiplier;
    private final long writeRetryMaxMs;
    private final int writeRetryMaxAttempts;

    private final AtomicReference<SinkState> sinkState = new AtomicReference<>(SinkState.HEALTHY);
    /** Callback invoked by drain VT when max retries are exhausted — signals actor to restart. */
    private volatile Consumer<Throwable> failureCallback;

    private Channel<WriteBatch> channel;
    private Thread drainThread;

    /**
     * Tracks in-flight {@link WriteBatch}es by their completion future.
     * Used by {@link #awaitDrain(Set)} to block until batches for specific
     * partitions have been written.
     */
    private final Set<WriteBatch> inFlight = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public DuckLakeWriteChannel(Connection duckDbConnection,
                                 JoxetteProperties properties,
                                 CassetteRecordingBus bus,
                                 JoxetteMetrics joxetteMetrics) {
        this.duckDbConnection = duckDbConnection;
        this.capacity = properties.getThreading().getWriteChannelCapacity();
        this.writeRetryInitialMs = properties.getThreading().getWriteRetryInitialMs();
        this.writeRetryMultiplier = properties.getThreading().getWriteRetryMultiplier();
        this.writeRetryMaxMs = properties.getThreading().getWriteRetryMaxMs();
        this.writeRetryMaxAttempts = properties.getThreading().getWriteRetryMaxAttempts();
        this.bus = bus;
        this.joxetteMetrics = joxetteMetrics;
    }

    private final JoxetteMetrics joxetteMetrics;

    @PostConstruct
    void start() throws SQLException {
        channel = Channel.newBufferedChannel(capacity);
        joxetteMetrics.registerWriteChannelDepthGauge(() -> inFlight.size());
        WriterSet writers = new WriterSet(duckDbConnection);
        drainThread = Thread.ofVirtual()
                .name("joxette-write-drain")
                .start(() -> drain(writers));
        log.info("DuckLakeWriteChannel started (capacity={})", capacity);
    }

    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        if (channel != null) {
            channel.done();
        }
        if (drainThread != null) {
            try {
                drainThread.join(10_000);
                if (drainThread.isAlive()) {
                    log.warn("Write drain thread did not stop within 10 s; interrupting");
                    drainThread.interrupt();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("DuckLakeWriteChannel stopped");
    }

    /** Registers the callback invoked when the drain VT exhausts max retries. */
    public void setFailureCallback(Consumer<Throwable> callback) {
        this.failureCallback = callback;
    }

    /** Returns the current sink health state. */
    public SinkState getSinkState() {
        return sinkState.get();
    }

    /** Returns true when the sink is healthy and accepting writes. */
    public boolean isHealthy() {
        return sinkState.get() == SinkState.HEALTHY;
    }

    /**
     * Resets a FAILED sink back to HEALTHY. Called by the actor supervisor after
     * restarting the sink — clears the failure state so consumers can resume.
     */
    public void resetSinkState() {
        SinkState prev = sinkState.getAndSet(SinkState.HEALTHY);
        if (prev != SinkState.HEALTHY) {
            log.info("DuckLakeWriteChannel: sink state reset to HEALTHY (was {})", prev);
        }
    }

    /**
     * Enqueues a batch and blocks until the drain VT has written it.
     *
     * <p>Throws {@link SinkDegradedException} immediately if the sink is in DEGRADED
     * state — callers must pause consumption and wait for {@link #isHealthy()} rather
     * than re-submitting. The batch is NOT enqueued in this case.
     *
     * <p>Blocking is intentional: it propagates backpressure into the Jox flow
     * pipeline so fast Kafka consumers do not outrun the DuckDB write path.
     */
    public WriteResult submit(WriteBatch batch) throws InterruptedException {
        SinkState state = sinkState.get();
        if (state == SinkState.DEGRADED) {
            throw new SinkDegradedException("Sink is DEGRADED (object store unreachable) for topic " + batch.topic());
        }
        if (state == SinkState.FAILED) {
            throw new SinkDegradedException("Sink is FAILED (max retries exhausted) for topic " + batch.topic());
        }
        inFlight.add(batch);
        try {
            channel.send(batch);
        } catch (ChannelClosedException e) {
            inFlight.remove(batch);
            throw new IllegalStateException(
                    "Write channel is closed; cannot accept batch for topic " + batch.topic(), e);
        }
        try {
            return batch.result().join();
        } catch (java.util.concurrent.CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) throw re;
            throw new RuntimeException("Write failed for topic " + batch.topic(), cause);
        } finally {
            inFlight.remove(batch);
        }
    }

    /**
     * Blocks until all in-flight batches whose partition set intersects
     * {@code revokedPartitions} have been written by the drain VT.
     *
     * <p>Pass {@code null} to drain all in-flight batches (classic protocol path).
     */
    public void awaitDrain(Collection<TopicPartition> revokedPartitions) {
        for (WriteBatch batch : inFlight) {
            boolean relevant = revokedPartitions == null
                    || batch.partitions().stream().anyMatch(revokedPartitions::contains);
            if (relevant) {
                try {
                    batch.result().join();
                } catch (Exception ignored) {
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Drain loop
    // -----------------------------------------------------------------------

    private void drain(WriterSet writers) {
        try {
            while (true) {
                Object received;
                try {
                    received = channel.receiveOrClosed();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.info("Write drain thread interrupted; exiting");
                    break;
                }
                if (received instanceof com.softwaremill.jox.ChannelClosed) {
                    log.debug("Write channel closed; drain loop exiting");
                    break;
                }
                processBatch((WriteBatch) received, writers);
            }
        } finally {
            writers.closeAll();
            log.info("Write drain loop finished");
        }
    }

    private void processBatch(WriteBatch batch, WriterSet writers) {
        long delayMs = writeRetryInitialMs;
        int attempt = 0;
        while (true) {
            try {
                int written = 0;

                if (!batch.generalRecords().isEmpty()) {
                    CassetteBatchWriter gw = writers.generalWriterFor(batch.topic());
                    gw.writeBatch(batch.generalRecords(), batch.generalMessageTypes());
                    written += batch.generalRecords().size();
                }

                if (!batch.entityItems().isEmpty()) {
                    writers.entityWriter().writeBatch(batch.entityItems());
                    for (WriteBatch.EntityWriteItem item : batch.entityItems()) {
                        written += item.routes().size();
                    }
                }

                if (attempt > 0) {
                    log.info("Write succeeded for topic '{}' after {} retries — sink recovering to HEALTHY",
                            batch.topic(), attempt);
                    sinkState.set(SinkState.HEALTHY);
                }
                batch.result().complete(new WriteResult(batch.topic(), written));
                if (bus != null) {
                    try {
                        bus.publish(batch);
                    } catch (RuntimeException be) {
                        log.warn("Recording bus publish failed for topic '{}': {}",
                                batch.topic(), be.getMessage(), be);
                    }
                }
                return;

            } catch (Exception e) {
                if (!isTransientStorageError(e)) {
                    log.error("Non-retryable write failure for topic '{}': {}", batch.topic(), e.getMessage(), e);
                    batch.result().completeExceptionally(e);
                    sinkState.set(SinkState.HEALTHY); // not a storage issue — stay healthy
                    return;
                }

                attempt++;
                // Transition to DEGRADED on first transient failure so consumers pause
                if (attempt == 1) {
                    sinkState.set(SinkState.DEGRADED);
                    log.warn("Sink DEGRADED for topic '{}': object store unreachable — consumers will pause",
                            batch.topic());
                }

                // Escalate to FAILED after max attempts
                if (writeRetryMaxAttempts > 0 && attempt >= writeRetryMaxAttempts) {
                    log.error("Sink FAILED for topic '{}' after {} attempts — signalling supervisor restart: {}",
                            batch.topic(), attempt, rootMessage(e));
                    sinkState.set(SinkState.FAILED);
                    batch.result().completeExceptionally(e);
                    Consumer<Throwable> cb = failureCallback;
                    if (cb != null) {
                        cb.accept(e);
                    }
                    return;
                }

                log.warn("Transient object-store failure for topic '{}' (attempt {}/{}), retrying in {} ms: {}",
                        batch.topic(), attempt,
                        writeRetryMaxAttempts < 0 ? "∞" : writeRetryMaxAttempts,
                        delayMs, rootMessage(e));
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    batch.result().completeExceptionally(ie);
                    return;
                }
                delayMs = Math.min((long) (delayMs * writeRetryMultiplier), writeRetryMaxMs);
            }
        }
    }

    // A failure is transient if it originates from an SQL/IO error that mentions
    // network connectivity (HTTP PUT/GET, connect, timeout). Schema errors,
    // constraint violations, and other DuckDB errors are not retryable.
    private static boolean isTransientStorageError(Throwable t) {
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof java.sql.SQLException) {
                String msg = cur.getMessage();
                if (msg != null && (
                        msg.contains("Could not connect") ||
                        msg.contains("HTTP PUT") ||
                        msg.contains("HTTP GET") ||
                        msg.contains("Connection refused") ||
                        msg.contains("Connection timed out") ||
                        msg.contains("IO Error"))) {
                    return true;
                }
            }
            cur = cur.getCause();
        }
        return false;
    }

    private static String rootMessage(Throwable t) {
        Throwable cur = t;
        while (cur.getCause() != null) cur = cur.getCause();
        return cur.getMessage();
    }

    // -----------------------------------------------------------------------
    // Writer registry
    // -----------------------------------------------------------------------

    private static final class WriterSet {

        private static final Logger log = LoggerFactory.getLogger(WriterSet.class);

        private final Connection sharedConn;
        private final Map<String, CassetteBatchWriter> generalWriters = new HashMap<>();
        private EntityCassetteBatchWriter entityWriter;

        WriterSet(Connection sharedConn) {
            this.sharedConn = sharedConn;
        }

        CassetteBatchWriter generalWriterFor(String topic) throws SQLException {
            CassetteBatchWriter w = generalWriters.get(topic);
            if (w == null) {
                w = new CassetteBatchWriter(topic, sharedConn);
                generalWriters.put(topic, w);
            }
            return w;
        }

        EntityCassetteBatchWriter entityWriter() throws SQLException {
            if (entityWriter == null) {
                entityWriter = new EntityCassetteBatchWriter(sharedConn);
            }
            return entityWriter;
        }

        void closeAll() {
            generalWriters.forEach((topic, w) -> {
                try { w.close(); } catch (Exception e) {
                    log.warn("Error closing general writer for '{}': {}", topic, e.getMessage());
                }
            });
            generalWriters.clear();
            if (entityWriter != null) {
                try { entityWriter.close(); } catch (Exception e) {
                    log.warn("Error closing entity writer: {}", e.getMessage());
                }
                entityWriter = null;
            }
        }
    }
}
