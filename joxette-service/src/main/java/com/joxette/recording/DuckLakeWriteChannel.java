package com.joxette.recording;

import com.joxette.config.JoxetteProperties;
import com.softwaremill.jox.Channel;
import com.softwaremill.jox.ChannelClosedException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializes all DuckDB writes through a single virtual thread.
 *
 * <p>DuckDB allows only one writer at a time on a single connection. This component
 * owns the entire write path: every {@link WriteBatch} from every per-topic
 * {@link TopicRecorder} is enqueued on a bounded Jox channel and drained
 * one-at-a-time by a dedicated virtual thread that holds the shared writer instances.
 *
 * <p>The bounded channel capacity is the backpressure valve: when full,
 * {@link #submit(WriteBatch)} blocks the calling Jox flow step, which slows Kafka
 * consumption and increases consumer lag rather than growing in-process buffers.
 *
 * <p>Reads (replay queries) are NOT routed here — they use separate
 * {@code Statement} objects on the shared connection and proceed concurrently.
 */
@Component
public class DuckLakeWriteChannel {

    private static final Logger log = LoggerFactory.getLogger(DuckLakeWriteChannel.class);

    private final Connection duckDbConnection;
    private final int capacity;
    private final CassetteRecordingBus bus;

    private Channel<WriteBatch> channel;
    private Thread drainThread;

    public DuckLakeWriteChannel(Connection duckDbConnection,
                                 JoxetteProperties properties,
                                 CassetteRecordingBus bus) {
        this.duckDbConnection = duckDbConnection;
        this.capacity = properties.getThreading().getWriteChannelCapacity();
        this.bus = bus;
    }

    @PostConstruct
    void start() throws SQLException {
        channel = Channel.newBufferedChannel(capacity);
        WriterSet writers = new WriterSet(duckDbConnection);
        drainThread = Thread.ofVirtual()
                .name("joxette-write-drain")
                .start(() -> drain(writers));
        log.info("DuckLakeWriteChannel started (capacity={})", capacity);
    }

    @PreDestroy
    void stop() {
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

    /**
     * Enqueues a batch and blocks until the drain VT has written it.
     *
     * <p>Blocking is intentional: it propagates backpressure into the Jox flow
     * pipeline so fast Kafka consumers do not outrun the DuckDB write path.
     */
    public WriteResult submit(WriteBatch batch) throws InterruptedException {
        try {
            channel.send(batch);
        } catch (ChannelClosedException e) {
            throw new IllegalStateException(
                    "Write channel is closed; cannot accept batch for topic " + batch.topic(), e);
        }
        // Block until the drain VT completes or fails this batch.
        try {
            return batch.result().join();
        } catch (java.util.concurrent.CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) throw re;
            throw new RuntimeException("Write failed for topic " + batch.topic(), cause);
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
        try {
            int written = 0;

            if (!batch.generalRecords().isEmpty()) {
                CassetteBatchWriter gw = writers.generalWriterFor(batch.topic());
                gw.writeBatch(batch.generalRecords(), batch.generalMessageTypes());
                written += batch.generalRecords().size();
            }

            for (WriteBatch.EntityWriteItem item : batch.entityItems()) {
                writers.entityWriter().writeRoutes(item.routes(), item.message());
                written += item.routes().size();
            }

            batch.result().complete(new WriteResult(batch.topic(), written));
            if (bus != null) {
                try {
                    bus.publish(batch);
                } catch (RuntimeException be) {
                    // Must never break the drain VT — bus publication is best-effort.
                    log.warn("Recording bus publish failed for topic '{}': {}",
                            batch.topic(), be.getMessage(), be);
                }
            }
        } catch (Exception e) {
            log.error("Failed to write batch for topic '{}': {}", batch.topic(), e.getMessage(), e);
            batch.result().completeExceptionally(e);
        }
    }

    // -----------------------------------------------------------------------
    // Writer registry — lazily initialized, owned by the drain VT
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
