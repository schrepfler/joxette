package com.joxette.recording;

import java.time.Instant;

/**
 * Live status snapshot for a topic recorder scope, returned by
 * {@link RecordingCoordinator#listRunning()}.
 */
public record RecorderStatus(
        String topic,
        boolean running,
        Instant startedAt,
        /** Timestamp of the last successfully written batch; {@code null} if no batch written yet. */
        Instant lastBatchAt,
        /**
         * Maximum consumer lag across all assigned partitions; {@code -1} if unavailable
         * (consumer not yet polled, or the metric has not been populated).
         * For parallelism > 1 this is the sum of per-recorder lag values.
         */
        long consumerLag,
        /** Last error message from a failed recorder scope; {@code null} if healthy. */
        String lastError
) {}
