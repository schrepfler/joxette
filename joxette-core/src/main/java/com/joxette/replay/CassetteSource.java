package com.joxette.replay;

import java.sql.SQLException;
import java.time.Instant;
import java.util.function.Consumer;

/**
 * SPI that {@link ReplayEngine} uses to pull general-cassette records out of
 * whatever storage backend is plugged in.
 *
 * <p>The production implementation reads from DuckLake; tests can supply an
 * in-memory implementation backed by a pre-seeded list. The engine only needs
 * the streaming method below — {@link CassetteRecord}s must be emitted in
 * ascending {@code timestamp} order so the engine's inter-message delay logic
 * reproduces the original pacing.
 */
public interface CassetteSource {

    /**
     * Streams every record matching the filters to {@code sink} in ascending
     * {@code (kafka_timestamp, kafka_partition, kafka_offset)} order.
     *
     * @param topic       topic whose general cassette should be scanned
     * @param from        inclusive lower bound on {@code kafka_timestamp}, or {@code null}
     * @param to          inclusive upper bound on {@code kafka_timestamp}, or {@code null}
     * @param partition   filter to a single Kafka partition, or {@code null} for all
     * @param offsetFrom  inclusive lower bound on {@code kafka_offset}, or {@code null}
     * @param offsetTo    inclusive upper bound on {@code kafka_offset}, or {@code null}
     * @param sink        consumer invoked once per record in stream order
     */
    void streamAll(
            String topic,
            Instant from, Instant to,
            Integer partition,
            Long offsetFrom, Long offsetTo,
            Consumer<CassetteRecord> sink
    ) throws SQLException;
}
