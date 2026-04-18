package com.joxette.replay;

import java.sql.SQLException;
import java.time.Instant;
import java.util.function.Consumer;

/**
 * SPI that {@link ReplayEngine} uses to pull entity-cassette records out of
 * whatever storage backend is plugged in. The production implementation reads
 * from DuckLake; tests can supply an in-memory implementation backed by a
 * pre-seeded list.
 */
public interface EntityCassetteSource {

    /**
     * Streams every event for the given entity to {@code sink} in ascending
     * {@code (kafka_timestamp, recorded_at, topic, partition, offset)} order —
     * i.e. a merge-sort across all source topics that fed this entity.
     */
    void streamEntityEvents(
            String entityType, String entityId,
            Instant from, Instant to,
            Consumer<EntityRecord> sink
    ) throws SQLException;
}
