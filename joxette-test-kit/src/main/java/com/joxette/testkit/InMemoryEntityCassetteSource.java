package com.joxette.testkit;

import com.joxette.replay.EntityCassetteSource;
import com.joxette.replay.EntityRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

/**
 * In-memory {@link EntityCassetteSource} for driving {@link com.joxette.replay.ReplayEngine}
 * outside the Joxette service. Records are sorted by
 * {@code (timestamp, recordedAt, topic, partition, offset)} on insert — matching the
 * merge-sort order the DuckLake-backed implementation produces — and filtered at
 * {@link #streamEntityEvents} time.
 */
public final class InMemoryEntityCassetteSource implements EntityCassetteSource {

    private static final Comparator<EntityRecord> ORDER =
            Comparator.comparing(EntityRecord::timestamp)
                      .thenComparing(EntityRecord::recordedAt)
                      .thenComparing(EntityRecord::topic)
                      .thenComparingInt(EntityRecord::partition)
                      .thenComparingLong(EntityRecord::offset);

    private final List<EntityRecord> records = new ArrayList<>();

    /** Returns an empty source — useful when replaying only topic cassettes. */
    public static InMemoryEntityCassetteSource empty() {
        return new InMemoryEntityCassetteSource();
    }

    public InMemoryEntityCassetteSource add(EntityRecord record) {
        records.add(record);
        records.sort(ORDER);
        return this;
    }

    public InMemoryEntityCassetteSource addAll(Iterable<EntityRecord> batch) {
        batch.forEach(records::add);
        records.sort(ORDER);
        return this;
    }

    @Override
    public void streamEntityEvents(
            String entityType, String entityId,
            Instant from, Instant to,
            Consumer<EntityRecord> sink
    ) {
        for (EntityRecord r : records) {
            if (!entityId.equals(r.entityId()))                      continue;
            if (from != null && r.timestamp().isBefore(from))        continue;
            if (to   != null && r.timestamp().isAfter(to))           continue;
            sink.accept(r);
        }
    }
}
