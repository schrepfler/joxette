package com.joxette.testkit;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.CassetteSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

/**
 * In-memory {@link CassetteSource} for driving {@link com.joxette.replay.ReplayEngine}
 * outside the Joxette service. Records are grouped by topic, sorted by
 * {@code (timestamp, partition, offset)} on {@link #add}, and filtered in-memory
 * at {@link #streamAll} time.
 *
 * <p>Typical use from a test-kit caller:
 * <pre>{@code
 *   var cassette = new InMemoryCassetteSource()
 *       .add(new CassetteRecord("t", 0, 0L, t0,        t0, "k0", v0, List.of(), null))
 *       .add(new CassetteRecord("t", 0, 1L, t0.plusMs, t0, "k1", v1, List.of(), null));
 *   var engine = new ReplayEngine(cassette, InMemoryEntityCassetteSource.empty(), sink);
 *   engine.replayTopic("t", request, 1.0, progress::add);
 * }</pre>
 */
public final class InMemoryCassetteSource implements CassetteSource {

    private static final Comparator<CassetteRecord> ORDER =
            Comparator.comparing(CassetteRecord::timestamp)
                      .thenComparingInt(CassetteRecord::partition)
                      .thenComparingLong(CassetteRecord::offset);

    private final List<CassetteRecord> records = new ArrayList<>();

    /** Appends a record; internal order is re-sorted on each insert. */
    public InMemoryCassetteSource add(CassetteRecord record) {
        records.add(record);
        records.sort(ORDER);
        return this;
    }

    /** Appends every record in iteration order. */
    public InMemoryCassetteSource addAll(Iterable<CassetteRecord> batch) {
        batch.forEach(records::add);
        records.sort(ORDER);
        return this;
    }

    @Override
    public void streamAll(
            String topic,
            Instant from, Instant to,
            Integer partition,
            Long offsetFrom, Long offsetTo,
            Consumer<CassetteRecord> sink
    ) {
        for (CassetteRecord r : records) {
            if (!topic.equals(r.topic()))                      continue;
            if (from       != null && r.timestamp().isBefore(from))      continue;
            if (to         != null && r.timestamp().isAfter(to))         continue;
            if (partition  != null && r.partition() != partition)        continue;
            if (offsetFrom != null && r.offset()    <  offsetFrom)       continue;
            if (offsetTo   != null && r.offset()    >  offsetTo)         continue;
            sink.accept(r);
        }
    }
}
