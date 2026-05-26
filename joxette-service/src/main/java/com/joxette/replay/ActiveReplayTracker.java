package com.joxette.replay;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks in-progress replay-to-topic operations so they are surfaced on the
 * live flow map. Entries linger for 2 s after completion so the next SSE
 * snapshot can still show them as completed before they disappear.
 */
@Component
public class ActiveReplayTracker {

    public record ActiveReplay(
            String id,
            String sourceTopic,
            String targetTopic,
            Instant startedAt,
            long sentCount,
            String status   // "running" | "completed" | "failed"
    ) {}

    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();

    private static final class Entry {
        final String id;
        final String sourceTopic;
        final String targetTopic;
        final Instant startedAt;
        final AtomicLong sentCount = new AtomicLong();
        volatile String status = "running";

        Entry(String id, String sourceTopic, String targetTopic) {
            this.id          = id;
            this.sourceTopic = sourceTopic;
            this.targetTopic = targetTopic;
            this.startedAt   = Instant.now();
        }

        ActiveReplay snapshot() {
            return new ActiveReplay(id, sourceTopic, targetTopic, startedAt, sentCount.get(), status);
        }
    }

    /**
     * Registers a new replay and returns a handle for progress updates.
     * Always use in a try-with-resources block to guarantee removal.
     */
    public Handle start(String sourceTopic, String targetTopic) {
        String id = UUID.randomUUID().toString().substring(0, 8);
        Entry e = new Entry(id, sourceTopic, targetTopic);
        entries.put(id, e);
        return new Handle(e, entries);
    }

    public List<ActiveReplay> listActive() {
        List<ActiveReplay> out = new ArrayList<>(entries.size());
        for (Entry e : entries.values()) out.add(e.snapshot());
        return out;
    }

    public final class Handle implements AutoCloseable {
        private final Entry entry;
        private final ConcurrentHashMap<String, Entry> map;

        Handle(Entry entry, ConcurrentHashMap<String, Entry> map) {
            this.entry = entry;
            this.map   = map;
        }

        /**
         * Called from the {@code ReplayProgress} callback on every event.
         * Updates the sent counter and mirrors the terminal status
         * ({@code "completed"} / {@code "failed"}) from the engine so the
         * tracker reflects reality even when {@code sentCount == 0} (all
         * records deduplicated) or when the engine fails mid-stream.
         */
        public void accept(ReplayProgress p) {
            entry.sentCount.set(p.sentCount());
            if ("completed".equals(p.status()) || "failed".equals(p.status())) {
                entry.status = p.status();
            }
        }

        /**
         * Marks the entry terminal if not already set, then schedules removal
         * after a 2-second linger so the next SSE snapshot can show it.
         */
        @Override
        public void close() {
            if ("running".equals(entry.status)) entry.status = "failed";
            String id = entry.id;
            Thread.ofVirtual().name("replay-tracker-cleanup-" + id).start(() -> {
                try { Thread.sleep(2_000); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                map.remove(id);
            });
        }
    }
}
