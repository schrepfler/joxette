package com.joxette.config.events;

import java.io.Serializable;

/**
 * Published to the cluster-wide {@code EntityConfigChanged} pub/sub topic whenever
 * an entity type configuration or source mapping is created, updated, or deleted.
 *
 * <p>All nodes subscribe and call {@code messageRouter.reload()} immediately,
 * keeping in-memory routing tables consistent without requiring a restart.
 */
public record EntityConfigChanged(String entityType, String changeType) implements Serializable {
    // changeType: "created" | "updated" | "deleted" | "source_added" | "source_deleted"
}
