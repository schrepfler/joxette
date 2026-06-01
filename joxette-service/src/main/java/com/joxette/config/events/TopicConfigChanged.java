package com.joxette.config.events;

import java.io.Serializable;

/**
 * Published to the cluster-wide {@code TopicConfigChanged} pub/sub topic whenever
 * a topic configuration is created, updated, paused, resumed, or deleted.
 *
 * <p>All recording-enabled nodes subscribe and trigger an immediate reconciliation
 * of their local Kafka consumers against the current catalog state, bypassing the
 * 30 s polling interval of {@link com.joxette.recording.RecordingConfigWatcher}.
 *
 * <p>Single-node: the subscriber is in the same JVM — effectively a direct call.
 * Multi-node (Phase 2): Pekko {@code DistributedPubSub} delivers to all cluster members.
 */
public record TopicConfigChanged(String topic, String changeType) implements Serializable {
    // changeType: "created" | "updated" | "paused" | "resumed" | "deleted"
}
