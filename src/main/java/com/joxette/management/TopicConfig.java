package com.joxette.management;

/**
 * Runtime view of a configured topic, combining persisted settings with the
 * live recording state from {@link com.joxette.recording.RecordingCoordinator}.
 */
public record TopicConfig(
        String topic,
        /** "general" | "entity_only" | "both" */
        String mode,
        boolean paused,
        boolean active
) {}
