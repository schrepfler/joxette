package com.joxette.management;

/**
 * Runtime view of a configured topic, combining persisted settings with the
 * live recording state from {@link com.joxette.recording.RecordingCoordinator}.
 */
public record TopicConfig(
        String topic,
        TopicMode mode,
        boolean paused,
        boolean active,
        /** Maximum age of records in days; {@code null} means no retention limit. */
        Integer retentionDays,
        /** "latest" | "earliest" — consumer offset reset strategy for initial start. */
        String startFrom,
        /** Broker ID to use for this topic; {@code null} means use the default broker. */
        String brokerId
) {}
