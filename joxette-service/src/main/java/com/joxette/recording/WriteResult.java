package com.joxette.recording;

/**
 * Outcome of a single {@link WriteBatch} processed by {@link DuckLakeWriteChannel}.
 */
public record WriteResult(String topic, int recordsWritten) {}
