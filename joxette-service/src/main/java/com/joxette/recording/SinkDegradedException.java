package com.joxette.recording;

/**
 * Thrown by {@link DuckLakeWriteChannel#submit} when the write sink is in
 * DEGRADED state (object-store unreachable, drain VT retrying).
 *
 * <p>Callers ({@link TopicRecorder}) must not re-submit the batch — instead
 * they should pause Kafka consumption and poll the channel's health until it
 * recovers, then resume. The batch is still held in the channel queue; the
 * drain VT will process it once the store comes back.
 */
public class SinkDegradedException extends RuntimeException {

    public SinkDegradedException(String message) {
        super(message, null, true, false); // no stack trace — this is a flow-control signal
    }
}
