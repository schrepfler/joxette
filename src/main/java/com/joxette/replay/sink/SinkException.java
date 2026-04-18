package com.joxette.replay.sink;

/**
 * Unchecked exception raised by {@link RecordSink} implementations when a send
 * fails permanently. The engine catches this to mark a replay run as failed.
 */
public class SinkException extends RuntimeException {

    public SinkException(String message) {
        super(message);
    }

    public SinkException(String message, Throwable cause) {
        super(message, cause);
    }
}
