package com.joxette.operator.rest;

/**
 * Raised when a call to the joxette-service REST API fails (transport error or a
 * non-success status the reconciler can't interpret). Reconcilers let this
 * propagate so JOSDK retries with backoff and surfaces {@code Degraded}.
 */
public class JoxetteRestException extends RuntimeException {

    public JoxetteRestException(String message) {
        super(message);
    }

    public JoxetteRestException(String message, Throwable cause) {
        super(message, cause);
    }
}
