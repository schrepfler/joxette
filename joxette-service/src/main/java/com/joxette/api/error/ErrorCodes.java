package com.joxette.api.error;

/**
 * Stable, machine-readable error codes attached to ProblemDetail bodies as the
 * {@code errorCode} extension field. These are part of the public API contract.
 */
public final class ErrorCodes {

    public static final String NOT_FOUND            = "ERR_NOT_FOUND";
    public static final String VALIDATION           = "ERR_VALIDATION";
    public static final String CONFLICT             = "ERR_CONFLICT";
    public static final String UPSTREAM_UNAVAILABLE = "ERR_UPSTREAM_UNAVAILABLE";
    public static final String INVALID_CURSOR       = "ERR_INVALID_CURSOR";
    public static final String MALFORMED_REQUEST    = "ERR_MALFORMED_REQUEST";
    public static final String MISSING_PARAMETER    = "ERR_MISSING_PARAMETER";
    public static final String TYPE_MISMATCH        = "ERR_TYPE_MISMATCH";
    public static final String INTERNAL             = "ERR_INTERNAL";
    public static final String FORBIDDEN            = "ERR_FORBIDDEN";
    public static final String UNAUTHORIZED         = "ERR_UNAUTHORIZED";
    public static final String SNAPSHOT_VERIFICATION_FAILED = "ERR_SNAPSHOT_VERIFICATION_FAILED";

    private ErrorCodes() {}
}
