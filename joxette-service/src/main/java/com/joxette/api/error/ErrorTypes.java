package com.joxette.api.error;

import java.net.URI;

/**
 * Canonical problem type URIs. These are stable identifiers that clients match on,
 * independent of the human-readable title or detail.
 */
public final class ErrorTypes {

    public static final URI NOT_FOUND            = URI.create("https://joxette.dev/problems/not-found");
    public static final URI VALIDATION           = URI.create("https://joxette.dev/problems/validation");
    public static final URI CONFLICT             = URI.create("https://joxette.dev/problems/conflict");
    public static final URI UPSTREAM_UNAVAILABLE = URI.create("https://joxette.dev/problems/upstream-unavailable");
    public static final URI INVALID_CURSOR       = URI.create("https://joxette.dev/problems/invalid-cursor");
    public static final URI INTERNAL             = URI.create("https://joxette.dev/problems/internal");

    private ErrorTypes() {}
}
