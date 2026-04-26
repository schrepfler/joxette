package com.joxette.api.error;

import org.springframework.http.HttpStatus;

import java.net.URI;
import java.util.Objects;

/**
 * Base type for all domain exceptions that should surface as RFC 7807 ProblemDetail
 * responses. Carries the status, type URI, title, detail message, and a stable
 * machine-readable error code that the {@link GlobalExceptionHandler} copies into
 * the response body.
 */
public abstract class JoxetteException extends RuntimeException {

    private final HttpStatus status;
    private final URI type;
    private final String title;
    private final String errorCode;

    protected JoxetteException(HttpStatus status, URI type, String title, String detail, String errorCode) {
        super(detail);
        this.status = Objects.requireNonNull(status, "status");
        this.type = Objects.requireNonNull(type, "type");
        this.title = Objects.requireNonNull(title, "title");
        this.errorCode = Objects.requireNonNull(errorCode, "errorCode");
    }

    protected JoxetteException(HttpStatus status, URI type, String title, String detail, String errorCode, Throwable cause) {
        super(detail, cause);
        this.status = Objects.requireNonNull(status, "status");
        this.type = Objects.requireNonNull(type, "type");
        this.title = Objects.requireNonNull(title, "title");
        this.errorCode = Objects.requireNonNull(errorCode, "errorCode");
    }

    public HttpStatus status() {
        return status;
    }

    public URI type() {
        return type;
    }

    public String title() {
        return title;
    }

    public String detail() {
        return getMessage();
    }

    public String errorCode() {
        return errorCode;
    }
}
