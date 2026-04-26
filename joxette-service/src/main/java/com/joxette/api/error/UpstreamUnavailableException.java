package com.joxette.api.error;

import org.springframework.http.HttpStatus;

public class UpstreamUnavailableException extends JoxetteException {

    public UpstreamUnavailableException(String detail) {
        super(HttpStatus.SERVICE_UNAVAILABLE, ErrorTypes.UPSTREAM_UNAVAILABLE,
                "Upstream Unavailable", detail, ErrorCodes.UPSTREAM_UNAVAILABLE);
    }

    public UpstreamUnavailableException(String detail, Throwable cause) {
        super(HttpStatus.SERVICE_UNAVAILABLE, ErrorTypes.UPSTREAM_UNAVAILABLE,
                "Upstream Unavailable", detail, ErrorCodes.UPSTREAM_UNAVAILABLE, cause);
    }

    public static UpstreamUnavailableException broker(String brokerId, Throwable cause) {
        return new UpstreamUnavailableException("Broker unavailable: " + brokerId, cause);
    }

    public static UpstreamUnavailableException objectStore(String detail, Throwable cause) {
        return new UpstreamUnavailableException("Object store unavailable: " + detail, cause);
    }
}
