package com.joxette.api.error;

import org.springframework.http.HttpStatus;

public class UnauthorizedException extends JoxetteException {

    public UnauthorizedException(String detail) {
        super(HttpStatus.UNAUTHORIZED, ErrorTypes.UNAUTHORIZED, "Unauthorized", detail, ErrorCodes.UNAUTHORIZED);
    }

    public static UnauthorizedException missingOrInvalidApiKey() {
        return new UnauthorizedException("Missing or invalid X-API-Key header");
    }
}
