package com.joxette.api.error;

import org.springframework.http.HttpStatus;

public class ValidationException extends JoxetteException {

    public ValidationException(String detail) {
        super(HttpStatus.BAD_REQUEST, ErrorTypes.VALIDATION, "Validation Failed", detail, ErrorCodes.VALIDATION);
    }

    public static ValidationException field(String field, String reason) {
        return new ValidationException("Invalid value for '" + field + "': " + reason);
    }
}
