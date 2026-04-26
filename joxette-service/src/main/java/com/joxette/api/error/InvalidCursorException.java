package com.joxette.api.error;

import org.springframework.http.HttpStatus;

public class InvalidCursorException extends JoxetteException {

    public InvalidCursorException(String detail) {
        super(HttpStatus.BAD_REQUEST, ErrorTypes.INVALID_CURSOR, "Invalid Cursor", detail, ErrorCodes.INVALID_CURSOR);
    }

    public InvalidCursorException(String detail, Throwable cause) {
        super(HttpStatus.BAD_REQUEST, ErrorTypes.INVALID_CURSOR, "Invalid Cursor", detail, ErrorCodes.INVALID_CURSOR, cause);
    }

    public static InvalidCursorException malformed(Throwable cause) {
        return new InvalidCursorException("Cursor could not be decoded", cause);
    }
}
