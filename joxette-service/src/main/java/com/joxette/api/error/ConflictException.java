package com.joxette.api.error;

import org.springframework.http.HttpStatus;

public class ConflictException extends JoxetteException {

    public ConflictException(String detail) {
        super(HttpStatus.CONFLICT, ErrorTypes.CONFLICT, "Conflict", detail, ErrorCodes.CONFLICT);
    }

    public static ConflictException topicAlreadyExists(String topic) {
        return new ConflictException("Topic already registered: " + topic);
    }

    public static ConflictException entityTypeAlreadyExists(String entityType) {
        return new ConflictException("Entity type already registered: " + entityType);
    }
}
