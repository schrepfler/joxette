package com.joxette.api.error;

import org.springframework.http.HttpStatus;

public class ResourceNotFoundException extends JoxetteException {

    public ResourceNotFoundException(String detail) {
        super(HttpStatus.NOT_FOUND, ErrorTypes.NOT_FOUND, "Not Found", detail, ErrorCodes.NOT_FOUND);
    }

    public static ResourceNotFoundException topic(String topic) {
        return new ResourceNotFoundException("Topic not found: " + topic);
    }

    public static ResourceNotFoundException entityType(String entityType) {
        return new ResourceNotFoundException("Entity type not found: " + entityType);
    }

    public static ResourceNotFoundException entity(String entityType, String entityId) {
        return new ResourceNotFoundException("Entity not found: " + entityType + "/" + entityId);
    }

    public static ResourceNotFoundException broker(String brokerId) {
        return new ResourceNotFoundException("Broker not found: " + brokerId);
    }
}
