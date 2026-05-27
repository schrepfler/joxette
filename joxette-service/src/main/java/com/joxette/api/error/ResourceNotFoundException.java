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

    public static ResourceNotFoundException snapshot(String name) {
        return new ResourceNotFoundException("Snapshot not found: " + name);
    }

    public static ResourceNotFoundException transformPreset(String name) {
        return new ResourceNotFoundException("Transform preset not found: " + name);
    }

    public static ResourceNotFoundException scheduledReplay(String id) {
        return new ResourceNotFoundException("Scheduled replay not found: " + id);
    }

    public static ResourceNotFoundException topicMatcher(String topic, String messageType) {
        return new ResourceNotFoundException(
                "Matcher not found: topic=" + topic + " messageType=" + messageType);
    }

    public static ResourceNotFoundException entitySource(String entityType, String topic) {
        return new ResourceNotFoundException(
                "Entity source mapping not found: " + entityType + " → " + topic);
    }

    public static ResourceNotFoundException entityMatcher(String entityType, String topic, String messageType) {
        return new ResourceNotFoundException(
                "Entity matcher not found: " + entityType + " / " + topic + " / " + messageType);
    }

    public static ResourceNotFoundException compactionRun(long id) {
        return new ResourceNotFoundException("Compaction run not found: " + id);
    }

    public static ResourceNotFoundException retentionRun(long id) {
        return new ResourceNotFoundException("Retention run not found: " + id);
    }

    public static ResourceNotFoundException stream(String id) {
        return new ResourceNotFoundException("Stream definition not found: " + id);
    }
}
