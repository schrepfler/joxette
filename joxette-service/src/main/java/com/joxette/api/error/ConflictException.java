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

    public static ConflictException brokerAlreadyExists(String brokerId) {
        return new ConflictException("Broker already registered: " + brokerId);
    }

    public static ConflictException snapshotAlreadyExists(String name) {
        return new ConflictException("Snapshot already exists: " + name);
    }

    public static ConflictException brokerInUse(String brokerId) {
        return new ConflictException("Cannot delete broker '" + brokerId + "': referenced by topics");
    }

    public static ConflictException compactionAlreadyRunning() {
        return new ConflictException("Compaction already in progress");
    }

    public static ConflictException retentionAlreadyRunning() {
        return new ConflictException("Retention run already in progress");
    }

    public static ConflictException scheduledReplayCapacityReached(int max) {
        return new ConflictException(
                "Maximum number of concurrent scheduled replays (" + max + ") reached");
    }

    public static ConflictException followCapacityReached(int max) {
        return new ConflictException("Max follow subscriptions reached (" + max + ")");
    }

    public static ConflictException scheduledReplayCannotCancel(String status) {
        return new ConflictException("Cannot cancel replay in status: " + status);
    }
}
