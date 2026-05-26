package com.joxette.api.error;

import org.springframework.http.HttpStatus;

public class ForbiddenException extends JoxetteException {

    private ForbiddenException(String detail, Throwable cause) {
        super(HttpStatus.FORBIDDEN, ErrorTypes.FORBIDDEN, "Forbidden", detail, ErrorCodes.FORBIDDEN, cause);
    }

    public static ForbiddenException kafkaTopicCreate(String topic, Throwable cause) {
        return new ForbiddenException(
                "Kafka credentials do not have permission to create topic '" + topic + "'", cause);
    }
}
