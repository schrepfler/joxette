package com.joxette.api.error;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;

import java.net.URI;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalExceptionHandlerTest {

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    static Stream<Arguments> joxetteExceptions() {
        return Stream.of(
                Arguments.of(ResourceNotFoundException.topic("orders"),
                        HttpStatus.NOT_FOUND, ErrorTypes.NOT_FOUND, ErrorCodes.NOT_FOUND),
                Arguments.of(new ValidationException("bad field"),
                        HttpStatus.BAD_REQUEST, ErrorTypes.VALIDATION, ErrorCodes.VALIDATION),
                Arguments.of(ConflictException.topicAlreadyExists("orders"),
                        HttpStatus.CONFLICT, ErrorTypes.CONFLICT, ErrorCodes.CONFLICT),
                Arguments.of(new UpstreamUnavailableException("kafka down"),
                        HttpStatus.SERVICE_UNAVAILABLE, ErrorTypes.UPSTREAM_UNAVAILABLE, ErrorCodes.UPSTREAM_UNAVAILABLE),
                Arguments.of(new InvalidCursorException("bad cursor"),
                        HttpStatus.BAD_REQUEST, ErrorTypes.INVALID_CURSOR, ErrorCodes.INVALID_CURSOR)
        );
    }

    @ParameterizedTest
    @MethodSource("joxetteExceptions")
    void mapsJoxetteExceptionToProblemDetail(
            JoxetteException ex, HttpStatus expectedStatus, URI expectedType, String expectedErrorCode) {

        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/some/path");
        ResponseEntity<ProblemDetail> response = handler.handleJoxette(ex, request);

        assertThat(response.getStatusCode()).isEqualTo(expectedStatus);
        ProblemDetail body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.getStatus()).isEqualTo(expectedStatus.value());
        assertThat(body.getType()).isEqualTo(expectedType);
        assertThat(body.getTitle()).isEqualTo(ex.title());
        assertThat(body.getDetail()).isEqualTo(ex.detail());
        assertThat(body.getProperties()).isNotNull();
        assertThat(body.getProperties().get("errorCode")).isEqualTo(expectedErrorCode);
        assertThat(body.getProperties().get("path")).isEqualTo("/some/path");
        assertThat(body.getProperties().get("timestamp")).isNotNull();
    }

    @Test
    void unhandledExceptionMapsToInternalServerError() {
        HttpServletRequest request = new MockHttpServletRequest("GET", "/boom");

        ResponseEntity<ProblemDetail> response =
                handler.handleUnexpected(new RuntimeException("kaboom"), request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        ProblemDetail body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.getType()).isEqualTo(ErrorTypes.INTERNAL);
        assertThat(body.getDetail()).doesNotContain("kaboom");
        assertThat(body.getProperties().get("errorCode")).isEqualTo(ErrorCodes.INTERNAL);
    }

    @Test
    void factoryHelpersProduceCorrectDetail() {
        assertThat(ResourceNotFoundException.topic("orders").detail())
                .isEqualTo("Topic not found: orders");
        assertThat(ResourceNotFoundException.entityType("order").detail())
                .isEqualTo("Entity type not found: order");
        assertThat(ConflictException.topicAlreadyExists("orders").detail())
                .isEqualTo("Topic already registered: orders");
    }
}
