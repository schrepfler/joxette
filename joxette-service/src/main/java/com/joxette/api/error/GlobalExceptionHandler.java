package com.joxette.api.error;

import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.ErrorResponseException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.resource.NoResourceFoundException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Central RFC 7807 error handler. Maps every thrown exception — both
 * {@link JoxetteException} domain errors and Spring MVC framework exceptions —
 * to a {@link ProblemDetail} response with a consistent set of extension fields:
 * {@code timestamp}, {@code path}, {@code errorCode}, and {@code traceId}
 * (when an MDC traceId is present).
 */
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    private static final String MDC_TRACE_ID = "traceId";

    @ExceptionHandler(JoxetteException.class)
    public ResponseEntity<ProblemDetail> handleJoxette(JoxetteException ex, HttpServletRequest request) {
        ProblemDetail body = ProblemDetail.forStatusAndDetail(ex.status(), ex.detail());
        body.setType(ex.type());
        body.setTitle(ex.title());
        decorate(body, ex.errorCode(), request.getRequestURI());
        if (ex.status().is5xxServerError()) {
            log.error("Joxette server error [{}]: {}", ex.errorCode(), ex.detail(), ex);
        } else {
            log.debug("Joxette client error [{}]: {}", ex.errorCode(), ex.detail());
        }
        return ResponseEntity.status(ex.status()).body(body);
    }

    @ExceptionHandler(SQLException.class)
    public ResponseEntity<ProblemDetail> handleSqlException(SQLException ex, HttpServletRequest request) {
        log.error("Unhandled SQL error", ex);
        ProblemDetail body = ProblemDetail.forStatusAndDetail(
                HttpStatus.SERVICE_UNAVAILABLE,
                "Database error: " + ex.getMessage());
        body.setType(ErrorTypes.UPSTREAM_UNAVAILABLE);
        body.setTitle("Upstream Unavailable");
        decorate(body, ErrorCodes.UPSTREAM_UNAVAILABLE, request.getRequestURI());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(body);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ProblemDetail> handleUnexpected(Exception ex, HttpServletRequest request) {
        log.error("Unhandled exception", ex);
        ProblemDetail body = ProblemDetail.forStatusAndDetail(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "An unexpected error occurred. See server logs for details.");
        body.setType(ErrorTypes.INTERNAL);
        body.setTitle("Internal Server Error");
        decorate(body, ErrorCodes.INTERNAL, request.getRequestURI());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body);
    }

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(
            MethodArgumentNotValidException ex, HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        List<Map<String, String>> fieldErrors = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> Map.of(
                        "field", fe.getField(),
                        "message", fe.getDefaultMessage() == null ? "invalid" : fe.getDefaultMessage()))
                .toList();
        String detail = fieldErrors.stream()
                .map(m -> m.get("field") + ": " + m.get("message"))
                .collect(Collectors.joining("; "));
        if (detail.isEmpty()) {
            detail = "Request body failed validation";
        }
        ProblemDetail body = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, detail);
        body.setType(ErrorTypes.VALIDATION);
        body.setTitle("Validation Failed");
        decorate(body, ErrorCodes.VALIDATION, pathOf(request));
        if (!fieldErrors.isEmpty()) {
            body.setProperty("errors", fieldErrors);
        }
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(body);
    }

    @Override
    protected ResponseEntity<Object> handleHttpMessageNotReadable(
            HttpMessageNotReadableException ex, HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        return buildFrameworkProblem(HttpStatus.BAD_REQUEST, ErrorTypes.VALIDATION, "Malformed Request",
                "Request body could not be parsed", ErrorCodes.MALFORMED_REQUEST, request);
    }

    @Override
    protected ResponseEntity<Object> handleNoResourceFoundException(
            NoResourceFoundException ex, HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        return buildFrameworkProblem(HttpStatus.NOT_FOUND, ErrorTypes.NOT_FOUND, "Not Found",
                "No endpoint " + ex.getHttpMethod() + " " + ex.getResourcePath(), ErrorCodes.NOT_FOUND, request);
    }

    @Override
    protected ResponseEntity<Object> handleMissingServletRequestParameter(
            MissingServletRequestParameterException ex, HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        String detail = "Missing required parameter '" + ex.getParameterName() + "'";
        return buildFrameworkProblem(HttpStatus.BAD_REQUEST, ErrorTypes.VALIDATION, "Missing Parameter",
                detail, ErrorCodes.MISSING_PARAMETER, request);
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ProblemDetail> handleTypeMismatch(
            MethodArgumentTypeMismatchException ex, HttpServletRequest request) {
        String expected = ex.getRequiredType() == null ? "?" : ex.getRequiredType().getSimpleName();
        String detail = "Parameter '" + ex.getName() + "' must be of type " + expected;
        ProblemDetail body = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, detail);
        body.setType(ErrorTypes.VALIDATION);
        body.setTitle("Type Mismatch");
        decorate(body, ErrorCodes.TYPE_MISMATCH, request.getRequestURI());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(body);
    }

    // Fallback for any other ErrorResponseException coming from the framework — keeps
    // the decorated extension fields consistent with the rest of the API.
    @Override
    protected ResponseEntity<Object> handleExceptionInternal(
            Exception ex, Object body, HttpHeaders headers, HttpStatusCode statusCode, WebRequest request) {
        HttpStatus status = HttpStatus.resolve(statusCode.value());
        if (status == null) {
            status = HttpStatus.INTERNAL_SERVER_ERROR;
        }
        String detail = ex.getMessage() == null ? status.getReasonPhrase() : ex.getMessage();
        ProblemDetail problem = (body instanceof ProblemDetail pd)
                ? pd
                : ProblemDetail.forStatusAndDetail(status, detail);
        if (problem.getTitle() == null) {
            problem.setTitle(status.getReasonPhrase());
        }
        String code = (ex instanceof ErrorResponseException ere && ere.getBody().getTitle() != null)
                ? "ERR_" + ere.getBody().getTitle().toUpperCase().replace(' ', '_')
                : "ERR_" + status.name();
        decorate(problem, code, pathOf(request));
        return ResponseEntity.status(status).headers(headers).body(problem);
    }

    private ResponseEntity<Object> buildFrameworkProblem(
            HttpStatus status, java.net.URI type, String title, String detail, String errorCode, WebRequest request) {
        ProblemDetail body = ProblemDetail.forStatusAndDetail(status, detail);
        body.setType(type);
        body.setTitle(title);
        decorate(body, errorCode, pathOf(request));
        return ResponseEntity.status(status).body(body);
    }

    private static void decorate(ProblemDetail body, String errorCode, String path) {
        body.setProperty("timestamp", Instant.now().toString());
        if (path != null) {
            body.setProperty("path", path);
        }
        body.setProperty("errorCode", errorCode);
        String traceId = MDC.get(MDC_TRACE_ID);
        if (traceId != null && !traceId.isBlank()) {
            body.setProperty("traceId", traceId);
        }
    }

    private static String pathOf(WebRequest request) {
        String desc = request.getDescription(false);
        // getDescription(false) returns "uri=/foo/bar"
        if (desc != null && desc.startsWith("uri=")) {
            return desc.substring(4);
        }
        return desc;
    }

}
