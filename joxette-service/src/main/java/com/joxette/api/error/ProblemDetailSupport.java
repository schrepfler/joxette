package com.joxette.api.error;

import org.slf4j.MDC;
import org.springframework.http.ProblemDetail;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Shared construction logic for the extension fields every Joxette error response carries,
 * used both by {@link GlobalExceptionHandler} (for exceptions thrown inside the
 * DispatcherServlet) and by {@code SecurityConfig.ApiKeyAuthenticationEntryPoint} (for
 * authentication failures raised by the Spring Security filter chain, upstream of the
 * DispatcherServlet, which never reaches {@code @RestControllerAdvice}).
 *
 * <p>Centralizing this here means a new field (e.g. {@code traceId}) only needs to be added
 * in one place instead of being hand-kept in sync across two call sites.
 */
public final class ProblemDetailSupport {

    private static final String MDC_TRACE_ID = "traceId";

    private ProblemDetailSupport() {}

    /**
     * Adds the standard extension fields — {@code timestamp}, {@code path} (when non-null),
     * {@code errorCode}, and {@code traceId} (when present in SLF4J MDC) — to a
     * {@link ProblemDetail} that already has its RFC 7807 core fields
     * ({@code type}/{@code title}/{@code status}/{@code detail}) set.
     */
    public static void decorate(ProblemDetail body, String errorCode, String path) {
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

    /**
     * Flattens a decorated {@link ProblemDetail} into the same top-level field shape that
     * Spring MVC's {@code ProblemDetailJacksonMixin} produces on the response body — RFC 7807
     * core fields followed by extension properties in the order they were set. Callers that
     * serialize through a raw {@code ObjectMapper} bean outside Spring MVC's message-converter
     * pipeline (which is where that mixin is registered) need this to match
     * {@link GlobalExceptionHandler}'s response shape instead of nesting extensions under a
     * {@code "properties"} object.
     */
    public static Map<String, Object> toFlatMap(ProblemDetail body) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("type", body.getType().toString());
        map.put("title", body.getTitle());
        map.put("status", body.getStatus());
        map.put("detail", body.getDetail());
        map.put("instance", body.getInstance());
        if (body.getProperties() != null) {
            map.putAll(body.getProperties());
        }
        return map;
    }
}
