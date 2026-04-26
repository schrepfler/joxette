# Error Handling

Joxette serves every error as an **RFC 7807** `application/problem+json`
response with a stable set of extension fields. Clients can rely on the
`type` URI and `errorCode` string as machine-readable identifiers — the
`title` and `detail` strings are human-readable and may change.

---

## Error contract

| Field         | Type     | Source              | Meaning                                                              |
|---------------|----------|---------------------|----------------------------------------------------------------------|
| `type`        | URI      | RFC 7807            | Stable problem-type URI, e.g. `https://joxette.dev/problems/not-found`. |
| `title`       | string   | RFC 7807            | Short, human-readable summary.                                       |
| `status`      | integer  | RFC 7807            | HTTP status code (duplicated for clients that only see the body).    |
| `detail`      | string   | RFC 7807            | Human-readable explanation specific to this occurrence.              |
| `instance`    | URI      | RFC 7807 (optional) | Not currently populated; reserved.                                   |
| `errorCode`   | string   | Joxette extension   | Stable, machine-readable code, e.g. `ERR_NOT_FOUND`.                 |
| `timestamp`   | string   | Joxette extension   | ISO-8601 instant the error response was constructed.                 |
| `path`        | string   | Joxette extension   | Request URI that produced the error.                                 |
| `traceId`     | string   | Joxette extension   | Correlation ID copied from SLF4J MDC when present.                   |
| `errors`      | array    | Joxette extension   | Per-field validation failures (bean-validation only).                |

The response `Content-Type` is always `application/problem+json`, including
for validation failures and 500 fallbacks. Bodies never contain stack traces
or internal class names — 5xx responses carry a generic `detail` and the full
exception is logged server-side.

### Example: 404 on unknown topic

```http
HTTP/1.1 404 Not Found
Content-Type: application/problem+json

{
  "type": "https://joxette.dev/problems/not-found",
  "title": "Not Found",
  "status": 404,
  "detail": "Topic not found: orders",
  "errorCode": "ERR_NOT_FOUND",
  "timestamp": "2026-04-26T14:02:10.123Z",
  "path": "/topics/orders"
}
```

### Example: 400 with field-level validation errors

```http
HTTP/1.1 400 Bad Request
Content-Type: application/problem+json

{
  "type": "https://joxette.dev/problems/validation",
  "title": "Validation Failed",
  "status": 400,
  "detail": "topic: must not be blank",
  "errorCode": "ERR_VALIDATION",
  "timestamp": "2026-04-26T14:02:10.123Z",
  "path": "/topics",
  "errors": [
    { "field": "topic", "message": "must not be blank" }
  ]
}
```

---

## Error code catalog

| `errorCode`                | `type` URI                                               | HTTP  | Meaning                                                            |
|----------------------------|----------------------------------------------------------|-------|--------------------------------------------------------------------|
| `ERR_NOT_FOUND`            | `https://joxette.dev/problems/not-found`                 | 404   | Resource (topic, entity, matcher, snapshot, …) does not exist.     |
| `ERR_VALIDATION`           | `https://joxette.dev/problems/validation`                | 400   | Request violated a bean-validation constraint or domain rule.      |
| `ERR_MALFORMED_REQUEST`    | `https://joxette.dev/problems/validation`                | 400   | JSON body could not be parsed at all.                              |
| `ERR_MISSING_PARAMETER`    | `https://joxette.dev/problems/validation`                | 400   | Required query/form parameter not supplied.                        |
| `ERR_TYPE_MISMATCH`        | `https://joxette.dev/problems/validation`                | 400   | Query or path parameter had the wrong type (e.g. non-numeric `limit`). |
| `ERR_INVALID_CURSOR`       | `https://joxette.dev/problems/invalid-cursor`            | 400   | Cursor could not be decoded (wrong signature, expired, malformed). |
| `ERR_CONFLICT`             | `https://joxette.dev/problems/conflict`                  | 409   | Duplicate resource, violated state transition, or capacity reached. |
| `ERR_UPSTREAM_UNAVAILABLE` | `https://joxette.dev/problems/upstream-unavailable`      | 503   | A downstream dependency (broker, object store, DuckDB) is unreachable. |
| `ERR_INTERNAL`             | `https://joxette.dev/problems/internal`                  | 500   | Uncaught exception; the cause is logged, not returned.             |

---

## Streaming error contract

The HTTP status of a streaming response (SSE or NDJSON) is fixed the moment
the response is committed. Errors encountered **mid-stream** cannot change
the status, so Joxette emits a final framed event that mirrors the
ProblemDetail body.

### SSE (`text/event-stream`)

A terminal event named `error` is written, with a JSON body carrying the
same fields as a ProblemDetail response, followed by a normal `complete()`:

```
event: error
data: {"type":"https://joxette.dev/problems/internal","title":"Internal Server Error","status":500,"detail":"Stream terminated abnormally. See server logs for details.","errorCode":"ERR_INTERNAL","timestamp":"2026-04-26T14:02:10.123Z"}

```

Clients distinguish a normal end-of-stream from an error termination by the
presence of an `error` event.

### NDJSON (`application/x-ndjson`)

A final line is written wrapping the payload under an `_error` key, after
which the stream is closed normally:

```
{"_error":{"type":"https://joxette.dev/problems/internal","title":"Internal Server Error","status":500,"detail":"Stream terminated abnormally. See server logs for details.","errorCode":"ERR_INTERNAL","timestamp":"2026-04-26T14:02:10.123Z"}}
```

Data lines never contain an `_error` key, so the distinction is unambiguous.

---

## Adding a new typed exception

Every domain error is a subclass of
`com.joxette.api.error.JoxetteException`. To add a new one:

1. **Pick (or add) a type URI** in
   `com.joxette.api.error.ErrorTypes`. The URI is part of the public API
   contract — choose a stable path under `https://joxette.dev/problems/`
   and never rename it. If no existing URI fits, add a new constant.

2. **Pick (or add) an error code** in
   `com.joxette.api.error.ErrorCodes`. Use the `ERR_…` prefix and keep
   the string in `SCREAMING_SNAKE_CASE`. This is also a stable contract.

3. **Create the exception class** in `com.joxette.api.error`. Extend
   `JoxetteException` and wire the constants through the constructor:

   ```java
   public class RateLimitedException extends JoxetteException {
       public RateLimitedException(String detail) {
           super(HttpStatus.TOO_MANY_REQUESTS, ErrorTypes.RATE_LIMITED,
                 "Rate Limited", detail, ErrorCodes.RATE_LIMITED);
       }

       public static RateLimitedException perClient(String clientId, int limit) {
           return new RateLimitedException(
                   "Client '" + clientId + "' exceeded " + limit + " req/s");
       }
   }
   ```

4. **Throw from the service layer**. Controllers should not catch and
   translate — the `GlobalExceptionHandler` picks up every
   `JoxetteException` automatically and renders it with the full set of
   extension fields.

5. **Update the catalog table above** and, if the new code can appear
   mid-stream, confirm `SseReplayHandler.problemPayload` renders it
   correctly (no changes needed — the helper reflects over any
   `JoxetteException`).

6. **Cover it with a MockMvc test** under
   `joxette-service/src/test/java/com/joxette/api/error/` using the shared
   `ProblemDetailAssertions` helpers, and a parameterized row in
   `GlobalExceptionHandlerTest.joxetteExceptions` so the base mapping
   contract is exercised.
