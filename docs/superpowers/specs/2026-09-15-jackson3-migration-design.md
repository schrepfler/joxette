# Jackson 2 → Jackson 3 Migration

## Motivation

While evaluating whether to adopt Jox 0.6.0's new `jox-json` module
(`com.softwaremill.jox:json`) for NDJSON streaming, we found it's built on
Jackson 3 (`tools.jackson.databind`), while Joxette's entire JSON stack —
the shared mapper bean, `TransformStepJacksonModule`, error-body
serialization, and every DTO — is built on Jackson 2
(`com.fasterxml.jackson.databind`).

Separately, Spring Boot 4.1 documents Jackson 3 as the default/preferred
mapper and Jackson 2 support as **deprecated, scheduled for removal**,
provided only to ease migration (`spring-boot-jackson2` module,
`Jackson2AutoConfiguration`). Joxette currently pins an explicit Jackson 2
dependency and defines its own `ObjectMapper` bean (`OpenApiConfig`),
which keeps the app on the deprecated path even though Spring Boot 4
ships Jackson 3 natively.

This migration gets Joxette off the deprecated Jackson 2 path and onto
Spring Boot 4's native default. It is a prerequisite for a follow-up spec
that migrates NDJSON streaming (`SseReplayHandler`, `ExportService`) to
`jox-json`, which is not part of this design.

## Scope

**In scope:**
- The shared JSON mapper bean (currently `OpenApiConfig.objectMapper()`).
- `TransformStepJacksonModule` and any other custom `SimpleModule` /
  `StdSerializer` / `StdDeserializer`.
- All DTOs/records using Jackson databind types (`ObjectMapper`,
  `JsonNode`, `ObjectReader`, `ObjectWriter`, `ObjectNode`, `TextNode`,
  `JsonNodeFactory`) and Jackson core types (`JsonParser`, `JsonGenerator`,
  `TypeReference`, `JsonProcessingException`).
- `SecurityConfig`'s `ApiKeyAuthenticationEntryPoint` error-body
  serialization and any other direct mapper consumer.
- Spring MVC's registered `HttpMessageConverter` (moving from the
  deprecated `MappingJackson2HttpMessageConverter` to Jackson 3's
  `JacksonJsonHttpMessageConverter`).
- All four modules' main **and** test sources: `joxette-core`,
  `joxette-kafka`, `joxette-sol`, `joxette-service` (~121 files total:
  82 main + 39 test, by current grep count).
- Removing the explicit Jackson 2 version pin (`com.fasterxml.jackson.core:
  jackson-databind`) from the root `pom.xml` once nothing in our own code
  references it.

**Out of scope:**
- `springdoc-openapi` / `swagger-core-jakarta`'s own internal Jackson 2
  usage for OpenAPI spec generation (`jackson-dataformat-yaml`,
  `jackson-datatype-jsr310` under `com.fasterxml.jackson`, pulled in
  transitively by `swagger-core-jakarta:2.2.52`). This is contained inside
  springdoc's own spec-generation path, uses its own internal mapper (not
  ours), and is not something we control or need to touch.
- Any other third-party library's internal, transitive use of Jackson 2
  (e.g. AWS SDK's `third-party-jackson-core`) — fine as long as it doesn't
  depend on autowiring *our* `ObjectMapper` bean by type.
- `com.jayway.jsonpath` (JsonPath) — no change needed. It defaults to the
  `json-smart` provider, not Jackson; confirmed no `JacksonJsonProvider` /
  `Configuration.setDefaults` wiring exists in the codebase.
- Migrating NDJSON streaming to `jox-json` — separate follow-up spec.
- The Jox 0.6.0 version bump itself — already done (`pom.xml`
  `jox.flows.version`), full test suite passed.

## Target Architecture

- Replace the `ObjectMapper` `@Bean` in `OpenApiConfig` with a
  `tools.jackson.databind.json.JsonMapper` bean built via
  `JsonMapper.builder()`. Jackson 3's `jackson-databind` bundles java-time
  (de)serialization natively (confirmed: `tools/jackson/databind/ext/
  javatime/**` ships inside `jackson-databind-3.1.x.jar`) — no
  `JavaTimeModule` needed. The current `WRITE_DATES_AS_TIMESTAMPS=false`
  behavior (ISO-8601 date strings, matching the `Replay Message Format`
  documented in the project's `CLAUDE.md`) is reproduced via the
  equivalent Jackson 3 date-time builder setting.
- `TransformStepJacksonModule` (and any other `SimpleModule` /
  `StdSerializer` / `StdDeserializer`) is rewritten against
  `tools.jackson.databind.*` types and registered on the `JsonMapper`
  builder, since Jackson 3's mapper is immutable/builder-based rather than
  the mutable, post-construction-configurable Jackson 2 `ObjectMapper`.
- Jackson **annotations** (`@JsonProperty`, `@JsonCreator`,
  `@JsonInclude`, `@JsonTypeInfo`, `@JsonSubTypes`, `@JsonValue`) stay on
  `com.fasterxml.jackson.annotation.*` — that package is unchanged in
  Jackson 3 (confirmed via the resolved dependency tree: `jackson-
  annotations` still ships under the `com.fasterxml.jackson.core` groupId
  and is pulled in transitively by `jackson-databind:3.1.5`). Most DTOs
  therefore only need their databind/core imports repointed, not a
  rewrite of every annotated class.
- Spring MVC's registered converter becomes Jackson 3's
  `JacksonJsonHttpMessageConverter`, Spring Boot 4's default once no
  Jackson 2 `ObjectMapper` bean is present to force the deprecated
  `MappingJackson2HttpMessageConverter` path.
- End state: the explicit Jackson 2 version pin is removed from the root
  `pom.xml`. Jackson 2 remains on the runtime classpath only as
  springdoc/swagger-core's own transitive dependency, never referenced by
  Joxette code.

## Migration Sequencing

Executed as one branch, staged as reviewable/bisectable commits — each
stage's tests must be green before the next stage starts:

1. **`joxette-core`** (no Spring, no DuckDB — smallest surface): repoint
   `MessageTransformer` and any other core Jackson usage to
   `tools.jackson.*`; update its tests.
2. **`joxette-kafka`**: repoint Jackson usage in message (de)serialization
   paths and tests.
3. **`joxette-sol`**: repoint Jackson usage in the SOL DSL/engine and
   tests.
4. **`joxette-service`** (the largest module):
   a. Introduce the new `JsonMapper` bean and rewritten
      `TransformStepJacksonModule` first.
   b. Sweep DTOs, `SseReplayHandler`, `ExportService`, `SecurityConfig`,
      `CassetteController`, the transform pipeline, and their tests,
      package by package.
   c. Remove the old `ObjectMapper` bean and the Jackson 2 dependency pin
      as the final commit in this stage.
5. **Full-suite verification**: `mvn test` across all modules, plus a
   manual check of a couple of replay endpoints (`application/json` and
   `application/x-ndjson`) against the running service.

Each module stage lands as its own commit (or small commit group) so a
regression bisects to the exact module/stage rather than one large diff.

## Testing & Verification Strategy

- Run `mvn test` (or the relevant module subset) after each stage above;
  no stage proceeds on red tests.
- Add a focused test that pins the exact wire shape Jackson 3 produces
  for a representative `MessageResponse` (timestamp format, header
  structure, `VARIANT`/`BigDecimal` fields) against a captured
  Jackson-2-era fixture, so subtle serialization drift (date formatting,
  number precision) is caught explicitly rather than incidentally.
- Existing Testcontainers-based integration tests (Kafka + DuckDB) serve
  as the main end-to-end regression net for the recording/replay
  pipeline.
- No WireMock/RestAssured dependency exists in this project, so there's
  no external JSON-contract tooling to reconcile.
- Final manual gate: exercise `GET /cassettes/topics/{topic}` in both
  `application/json` and `application/x-ndjson` via the running service.

## Risks & Non-Goals

- **Risk:** Jackson 3's date-time feature name/default may differ subtly
  from Jackson 2's `WRITE_DATES_AS_TIMESTAMPS`. Mitigated by the
  fixture-comparison test above.
- **Risk:** a third-party integration (e.g. AWS SDK's
  `third-party-jackson-core`) could in principle probe for a Jackson 2
  `ObjectMapper` bean by type. Verify nothing in the Spring context
  autowires `com.fasterxml.jackson.databind.ObjectMapper` expecting our
  bean specifically before removing it (springdoc uses its own internal
  mapper, not ours, so it's unaffected).
- **Non-goal:** touching springdoc/swagger-core's bundled Jackson 2 —
  out of our control and irrelevant to the app's own wire format.
- **Non-goal:** the NDJSON/`jox-json` migration itself — tracked as a
  separate follow-up spec once this lands.
