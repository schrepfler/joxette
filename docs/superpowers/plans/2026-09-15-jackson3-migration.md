# Jackson 2 → Jackson 3 Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move Joxette's own JSON stack from Jackson 2 (`com.fasterxml.jackson.databind`) to Jackson 3 (`tools.jackson.databind`), the mapper Spring Boot 4.1 treats as default/preferred (Jackson 2 support there is deprecated for removal).

**Architecture:** Stage the migration module-by-module (`joxette-core` → `joxette-kafka` → `joxette-sol` → `joxette-service`), each stage green before the next. Inside `joxette-service`, introduce the new `JsonMapper` bean and rewritten custom (de)serializers first (coexisting briefly with the old Jackson 2 bean), then sweep main sources, then test sources, then remove the old bean/dependency as the final cutover commit.

**Tech Stack:** Java 25, Spring Boot 4.1.1, Maven, Jackson 3 (`tools.jackson.core:jackson-core`/`jackson-databind` 3.1.5, BOM-managed via `spring-boot-dependencies`), JUnit 5, AssertJ, Testcontainers.

## Global Constraints

- Every stage's module tests must be green (`mvn -pl <module> -am test`) before moving to the next stage. No stage proceeds on red tests.
- Every stage lands as its own commit (or small commit group) — reviewable and bisectable, per the approved design spec's Migration Sequencing section (`docs/superpowers/specs/2026-09-15-jackson3-migration-design.md`).
- Out of scope, confirmed during planning:
  - `springdoc-openapi`/`swagger-core-jakarta`'s own internal Jackson 2 usage (contained, transitive, uses its own mapper — spec's stated non-goal).
  - **`joxette-operator`** — not mentioned in the approved spec, and confirmed during planning to depend on JOSDK 5.3.4 + Fabric8 7.7.0, Kubernetes client libraries that are Jackson-2-locked internally. Forcing this module onto Jackson 3 risks breaking that integration for no benefit; it keeps using `com.fasterxml.jackson.*` (Jackson 2) throughout, same as springdoc.
  - `joxette-test-kit` — confirmed to contain zero Jackson imports; nothing to do there.
  - `com.jayway.jsonpath` (JsonPath) — confirmed to use its default `json-smart` provider, not Jackson; no code references a `JacksonJsonProvider`. No change needed anywhere it's used.
- Jackson 3 facts this plan relies on (verified against the actual `jackson-databind-3.1.5.jar`/`jackson-core-3.1.5.jar` classes, not assumed):
  - `com.fasterxml.jackson.annotation.*` (the annotations-only package, e.g. `@JsonProperty`, `@JsonInclude`, `@JsonCreator`, `@JsonTypeInfo`, `@JsonSubTypes`, `@JsonValue`) is **unchanged** — still `com.fasterxml.jackson.core:jackson-annotations`, pulled in transitively. Files using only this package need **no changes**.
  - `com.fasterxml.jackson.databind.*` → `tools.jackson.databind.*`; `com.fasterxml.jackson.databind.annotation.*` (e.g. `@JsonSerialize`, `@JsonDeserialize`) → `tools.jackson.databind.annotation.*`; `com.fasterxml.jackson.core.*` → `tools.jackson.core.*`.
  - `ObjectMapper` keeps its name under `tools.jackson.databind` and has a public no-arg constructor (`new ObjectMapper()` still works). `JsonMapper` (`tools.jackson.databind.json.JsonMapper`) extends `ObjectMapper`, is immutable, and is built via `JsonMapper.builder()` (also has a no-arg constructor for the plain-defaults case).
  - **No mutable post-construction module registration.** `ObjectMapper`/`JsonMapper` has **no `registerModule` instance method** in Jackson 3. Any `new ObjectMapper().registerModule(...)` call site must become `JsonMapper.builder().addModule(...).build()`.
  - Java-time (de)serialization is now built into `jackson-databind` core (confirmed: `tools/jackson/databind/ext/javatime/**` ships inside the jar). No `JavaTimeModule` needed or available — any `.registerModule(new JavaTimeModule())` call is simply dropped.
  - `SerializationFeature.WRITE_DATES_AS_TIMESTAMPS` becomes `tools.jackson.databind.cfg.DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS`, toggled via `builder.disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)`.
  - `com.fasterxml.jackson.core.JsonProcessingException` (checked, extends `IOException`) **has no Jackson 3 equivalent by that name** — replaced by `tools.jackson.core.JacksonException` (**unchecked**, extends `RuntimeException`). Every `catch (JsonProcessingException e)` must become `catch (JacksonException e)`.
  - `JsonGenerator.getCodec()` / `JsonParser.getCodec()` are **removed**. Replacements: `SerializationContext.valueToTree(Object)` / `SerializationContext.writeTree(JsonGenerator, TreeNode)` (serialization side, `SerializationContext` is the Jackson 3 rename of `SerializerProvider`); `DeserializationContext.readTreeAsValue(JsonNode, Class<T>)` (deserialization side).
  - `StdSerializer`/`StdDeserializer` keep their names and constructors under `tools.jackson.databind.ser.std`/`deser.std`, but their base classes are renamed `ValueSerializer`/`ValueDeserializer` (from `JsonSerializer`/`JsonDeserializer`), and `serialize`/`deserialize` now take `SerializationContext`/`DeserializationContext` and declare `throws JacksonException` (unchecked) instead of `throws IOException`.
  - `DeserializationContext.instantiationException(Class<?>, String)` still exists, now returns `DatabindException` (unchecked).
  - `ObjectMapper.getTypeFactory()` and `ObjectMapper.treeToValue(TreeNode, Class)` are unchanged.
  - Dependency coordinates: `tools.jackson.core:jackson-databind` (brings in `tools.jackson.core:jackson-core` transitively), version-managed by the existing `spring-boot-dependencies:4.1.1` BOM import in the root `pom.xml` — no explicit `<version>` needed, same pattern the codebase already uses for the Jackson 2 coordinate.

---

### Task 1: `joxette-core` — confirm no migration needed

**Files:** none changed. Verification only.

**Interfaces:** N/A.

- [ ] **Step 1: Confirm the file set**

Run:
```bash
grep -rln "com.fasterxml.jackson" --include="*.java" joxette-core/src
```
Expected: exactly these 7 files, each importing only `com.fasterxml.jackson.annotation.JsonInclude` (verified during planning):
```
joxette-core/src/main/java/com/joxette/replay/ReplayTransformConfig.java
joxette-core/src/main/java/com/joxette/replay/FieldSubstitution.java
joxette-core/src/main/java/com/joxette/replay/PagedResponse.java
joxette-core/src/main/java/com/joxette/replay/ReplayToTopicRequest.java
joxette-core/src/main/java/com/joxette/replay/CassetteRecord.java
joxette-core/src/main/java/com/joxette/replay/EntityRecord.java
joxette-core/src/main/java/com/joxette/replay/ReplayProgress.java
```
If any file appears that is NOT in this list, or any of these 7 files now import something under `com.fasterxml.jackson.core.` or `com.fasterxml.jackson.databind.` (not just `.annotation.`), stop and re-classify it using the same method as Task 5/6 below before continuing — the "no changes needed" conclusion no longer holds for that file.

- [ ] **Step 2: Confirm `joxette-core/pom.xml` only depends on `jackson-annotations`**

Run:
```bash
grep -B2 -A4 -i jackson joxette-core/pom.xml
```
Expected: one dependency block for `com.fasterxml.jackson.core:jackson-annotations` (no `jackson-databind` or `jackson-core`). This dependency does not change — `jackson-annotations` keeps its Jackson-2-era coordinates in Jackson 3 too.

- [ ] **Step 3: Run the module's tests as a baseline**

Run: `mvn -pl joxette-core -am test`
Expected: BUILD SUCCESS, all tests green (this is a no-op check — no files changed, so this just confirms the starting baseline before later stages touch shared build state).

- [ ] **Step 4: No commit**

Nothing changed in this task — there is nothing to commit. Move directly to Task 2.

---

### Task 2: `joxette-kafka` — confirm no migration needed

**Files:** none changed. Verification only.

**Interfaces:** N/A.

- [ ] **Step 1: Confirm zero Jackson usage**

Run:
```bash
grep -rl "com.fasterxml.jackson" --include="*.java" joxette-kafka/src
```
Expected: no output (confirmed empty during planning).

- [ ] **Step 2: Run the module's tests as a baseline**

Run: `mvn -pl joxette-kafka -am test`
Expected: BUILD SUCCESS, all tests green.

- [ ] **Step 3: No commit**

Nothing changed. Move to Task 3.

---

### Task 3: `joxette-sol` — migrate `EntityRecordAdapter`

**Files:**
- Modify: `joxette-sol/pom.xml`
- Modify: `joxette-sol/src/main/java/com/joxette/sol/EntityRecordAdapter.java`

**Interfaces:**
- Consumes: nothing new.
- Produces: `EntityRecordAdapter.toSequence(String, List<EntityRecord>)` and `EntityRecordAdapter.toEvent(EntityRecord)` — signatures unchanged, only internal Jackson types change. Nothing downstream needs to know.

- [ ] **Step 1: Confirm current behavior with a failing-if-broken baseline**

Run: `mvn -pl joxette-sol -am test`
Expected: BUILD SUCCESS (this is the pre-migration baseline for this module; no test changes needed since the public behavior of `EntityRecordAdapter` doesn't change).

- [ ] **Step 2: Update the dependency coordinate**

In `joxette-sol/pom.xml`, find:
```xml
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
```
Replace with:
```xml
        <dependency>
            <groupId>tools.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
```

- [ ] **Step 3: Update `EntityRecordAdapter.java`'s imports**

Change:
```java
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
```
to:
```java
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
```
No other line in the file changes — `new ObjectMapper()`, `new TypeReference<>() {}`, and `MAPPER.readValue(bytes, MAP_TYPE)` all keep the exact same signatures in Jackson 3.

- [ ] **Step 4: Compile and test**

Run: `mvn -pl joxette-sol -am test`
Expected: BUILD SUCCESS, all tests green (in particular, any test exercising `EntityRecordAdapter.toEvent` with a JSON `value` field).

- [ ] **Step 5: Commit**

```bash
git add joxette-sol/pom.xml joxette-sol/src/main/java/com/joxette/sol/EntityRecordAdapter.java
git commit -m "$(cat <<'EOF'
Migrate joxette-sol to Jackson 3

EntityRecordAdapter is the only Jackson user in this module. Swaps the
jackson-databind dependency coordinate from com.fasterxml.jackson.core to
tools.jackson.core and repoints the two imports; no behavior change
(ObjectMapper/TypeReference keep the same API surface for this usage).

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: `joxette-service` — introduce the Jackson 3 mapper and custom (de)serializers

This is the pivotal task: it adds the Jackson 3 dependency and stands up the new shared `JsonMapper` bean plus every custom (de)serializer that goes beyond a plain import rename, while **keeping the old Jackson 2 `ObjectMapper` bean alive** so every not-yet-migrated class in this module still resolves. Tasks 5 and 6 sweep the rest; Task 7 removes the old bean/dependency.

**Files:**
- Modify: `joxette-service/pom.xml` (add the Jackson 3 dependency; keep the Jackson 2 one for now)
- Modify: `joxette-service/src/main/java/com/joxette/config/OpenApiConfig.java`
- Delete: `joxette-service/src/main/java/com/joxette/config/JacksonConfig.java`
- Modify: `joxette-service/src/main/java/com/joxette/replay/transform/TransformStepJacksonModule.java`
- Modify: `joxette-service/src/main/java/com/joxette/replay/transform/TransformStepDeserializer.java`
- Modify: `joxette-service/src/main/java/com/joxette/replay/transform/GuardedStepSerializer.java`
- Modify: `joxette-service/src/main/java/com/joxette/replay/transform/GuardedStep.java`
- Modify: `joxette-service/src/main/java/com/joxette/replay/transform/gap/MessagePattern.java`

**Interfaces:**
- Consumes: nothing new from earlier tasks.
- Produces: a `@Primary` `JsonMapper` bean (method `OpenApiConfig.jsonMapper()`, type `tools.jackson.databind.json.JsonMapper`) that Task 5/6/7 classes will be autowired against once they're repointed to `tools.jackson.databind.ObjectMapper`. `TransformStepJacksonModule`, `TransformStepDeserializer`, `GuardedStepSerializer`, `GuardedStep`, and `MessagePattern.QuantifierDeserializer` all now operate purely on `tools.jackson.*` types — nothing outside this task's file list depends on their internals directly (they're wired in only via the module registration below).

- [ ] **Step 1: Baseline**

Run: `mvn -pl joxette-service -am test`
Expected: BUILD SUCCESS (confirms the module is green before this migration starts; it was already covered by the earlier Jox 0.6.0 bump's full-suite run, but re-confirm here since this is the point of no return for this module).

- [ ] **Step 2: Add the Jackson 3 dependency to `joxette-service/pom.xml`**

Find:
```xml
        <!-- Jackson for JSON handling in REST API -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
```
Replace with (keep both for now — the old one is removed in Task 7):
```xml
        <!-- Jackson 3 for JSON handling in REST API (migration in progress: see
             docs/superpowers/plans/2026-09-15-jackson3-migration.md) -->
        <dependency>
            <groupId>tools.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>

        <!-- Jackson 2, kept temporarily during the Jackson 3 migration for classes
             not yet repointed. Removed once every joxette-service source file
             uses tools.jackson.* instead. -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
```

- [ ] **Step 3: Rewrite `TransformStepJacksonModule.java`**

Full new content:
```java
package com.joxette.replay.transform;

import tools.jackson.databind.module.SimpleModule;

/**
 * Jackson {@link SimpleModule} that registers {@link TransformStepDeserializer}
 * for the {@link TransformStep} interface only.
 *
 * <p>Registering via a module (instead of {@code @JsonDeserialize} on the interface)
 * scopes the deserializer to the exact {@code TransformStep.class} target type.
 * Jackson does <em>not</em> propagate module-registered deserializers to subtypes, so
 * deserializing a concrete step class directly (e.g. {@code om.readValue(json, RedactStep.class)})
 * uses the normal Jackson mechanism rather than triggering {@link TransformStepDeserializer}.
 *
 * <p>Register this module on the {@code JsonMapper} builder in the application's
 * Jackson configuration:
 * <pre>{@code
 *   JsonMapper.builder()
 *       .addModule(new TransformStepJacksonModule())
 *       .build();
 * }</pre>
 */
public class TransformStepJacksonModule extends SimpleModule {

    public TransformStepJacksonModule() {
        super("TransformStepModule");
        addDeserializer(TransformStep.class, new TransformStepDeserializer());
    }
}
```

- [ ] **Step 4: Rewrite `TransformStepDeserializer.java`**

Full new content (only the imports, the `deserialize` signature, and the two `getCodec().treeToValue(...)` call sites change — the `TYPE_MAP` and class-resolution logic are untouched):
```java
package com.joxette.replay.transform;

import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.deser.std.StdDeserializer;
import tools.jackson.databind.node.ObjectNode;
import com.joxette.replay.transform.steps.AddComputedFieldStep;
import com.joxette.replay.transform.steps.AddHeaderStep;
import com.joxette.replay.transform.steps.CoalesceStep;
import com.joxette.replay.transform.steps.ConditionalStep;
import com.joxette.replay.transform.steps.CopyFieldStep;
import com.joxette.replay.transform.steps.CopyToHeaderStep;
import com.joxette.replay.transform.steps.DeleteFieldStep;
import com.joxette.replay.transform.steps.FanOutStep;
import com.joxette.replay.transform.steps.FilterDropStep;
import com.joxette.replay.transform.steps.FlattenFieldStep;
import com.joxette.replay.transform.steps.KeyFromValueStep;
import com.joxette.replay.transform.steps.MaskHashStep;
import com.joxette.replay.transform.steps.MergePatchStep;
import com.joxette.replay.transform.steps.NullKeyStep;
import com.joxette.replay.transform.steps.RedactStep;
import com.joxette.replay.transform.steps.RedirectTopicStep;
import com.joxette.replay.transform.steps.RemapKeyStep;
import com.joxette.replay.transform.steps.RemoveHeaderStep;
import com.joxette.replay.transform.steps.RenameFieldStep;
import com.joxette.replay.transform.steps.SetConstantStep;
import com.joxette.replay.transform.steps.TemplateStep;
import com.joxette.replay.transform.steps.TimeCompressStep;
import com.joxette.replay.transform.steps.TimeFreezeStep;
import com.joxette.replay.transform.steps.TimeShiftStep;
import com.joxette.replay.transform.steps.WallTimeStep;

import java.util.Map;

/**
 * Custom Jackson deserializer for {@link TransformStep}.
 *
 * <p>Provides two capabilities above what {@code @JsonTypeInfo}/{@code @JsonSubTypes}
 * alone could offer:
 *
 * <ol>
 *   <li><b>Per-step {@code when} guard</b> — extracts the optional {@code "when"}
 *       field from any step JSON object and wraps the deserialized step in a
 *       {@link GuardedStep} carrying that {@link Predicate}.</li>
 *   <li><b>Type resolution</b> — maps the {@code "type"} discriminator string to
 *       the concrete step class, replicating the {@code @JsonSubTypes} mapping which
 *       still governs serialization (adding the {@code "type"} property on the way
 *       out).</li>
 * </ol>
 *
 * <p>Registration: applied via {@code @JsonDeserialize(using = TransformStepDeserializer.class)}
 * on the {@link TransformStep} interface. The {@code @JsonTypeInfo} and
 * {@code @JsonSubTypes} annotations on that interface are kept for serialization only.
 *
 * <h2>Recursion</h2>
 * <p>When deserializing a {@link ConditionalStep}, its nested {@code then_steps} and
 * {@code else_steps} lists are typed as {@code List<TransformStep>}. Jackson uses this
 * deserializer for each element, so nested steps inside conditionals also support
 * {@code when} guards.
 */
public class TransformStepDeserializer extends StdDeserializer<TransformStep> {

    /** Maps the {@code "type"} discriminator string to the concrete step class. */
    private static final Map<String, Class<? extends TransformStep>> TYPE_MAP = Map.ofEntries(
            Map.entry("set_constant",       SetConstantStep.class),
            Map.entry("copy_field",         CopyFieldStep.class),
            Map.entry("template",           TemplateStep.class),
            Map.entry("redact",             RedactStep.class),
            Map.entry("mask_hash",          MaskHashStep.class),
            Map.entry("coalesce",           CoalesceStep.class),
            Map.entry("wall_time",          WallTimeStep.class),
            Map.entry("time_shift",         TimeShiftStep.class),
            Map.entry("time_compress",      TimeCompressStep.class),
            Map.entry("time_freeze",        TimeFreezeStep.class),
            Map.entry("rename_field",       RenameFieldStep.class),
            Map.entry("delete_field",       DeleteFieldStep.class),
            Map.entry("flatten_field",      FlattenFieldStep.class),
            Map.entry("add_computed_field", AddComputedFieldStep.class),
            Map.entry("merge_patch",        MergePatchStep.class),
            Map.entry("remap_key",          RemapKeyStep.class),
            Map.entry("null_key",           NullKeyStep.class),
            Map.entry("key_from_value",     KeyFromValueStep.class),
            Map.entry("add_header",         AddHeaderStep.class),
            Map.entry("remove_header",      RemoveHeaderStep.class),
            Map.entry("copy_to_header",     CopyToHeaderStep.class),
            Map.entry("redirect_topic",     RedirectTopicStep.class),
            Map.entry("fan_out",            FanOutStep.class),
            Map.entry("filter_drop",        FilterDropStep.class),
            Map.entry("conditional",        ConditionalStep.class)
    );

    public TransformStepDeserializer() {
        super(TransformStep.class);
    }

    @Override
    public TransformStep deserialize(JsonParser p, DeserializationContext ctxt) {
        ObjectNode node = p.readValueAsTree();

        // Peek at the 'when' guard before delegating to the concrete type.
        // We remove it from the node so concrete step classes don't see an unknown property
        // (this avoids FAIL_ON_UNKNOWN_PROPERTIES errors on strictly-configured mappers).
        JsonNode whenNode = node.remove("when");

        // Resolve the concrete step class from the 'type' discriminator
        JsonNode typeNode = node.get("type");
        if (typeNode == null || typeNode.isNull()) {
            throw ctxt.instantiationException(TransformStep.class,
                    "Missing required 'type' discriminator field in transform step");
        }
        String type = typeNode.asText();
        Class<? extends TransformStep> concreteClass = TYPE_MAP.get(type);
        if (concreteClass == null) {
            throw ctxt.instantiationException(TransformStep.class,
                    "Unknown transform step type: '" + type + "'");
        }

        // Deserialize the concrete step. The 'type' field remains in the node but concrete
        // classes don't declare a 'type' component/property, so it is silently ignored.
        TransformStep step = ctxt.readTreeAsValue(node, concreteClass);

        // Wrap in GuardedStep when a 'when' predicate was present
        if (whenNode != null && !whenNode.isNull() && !whenNode.isMissingNode()) {
            Predicate guard = ctxt.readTreeAsValue(whenNode, Predicate.class);
            return new GuardedStep(guard, step);
        }
        return step;
    }
}
```
Note what's gone: `import com.fasterxml.jackson.core.JsonParser;`/`DeserializationContext`/`JsonNode`/`StdDeserializer`/`ObjectNode` (all repointed to `tools.jackson.*`), `import java.io.IOException;` (no longer needed — the method no longer declares `throws IOException`, since `ValueDeserializer.deserialize` in Jackson 3 declares `throws JacksonException`, unchecked, and an override cannot add a checked exception the parent doesn't declare), and both `p.getCodec().treeToValue(...)` call sites replaced with `ctxt.readTreeAsValue(...)`.

- [ ] **Step 5: Rewrite `GuardedStepSerializer.java`**

Full new content:
```java
package com.joxette.replay.transform;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.ser.std.StdSerializer;

/**
 * Jackson serializer for {@link GuardedStep}.
 *
 * <p>Serializes a {@code GuardedStep} by first serializing its delegate step (which
 * picks up the delegate's own {@code @JsonTypeInfo} → adds the {@code "type"} field),
 * then injecting the {@code "when"} predicate into the resulting JSON object.
 *
 * <p>Round-trip example — input:
 * <pre>{@code
 * { "type": "redact", "target": "$.value.email",
 *   "when": { "field": "$.headers[x-env]", "operator": "NEQ", "value": "prod" } }
 * }</pre>
 *
 * <p>Output after deserialization → re-serialization:
 * <pre>{@code
 * { "type": "redact", "target": "$.value.email",
 *   "when": { "match": "leaf", "field": "$.headers[x-env]", "operator": "NEQ", "value": "prod" } }
 * }</pre>
 * (Note: {@code "match": "leaf"} is added by {@link Predicate}'s {@code @JsonTypeInfo} on
 * the way out; it is optional on input due to {@code defaultImpl}.)
 */
public class GuardedStepSerializer extends StdSerializer<GuardedStep> {

    public GuardedStepSerializer() {
        super(GuardedStep.class);
    }

    @Override
    public void serialize(GuardedStep value, JsonGenerator gen, SerializationContext provider) {
        // Serialize the delegate step — picks up @JsonTypeInfo and adds "type" field
        ObjectNode node = (ObjectNode) provider.valueToTree(value.delegate());
        // Inject the 'when' predicate into the same object
        node.set("when", provider.valueToTree(value.when()));
        provider.writeTree(gen, node);
    }
}
```
Note what changed: `SerializerProvider` → `SerializationContext` (Jackson 3's rename); `gen.getCodec()` cast to `ObjectMapper` is gone — `SerializationContext.valueToTree(Object)`/`writeTree(JsonGenerator, TreeNode)` do the same job directly, no codec cast needed; `import java.io.IOException;` and the `throws IOException` clause are dropped for the same reason as `TransformStepDeserializer`.

- [ ] **Step 6: Update `GuardedStep.java`'s import**

Change:
```java
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
```
to:
```java
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import tools.jackson.databind.annotation.JsonSerialize;
```
(`JsonTypeInfo` stays under `com.fasterxml.jackson.annotation` — it's an annotations-only type, unchanged in Jackson 3. Only `JsonSerialize`, which lives under `databind.annotation`, moves.) Nothing else in the file changes.

- [ ] **Step 7: Fix `MessagePattern.java`'s `QuantifierDeserializer`**

In `joxette-service/src/main/java/com/joxette/replay/transform/gap/MessagePattern.java`, update the imports at the top of the file: repoint every `com.fasterxml.jackson.{core,databind}.*` import to `tools.jackson.{core,databind}.*` (leave any `com.fasterxml.jackson.annotation.*` import, e.g. `@JsonProperty`, as-is).

Then replace the `QuantifierDeserializer` inner class body (currently at approximately lines 117–146) with:
```java
    static final class QuantifierDeserializer extends StdDeserializer<Quantifier> {

        QuantifierDeserializer() {
            super(Quantifier.class);
        }

        @Override
        public Quantifier deserialize(JsonParser p, DeserializationContext ctx) {
            JsonNode node = p.readValueAsTree();
            if (node.isTextual()) {
                return switch (node.textValue()) {
                    case "first" -> Quantifier.First.INSTANCE;
                    case "last"  -> Quantifier.Last.INSTANCE;
                    case "any"   -> Quantifier.Any.INSTANCE;
                    default -> ctx.reportInputMismatch(Quantifier.class,
                            "Unknown quantifier string: %s", node.textValue());
                };
            }
            if (node.isObject()) {
                if (node.has("nth")) {
                    return new Quantifier.Nth(node.get("nth").intValue());
                }
                if (node.has("first_after")) {
                    MessagePattern after = ctx.readTreeAsValue(node.get("first_after"), MessagePattern.class);
                    return new Quantifier.FirstAfter(after);
                }
                return ctx.reportInputMismatch(Quantifier.class,
                        "Unknown quantifier object shape (expected 'nth' or 'first_after'): %s", node);
            }
            return ctx.reportInputMismatch(Quantifier.class,
                    "Expected string or object for Quantifier, got: %s", node.getNodeType());
        }
    }
```
This replaces `p.getCodec().readTree(p)` with `p.readValueAsTree()`, replaces `p.getCodec().treeToValue(...)` with `ctx.readTreeAsValue(...)`, and replaces the three `throw new IOException(...)` sites with `ctx.reportInputMismatch(...)` — `IOException` is checked and the overridden `deserialize` method can no longer declare `throws IOException` (Jackson 3's `ValueDeserializer.deserialize` declares `throws JacksonException`, unchecked, and Java forbids an override from adding a broader checked exception). `reportInputMismatch` is Jackson 3 databind's own convention for this exact situation — it throws an unchecked `DatabindException` carrying parser location context, which is strictly better diagnostics than the original bare `IOException` message. Confirmed no test asserts on the exact exception type/message for invalid quantifier input (`GapModelSerializationTest` only covers valid round-trips and serialization shape), so this is safe.

- [ ] **Step 8: Rewrite `OpenApiConfig.java`**

Full new content:
```java
package com.joxette.config;

import com.joxette.replay.transform.TransformStepJacksonModule;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI joxetteOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Joxette API")
                        .version("0.1.0")
                        .description("Kafka topic cassette recorder backed by DuckLake"));
    }

    /**
     * Shared Jackson 3 mapper for the whole application. Java-time (de)serialization
     * is built into jackson-databind in Jackson 3 (no JavaTimeModule needed);
     * {@code WRITE_DATES_AS_TIMESTAMPS} is disabled so dates render as ISO-8601
     * strings, matching the documented Replay Message Format.
     */
    @Bean
    @Primary
    public JsonMapper jsonMapper() {
        return JsonMapper.builder()
                .disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
                .addModule(new TransformStepJacksonModule())
                .build();
    }
}
```
(The old `objectMapper()` bean and its `com.fasterxml.jackson.databind.ObjectMapper`/`SerializationFeature`/`JavaTimeModule` imports are gone. The old Jackson 2 dependency stays on the classpath per Step 2 above, so this compiles fine even though nothing produces a Jackson 2 `ObjectMapper` bean anymore as of this task — Tasks 5–7 finish repointing every consumer.)

- [ ] **Step 9: Delete `JacksonConfig.java`**

```bash
git rm joxette-service/src/main/java/com/joxette/config/JacksonConfig.java
```
Its only responsibility (registering `TransformStepJacksonModule`) now happens directly on the `JsonMapper` builder in `OpenApiConfig.jsonMapper()` — no more relying on Spring's Jackson auto-configuration to pick up a `Module` bean.

- [ ] **Step 10: Compile**

Run: `mvn -pl joxette-service -am compile`
Expected: **This will fail** — every remaining class in `joxette-service` that autowires a `com.fasterxml.jackson.databind.ObjectMapper` bean (Jackson 2 type) now has no matching bean, since `OpenApiConfig`'s old bean is gone and the new `JsonMapper` bean is a different, incompatible type. That's expected and is what Tasks 5–6 fix — this step is here to confirm the *compiler* still succeeds (the code compiles fine; it's a Spring *context-startup* failure, not a compile error, so `mvn compile` alone should actually succeed). Confirm: `mvn -pl joxette-service -am compile` → BUILD SUCCESS. Do **not** run `mvn test` yet — the Spring context will fail to start until Task 5/6 finish repointing every `ObjectMapper` consumer. That's expected; proceed to commit this task's isolated, compilable state.

- [ ] **Step 11: Commit**

```bash
git add joxette-service/pom.xml \
        joxette-service/src/main/java/com/joxette/config/OpenApiConfig.java \
        joxette-service/src/main/java/com/joxette/replay/transform/TransformStepJacksonModule.java \
        joxette-service/src/main/java/com/joxette/replay/transform/TransformStepDeserializer.java \
        joxette-service/src/main/java/com/joxette/replay/transform/GuardedStepSerializer.java \
        joxette-service/src/main/java/com/joxette/replay/transform/GuardedStep.java \
        joxette-service/src/main/java/com/joxette/replay/transform/gap/MessagePattern.java
git rm joxette-service/src/main/java/com/joxette/config/JacksonConfig.java 2>/dev/null || true
git commit -m "$(cat <<'EOF'
Introduce Jackson 3 JsonMapper bean and migrate custom (de)serializers

Adds tools.jackson.core:jackson-databind alongside the existing Jackson 2
dependency (removed once every consumer is repointed, in a later commit).
Replaces OpenApiConfig's Jackson 2 ObjectMapper bean with a @Primary
JsonMapper bean built via the Jackson 3 builder API, and rewrites
TransformStepJacksonModule/TransformStepDeserializer/GuardedStepSerializer/
GuardedStep/MessagePattern's QuantifierDeserializer against tools.jackson.*
types (SerializerProvider -> SerializationContext, JsonGenerator/JsonParser
getCodec() removed in favor of SerializationContext.valueToTree/writeTree
and DeserializationContext.readTreeAsValue, checked IOException replaced by
unchecked JacksonException throughout). JacksonConfig.java is deleted --
its module registration now happens directly on the JsonMapper builder.

Not yet buildable end-to-end: classes still using the old Jackson 2
ObjectMapper type are repointed in the next two commits.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: `joxette-service` — sweep remaining main sources

Repoints every remaining main-source file in `joxette-service` from `com.fasterxml.jackson.{core,databind}.*` to `tools.jackson.{core,databind}.*`. After this task, every main-source class compiles against `tools.jackson.databind.ObjectMapper`, which the `JsonMapper` bean from Task 4 satisfies — so the Spring context should start again (test sources still reference the old type until Task 6, so full-suite tests still can't run yet, but a plain context-load smoke check becomes possible).

**Files (27 files, exact list):**
```
joxette-service/src/main/java/com/joxette/reconciliation/ReconciliationService.java
joxette-service/src/main/java/com/joxette/cluster/InstanceController.java
joxette-service/src/main/java/com/joxette/cluster/InstanceRegistry.java
joxette-service/src/main/java/com/joxette/config/SecurityConfig.java
joxette-service/src/main/java/com/joxette/streams/StreamDefinitionRepository.java
joxette-service/src/main/java/com/joxette/exports/ExportService.java
joxette-service/src/main/java/com/joxette/replay/PortraitService.java
joxette-service/src/main/java/com/joxette/replay/DiffService.java
joxette-service/src/main/java/com/joxette/replay/SseReplayHandler.java
joxette-service/src/main/java/com/joxette/replay/CassetteLifecycleService.java
joxette-service/src/main/java/com/joxette/replay/CassetteController.java
joxette-service/src/main/java/com/joxette/replay/EntityCursor.java
joxette-service/src/main/java/com/joxette/replay/DiffRecord.java
joxette-service/src/main/java/com/joxette/replay/TopicCursor.java
joxette-service/src/main/java/com/joxette/replay/StateFoldService.java
joxette-service/src/main/java/com/joxette/replay/transform/MessageJsonPath.java
joxette-service/src/main/java/com/joxette/replay/transform/TransformPresetRepository.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/JsonStepHelper.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/KeyFromValueStep.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/DeleteFieldStep.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/CoalesceStep.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/AddComputedFieldStep.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/RenameFieldStep.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/RemapKeyStep.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/MergePatchStep.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/SetConstantStep.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/FlattenFieldStep.java
```

Of these, 8 files have a `catch (JsonProcessingException e)` block that needs a distinct fix (Jackson 3 has no `JsonProcessingException` class at all — `writeValueAsString`/`readValue`/`writeValueAsBytes` now throw the unchecked `tools.jackson.core.JacksonException`):
```
joxette-service/src/main/java/com/joxette/cluster/InstanceController.java
joxette-service/src/main/java/com/joxette/cluster/InstanceRegistry.java
joxette-service/src/main/java/com/joxette/streams/StreamDefinitionRepository.java
joxette-service/src/main/java/com/joxette/replay/SseReplayHandler.java
joxette-service/src/main/java/com/joxette/replay/CassetteLifecycleService.java
joxette-service/src/main/java/com/joxette/replay/CassetteController.java
joxette-service/src/main/java/com/joxette/replay/transform/TransformPresetRepository.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/JsonStepHelper.java
```

**Interfaces:**
- Consumes: `tools.jackson.databind.json.JsonMapper` bean from Task 4 (satisfies every `@Autowired`/constructor-injected `tools.jackson.databind.ObjectMapper` parameter after this task).
- Produces: nothing new — every class here keeps its exact public signature; only internal Jackson types change.

- [ ] **Step 1: Fix the 8 `JsonProcessingException` files first**

Run this exact command (macOS/BSD `sed`, in-place, no backup):
```bash
cd /Users/schrepfler/repos/joxette
FILES="joxette-service/src/main/java/com/joxette/cluster/InstanceController.java \
joxette-service/src/main/java/com/joxette/cluster/InstanceRegistry.java \
joxette-service/src/main/java/com/joxette/streams/StreamDefinitionRepository.java \
joxette-service/src/main/java/com/joxette/replay/SseReplayHandler.java \
joxette-service/src/main/java/com/joxette/replay/CassetteLifecycleService.java \
joxette-service/src/main/java/com/joxette/replay/CassetteController.java \
joxette-service/src/main/java/com/joxette/replay/transform/TransformPresetRepository.java \
joxette-service/src/main/java/com/joxette/replay/transform/steps/JsonStepHelper.java"

for f in $FILES; do
  sed -i '' \
    -e 's/import com\.fasterxml\.jackson\.core\.JsonProcessingException;/import tools.jackson.core.JacksonException;/' \
    -e 's/catch (JsonProcessingException e)/catch (JacksonException e)/' \
    "$f"
done
```
Verify no `JsonProcessingException` remains in these files:
```bash
grep -l "JsonProcessingException" $FILES
```
Expected: no output.

- [ ] **Step 2: Mechanical import rename across all 27 files**

Run:
```bash
cd /Users/schrepfler/repos/joxette
FILES="joxette-service/src/main/java/com/joxette/reconciliation/ReconciliationService.java
joxette-service/src/main/java/com/joxette/cluster/InstanceController.java
joxette-service/src/main/java/com/joxette/cluster/InstanceRegistry.java
joxette-service/src/main/java/com/joxette/config/SecurityConfig.java
joxette-service/src/main/java/com/joxette/streams/StreamDefinitionRepository.java
joxette-service/src/main/java/com/joxette/exports/ExportService.java
joxette-service/src/main/java/com/joxette/replay/PortraitService.java
joxette-service/src/main/java/com/joxette/replay/DiffService.java
joxette-service/src/main/java/com/joxette/replay/SseReplayHandler.java
joxette-service/src/main/java/com/joxette/replay/CassetteLifecycleService.java
joxette-service/src/main/java/com/joxette/replay/CassetteController.java
joxette-service/src/main/java/com/joxette/replay/EntityCursor.java
joxette-service/src/main/java/com/joxette/replay/DiffRecord.java
joxette-service/src/main/java/com/joxette/replay/TopicCursor.java
joxette-service/src/main/java/com/joxette/replay/StateFoldService.java
joxette-service/src/main/java/com/joxette/replay/transform/MessageJsonPath.java
joxette-service/src/main/java/com/joxette/replay/transform/TransformPresetRepository.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/JsonStepHelper.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/KeyFromValueStep.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/DeleteFieldStep.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/CoalesceStep.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/AddComputedFieldStep.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/RenameFieldStep.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/RemapKeyStep.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/MergePatchStep.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/SetConstantStep.java
joxette-service/src/main/java/com/joxette/replay/transform/steps/FlattenFieldStep.java"

for f in $FILES; do
  sed -i '' \
    -e 's/^import com\.fasterxml\.jackson\.core\./import tools.jackson.core./' \
    -e 's/^import com\.fasterxml\.jackson\.databind\./import tools.jackson.databind./' \
    "$f"
done
```
This leaves any `com.fasterxml.jackson.annotation.*` import in these files untouched (correct — that package is unchanged in Jackson 3).

- [ ] **Step 3: Fix the `.registerModule(new JavaTimeModule())` pattern — none expected in main, verify**

Run:
```bash
grep -rn "registerModule" joxette-service/src/main
```
Expected: no output (the only main-source `.registerModule` call was in the now-deleted `OpenApiConfig.objectMapper()` bean, replaced in Task 4). If this prints anything, stop and handle it the same way as Task 4 Step 8 — either drop the call (if it's `JavaTimeModule`, now built-in) or convert to `JsonMapper.builder().addModule(...).build()` (if it's a real custom module).

- [ ] **Step 4: Compile**

Run: `mvn -pl joxette-service -am compile`
Expected: BUILD SUCCESS. If it fails, the error will name the file and line — given the verification already done during planning (no `getCodec()`, `SerializerProvider`, or direct `ValueDeserializer`/`ValueSerializer` implementations exist outside the files already handled in Task 4), the only remaining failure classes should be:
  - A missed `catch (JsonProcessingException e)` — apply the Step 1 fix to that file.
  - A missed `.registerModule(...)` call — apply the Step 3 fix.
  If you see anything else, stop and investigate before continuing (it means this plan's verification missed something).

- [ ] **Step 5: Confirm the Spring context now starts (test sources aren't migrated yet, so run only a context-load-shaped check)**

Test sources still reference `com.fasterxml.jackson.databind.ObjectMapper`, so the full test suite will fail at Spring context startup in test code that autowires the old type (this is expected and fixed in Task 6). Skip `mvn test` for this task; the compile success in Step 4 is the checkpoint. Full-suite verification happens at the end of Task 6.

- [ ] **Step 6: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/reconciliation/ReconciliationService.java \
        joxette-service/src/main/java/com/joxette/cluster/InstanceController.java \
        joxette-service/src/main/java/com/joxette/cluster/InstanceRegistry.java \
        joxette-service/src/main/java/com/joxette/config/SecurityConfig.java \
        joxette-service/src/main/java/com/joxette/streams/StreamDefinitionRepository.java \
        joxette-service/src/main/java/com/joxette/exports/ExportService.java \
        joxette-service/src/main/java/com/joxette/replay/PortraitService.java \
        joxette-service/src/main/java/com/joxette/replay/DiffService.java \
        joxette-service/src/main/java/com/joxette/replay/SseReplayHandler.java \
        joxette-service/src/main/java/com/joxette/replay/CassetteLifecycleService.java \
        joxette-service/src/main/java/com/joxette/replay/CassetteController.java \
        joxette-service/src/main/java/com/joxette/replay/EntityCursor.java \
        joxette-service/src/main/java/com/joxette/replay/DiffRecord.java \
        joxette-service/src/main/java/com/joxette/replay/TopicCursor.java \
        joxette-service/src/main/java/com/joxette/replay/StateFoldService.java \
        joxette-service/src/main/java/com/joxette/replay/transform/MessageJsonPath.java \
        joxette-service/src/main/java/com/joxette/replay/transform/TransformPresetRepository.java \
        joxette-service/src/main/java/com/joxette/replay/transform/steps/JsonStepHelper.java \
        joxette-service/src/main/java/com/joxette/replay/transform/steps/KeyFromValueStep.java \
        joxette-service/src/main/java/com/joxette/replay/transform/steps/DeleteFieldStep.java \
        joxette-service/src/main/java/com/joxette/replay/transform/steps/CoalesceStep.java \
        joxette-service/src/main/java/com/joxette/replay/transform/steps/AddComputedFieldStep.java \
        joxette-service/src/main/java/com/joxette/replay/transform/steps/RenameFieldStep.java \
        joxette-service/src/main/java/com/joxette/replay/transform/steps/RemapKeyStep.java \
        joxette-service/src/main/java/com/joxette/replay/transform/steps/MergePatchStep.java \
        joxette-service/src/main/java/com/joxette/replay/transform/steps/SetConstantStep.java \
        joxette-service/src/main/java/com/joxette/replay/transform/steps/FlattenFieldStep.java
git commit -m "$(cat <<'EOF'
Repoint remaining joxette-service main sources to Jackson 3

Mechanical import rename (com.fasterxml.jackson.{core,databind} ->
tools.jackson.{core,databind}) across the remaining 27 main-source files
that reference Jackson databind/core types. Eight of them also swap
catch (JsonProcessingException e) for catch (JacksonException e), since
Jackson 3 has no JsonProcessingException class -- writeValueAsString/
readValue now throw the unchecked tools.jackson.core.JacksonException.

No behavior change. Every joxette-service main source now compiles
against the JsonMapper bean introduced in the previous commit; test
sources are repointed next.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: `joxette-service` — sweep remaining test sources

**Files (32 files, exact list):**
```
joxette-service/src/test/java/com/joxette/reconciliation/ReconciliationServiceWatchdogTest.java
joxette-service/src/test/java/com/joxette/reconciliation/ReconciliationServiceLifecycleTest.java
joxette-service/src/test/java/com/joxette/streams/StreamDefinitionRepositoryTest.java
joxette-service/src/test/java/com/joxette/it/SpringDocIT.java
joxette-service/src/test/java/com/joxette/it/TimestampSerializationIT.java
joxette-service/src/test/java/com/joxette/it/ProblemDetailContractIT.java
joxette-service/src/test/java/com/joxette/it/ApiKeyAuthenticationIT.java
joxette-service/src/test/java/com/joxette/exports/ExportControllerTest.java
joxette-service/src/test/java/com/joxette/api/error/EntityControllerProblemDetailTest.java
joxette-service/src/test/java/com/joxette/api/error/CompactionControllerProblemDetailTest.java
joxette-service/src/test/java/com/joxette/api/error/TopicControllerProblemDetailTest.java
joxette-service/src/test/java/com/joxette/replay/DiffServiceTest.java
joxette-service/src/test/java/com/joxette/api/error/CassetteControllerProblemDetailTest.java
joxette-service/src/test/java/com/joxette/replay/CassetteLifecycleServiceRestoreResilienceTest.java
joxette-service/src/test/java/com/joxette/replay/TopicReplayServiceTransformTest.java
joxette-service/src/test/java/com/joxette/replay/BatchReplayTest.java
joxette-service/src/test/java/com/joxette/replay/StateFoldServiceTest.java
joxette-service/src/test/java/com/joxette/replay/CassetteControllerLastNExclusivityTest.java
joxette-service/src/test/java/com/joxette/replay/SseReplayHandlerLifecycleTest.java
joxette-service/src/test/java/com/joxette/replay/PortraitServiceTest.java
joxette-service/src/test/java/com/joxette/replay/SseReplayHandlerErrorTest.java
joxette-service/src/test/java/com/joxette/replay/transform/TransformPipelineIntegrationTest.java
joxette-service/src/test/java/com/joxette/replay/CassetteControllerTransformValidationTest.java
joxette-service/src/test/java/com/joxette/replay/transform/steps/CoalesceStepTest.java
joxette-service/src/test/java/com/joxette/replay/transform/gap/GapModelSerializationTest.java
joxette-service/src/test/java/com/joxette/replay/transform/steps/CopyFieldStepTest.java
joxette-service/src/test/java/com/joxette/replay/transform/steps/RedactStepTest.java
joxette-service/src/test/java/com/joxette/replay/transform/TransformPresetRepositoryConcurrencyTest.java
joxette-service/src/test/java/com/joxette/replay/transform/steps/MaskHashStepTest.java
joxette-service/src/test/java/com/joxette/replay/transform/steps/TemplateStepTest.java
joxette-service/src/test/java/com/joxette/replay/transform/steps/StructuralKeyStepTest.java
joxette-service/src/test/java/com/joxette/replay/transform/steps/SetConstantStepTest.java
```

Three of these (`StreamDefinitionRepositoryTest.java`, `ExportControllerTest.java`, `BatchReplayTest.java`) call `.registerModule(new JavaTimeModule())` on a locally-constructed `ObjectMapper` — since java-time support is now built in, this call is simply dropped. One (`GapModelSerializationTest.java`) calls `.registerModule(new TransformStepJacksonModule())` — since that module is essential, it must switch to the builder API.

**Interfaces:**
- Consumes: `tools.jackson.databind.json.JsonMapper` bean from Task 4; `tools.jackson.databind.module.SimpleModule`-based `TransformStepJacksonModule` from Task 4.
- Produces: nothing new.

- [ ] **Step 1: Fix the three `JavaTimeModule`-drop files**

In `joxette-service/src/test/java/com/joxette/streams/StreamDefinitionRepositoryTest.java` (around line 43-44), change:
```java
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule());
```
to:
```java
        ObjectMapper mapper = new ObjectMapper();
```
and remove its `import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;` line (java-time support no longer needs a module).

In `joxette-service/src/test/java/com/joxette/exports/ExportControllerTest.java` (around line 41-42), change:
```java
        mapper = new ObjectMapper();
        mapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
```
to:
```java
        mapper = new ObjectMapper();
```

In `joxette-service/src/test/java/com/joxette/replay/BatchReplayTest.java` (around line 84-85), change:
```java
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
```
to:
```java
        objectMapper = new ObjectMapper();
```

- [ ] **Step 2: Fix `GapModelSerializationTest.java`'s module registration**

Change:
```java
    private static final ObjectMapper OM = new ObjectMapper()
            .registerModule(new TransformStepJacksonModule());
```
to:
```java
    private static final ObjectMapper OM = JsonMapper.builder()
            .addModule(new TransformStepJacksonModule())
            .build();
```
and add the import `import tools.jackson.databind.json.JsonMapper;` (the existing `import com.joxette.replay.transform.TransformStepJacksonModule;` stays as-is — that class doesn't move).

- [ ] **Step 3: Mechanical import rename across all 32 files**

Run:
```bash
cd /Users/schrepfler/repos/joxette
FILES="joxette-service/src/test/java/com/joxette/reconciliation/ReconciliationServiceWatchdogTest.java
joxette-service/src/test/java/com/joxette/reconciliation/ReconciliationServiceLifecycleTest.java
joxette-service/src/test/java/com/joxette/streams/StreamDefinitionRepositoryTest.java
joxette-service/src/test/java/com/joxette/it/SpringDocIT.java
joxette-service/src/test/java/com/joxette/it/TimestampSerializationIT.java
joxette-service/src/test/java/com/joxette/it/ProblemDetailContractIT.java
joxette-service/src/test/java/com/joxette/it/ApiKeyAuthenticationIT.java
joxette-service/src/test/java/com/joxette/exports/ExportControllerTest.java
joxette-service/src/test/java/com/joxette/api/error/EntityControllerProblemDetailTest.java
joxette-service/src/test/java/com/joxette/api/error/CompactionControllerProblemDetailTest.java
joxette-service/src/test/java/com/joxette/api/error/TopicControllerProblemDetailTest.java
joxette-service/src/test/java/com/joxette/replay/DiffServiceTest.java
joxette-service/src/test/java/com/joxette/api/error/CassetteControllerProblemDetailTest.java
joxette-service/src/test/java/com/joxette/replay/CassetteLifecycleServiceRestoreResilienceTest.java
joxette-service/src/test/java/com/joxette/replay/TopicReplayServiceTransformTest.java
joxette-service/src/test/java/com/joxette/replay/BatchReplayTest.java
joxette-service/src/test/java/com/joxette/replay/StateFoldServiceTest.java
joxette-service/src/test/java/com/joxette/replay/CassetteControllerLastNExclusivityTest.java
joxette-service/src/test/java/com/joxette/replay/SseReplayHandlerLifecycleTest.java
joxette-service/src/test/java/com/joxette/replay/PortraitServiceTest.java
joxette-service/src/test/java/com/joxette/replay/SseReplayHandlerErrorTest.java
joxette-service/src/test/java/com/joxette/replay/transform/TransformPipelineIntegrationTest.java
joxette-service/src/test/java/com/joxette/replay/CassetteControllerTransformValidationTest.java
joxette-service/src/test/java/com/joxette/replay/transform/steps/CoalesceStepTest.java
joxette-service/src/test/java/com/joxette/replay/transform/gap/GapModelSerializationTest.java
joxette-service/src/test/java/com/joxette/replay/transform/steps/CopyFieldStepTest.java
joxette-service/src/test/java/com/joxette/replay/transform/steps/RedactStepTest.java
joxette-service/src/test/java/com/joxette/replay/transform/TransformPresetRepositoryConcurrencyTest.java
joxette-service/src/test/java/com/joxette/replay/transform/steps/MaskHashStepTest.java
joxette-service/src/test/java/com/joxette/replay/transform/steps/TemplateStepTest.java
joxette-service/src/test/java/com/joxette/replay/transform/steps/StructuralKeyStepTest.java
joxette-service/src/test/java/com/joxette/replay/transform/steps/SetConstantStepTest.java"

for f in $FILES; do
  sed -i '' \
    -e 's/^import com\.fasterxml\.jackson\.core\./import tools.jackson.core./' \
    -e 's/^import com\.fasterxml\.jackson\.databind\./import tools.jackson.databind./' \
    "$f"
done
```

- [ ] **Step 4: Verify no stray `JavaTimeModule`/`registerModule`/`JsonProcessingException` remain**

Run:
```bash
grep -rln "JavaTimeModule\|registerModule\|JsonProcessingException" joxette-service/src/test
```
Expected: no output (Steps 1–2 handled the only four sites found during planning).

- [ ] **Step 5: Compile and run the full `joxette-service` test suite**

Run: `mvn -pl joxette-service -am test`
Expected: BUILD SUCCESS, all tests green. Pay particular attention to (these are the tests that directly exercise the riskiest behavioral surface identified during planning):
  - `TimestampSerializationIT` — asserts ISO-8601-with-timezone date rendering and exact round-trip across JSON/SSE/NDJSON, for both topic and entity replay. This is the direct verification that `DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS` on the new `JsonMapper` reproduces the old `SerializationFeature.WRITE_DATES_AS_TIMESTAMPS=false` behavior.
  - `ProblemDetailContractIT`, `EntityControllerProblemDetailTest`, `CompactionControllerProblemDetailTest`, `TopicControllerProblemDetailTest`, `CassetteControllerProblemDetailTest`, `ApiKeyAuthenticationIT` — assert the flattened RFC 7807 `application/problem+json` shape end-to-end. This is the direct verification that Spring's `ProblemDetail` handling still flattens correctly once Spring MVC's registered converter is Jackson 3's, not `MappingJackson2HttpMessageConverter`.
  - `SpringDocIT` — confirms OpenAPI spec generation (springdoc's own internal Jackson 2 mapper) is unaffected by the application's mapper swap.
  - `GapModelSerializationTest`, `TransformPipelineIntegrationTest`, and the `transform/steps/*Test` files — confirm the rewritten `TransformStepDeserializer`/`GuardedStepSerializer`/`MessagePattern.QuantifierDeserializer` round-trip correctly.

If anything fails, do not proceed to Task 7 — fix it here, since Task 7 removes the fallback (Jackson 2 bean/dependency) this task still has available.

- [ ] **Step 6: Commit**

```bash
git add joxette-service/src/test
git commit -m "$(cat <<'EOF'
Repoint remaining joxette-service test sources to Jackson 3

Mechanical import rename across the remaining 32 test files. Three drop
a now-unnecessary .registerModule(new JavaTimeModule()) call (java-time
support is built into Jackson 3's databind); GapModelSerializationTest
switches its TransformStepJacksonModule registration to the builder API,
since Jackson 3 has no mutable post-construction module registration.

Full joxette-service test suite is green, including TimestampSerializationIT
(date/timezone wire format) and the ProblemDetail contract tests (RFC 7807
flattening under Spring MVC's Jackson 3 message converter) -- the two
behavioral risks identified during design.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: `joxette-service` — remove the Jackson 2 fallback, final verification

**Files:**
- Modify: `joxette-service/pom.xml`

**Interfaces:**
- Consumes: nothing new.
- Produces: final state — no Jackson 2 dependency, no Jackson 2 type anywhere in `joxette-core`/`joxette-kafka`/`joxette-sol`/`joxette-service` source.

- [ ] **Step 1: Confirm nothing in `joxette-service` still references Jackson 2**

Run:
```bash
grep -rln "com\.fasterxml\.jackson\.\(core\|databind\)\." --include="*.java" joxette-service/src
```
Expected: no output. (Files using only `com.fasterxml.jackson.annotation.*` are fine and expected to remain — that package doesn't move.)

- [ ] **Step 2: Remove the Jackson 2 dependency from `joxette-service/pom.xml`**

Find:
```xml
        <!-- Jackson 3 for JSON handling in REST API (migration in progress: see
             docs/superpowers/plans/2026-09-15-jackson3-migration.md) -->
        <dependency>
            <groupId>tools.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>

        <!-- Jackson 2, kept temporarily during the Jackson 3 migration for classes
             not yet repointed. Removed once every joxette-service source file
             uses tools.jackson.* instead. -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
```
Replace with:
```xml
        <!-- Jackson 3 for JSON handling in REST API -->
        <dependency>
            <groupId>tools.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
```

- [ ] **Step 3: Compile and run the full reactor test suite**

Run: `mvn test`
Expected: BUILD SUCCESS across every module (`joxette-core`, `joxette-kafka`, `joxette-sol`, `joxette-service`, `joxette-test-kit`, `joxette-operator`). `joxette-operator` is unaffected — it never depended on this migration and keeps its own Jackson 2 usage via Fabric8/JOSDK.

- [ ] **Step 4: Confirm Jackson 2 is no longer a direct/explicit dependency of any migrated module**

Run:
```bash
mvn -pl joxette-core,joxette-kafka,joxette-sol,joxette-service dependency:tree -DoutputFile=/tmp/deptree-final.txt
grep "com.fasterxml.jackson.core:jackson-databind" /tmp/deptree-final.txt
```
Expected: either no output, or matches only under a transitive branch belonging to a third-party library (e.g. `springdoc-openapi`/`swagger-core-jakarta`), never as a top-level `+-` entry declared by a `joxette-*` module itself.

- [ ] **Step 5: Manual smoke test**

Start the service locally (however this project normally does — check for a `run` skill or the project's usual `mvn spring-boot:run` / Testcontainers-backed profile) and exercise, per the approved design spec's testing strategy:
```bash
curl -s -H "Accept: application/json" "http://localhost:<port>/v1/cassettes/topics/<some-topic>"
curl -s -H "Accept: application/x-ndjson" "http://localhost:<port>/v1/cassettes/topics/<some-topic>"
```
Confirm both return well-formed responses with ISO-8601 timestamps, matching what `TimestampSerializationIT` already asserts automatically. Since this needs a populated topic/cassette and running Kafka, if there's no quick local fixture available, treat the automated `TimestampSerializationIT`/`ProblemDetailContractIT` runs from Task 6 as sufficient evidence and note in the commit message that the manual check was skipped in favor of the equivalent automated IT coverage.

- [ ] **Step 6: Commit**

```bash
git add joxette-service/pom.xml
git commit -m "$(cat <<'EOF'
Remove the Jackson 2 fallback dependency from joxette-service

Final step of the Jackson 2 -> Jackson 3 migration: every joxette-core/
joxette-kafka/joxette-sol/joxette-service source file now uses
tools.jackson.* exclusively (annotations-only usages, e.g. @JsonProperty,
correctly remain on com.fasterxml.jackson.annotation, which is unchanged
in Jackson 3). Full reactor test suite is green. joxette-operator is
intentionally untouched -- it depends on Fabric8/JOSDK, which are
Jackson-2-locked internally.

This unblocks the follow-up spec: migrating NDJSON streaming
(SseReplayHandler, ExportService) to the new jox-json module, which
requires tools.jackson.* types throughout.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```
