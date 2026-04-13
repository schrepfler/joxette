package com.joxette.replay.transform;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.steps.AddHeaderStep;
import com.joxette.replay.transform.steps.CopyToHeaderStep;
import com.joxette.replay.transform.steps.FanOutStep;
import com.joxette.replay.transform.steps.FilterDropStep;
import com.joxette.replay.transform.steps.RedirectTopicStep;
import com.joxette.replay.transform.steps.RemoveHeaderStep;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An ordered sequence of {@link TransformStep}s applied to a {@link ReplayMessage}.
 *
 * <p>This class is <em>not</em> a Spring bean — it is per-replay-request and
 * passed as a method argument to the replay services. Use the static
 * {@link #IDENTITY} sentinel when no transformation is required (truly no-op,
 * no metadata injection).
 *
 * <h2>Execution model</h2>
 * <ol>
 *   <li>{@link ReplayMetadataInjector} always runs first when present, injecting
 *       the six provenance headers.</li>
 *   <li>User-defined steps execute in declaration order on the current set of
 *       live messages.</li>
 *   <li>{@link FilterDropStep} — evaluates the predicate; drops matching messages
 *       from the live set.</li>
 *   <li>{@link FanOutStep} — expands each live message into N copies (one per
 *       target topic); subsequent steps run independently on each copy.</li>
 *   <li>{@link AddHeaderStep}, {@link RemoveHeaderStep}, {@link CopyToHeaderStep},
 *       {@link RedirectTopicStep} — implemented; all other steps are identity stubs.</li>
 * </ol>
 *
 * <h2>Return type</h2>
 * <p>{@link #apply} returns a {@code List<ReplayMessage>}:
 * <ul>
 *   <li>One element — normal pass-through.</li>
 *   <li>Empty — message was dropped by a {@link FilterDropStep}.</li>
 *   <li>Multiple elements — expanded by a {@link FanOutStep}.</li>
 * </ul>
 *
 * <h2>Two usage tiers</h2>
 * <ul>
 *   <li>{@link #IDENTITY} — static sentinel (null injector, no steps). Zero overhead.</li>
 *   <li>{@code new TransformPipeline(List.of(), injector)} — metadata-only pipeline
 *       (provenance headers injected, no user steps).</li>
 * </ul>
 */
public final class TransformPipeline {

    /**
     * Truly no-op sentinel — skips both metadata injection and all steps.
     * Preserved for internal code paths that do not require provenance headers.
     */
    public static final TransformPipeline IDENTITY =
            new TransformPipeline(List.of(), null);

    private static final Base64.Decoder B64_DEC = Base64.getUrlDecoder();
    private static final Pattern TEMPLATE_VAR   = Pattern.compile("\\$\\{([^}]+)\\}");

    private final List<TransformStep>    steps;
    private final ReplayMetadataInjector injector;  // null only for IDENTITY

    /**
     * Creates a pipeline with the given steps and metadata injector.
     *
     * @param steps    ordered list of steps; may be empty but not null
     * @param injector the {@link ReplayMetadataInjector} Spring component;
     *                 {@code null} is only valid for the {@link #IDENTITY} sentinel
     */
    public TransformPipeline(List<TransformStep> steps, ReplayMetadataInjector injector) {
        this.steps    = List.copyOf(steps);
        this.injector = injector;
    }

    // -------------------------------------------------------------------------
    // Pipeline execution
    // -------------------------------------------------------------------------

    /**
     * Applies the pipeline to {@code msg}, returning the resulting messages.
     *
     * @param msg      the mutable message to transform (will be mutated in place
     *                 unless {@link FanOutStep} forces copies)
     * @param replayId UUID of the replay session, forwarded to the injector
     * @return list of output messages; empty if dropped, multiple if fan-out expanded
     */
    public List<ReplayMessage> apply(ReplayMessage msg, String replayId) {
        // Step 1: metadata injection (skipped for the IDENTITY null-injector sentinel)
        if (injector != null) {
            injector.inject(msg, replayId);
        }

        // Start with the single input message
        List<ReplayMessage> current = new ArrayList<>();
        current.add(msg);

        // Step 2: user-defined steps in order
        for (TransformStep step : steps) {
            if (current.isEmpty()) break;

            switch (step) {
                case FilterDropStep fds -> {
                    current = current.stream()
                            .filter(m -> !fds.test(m))
                            .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
                }
                case FanOutStep fos -> {
                    List<ReplayMessage> expanded = new ArrayList<>(
                            current.size() * fos.topics().size());
                    for (ReplayMessage m : current) {
                        for (String targetTopic : fos.topics()) {
                            ReplayMessage copy = m.copy();
                            copy.topic = targetTopic;
                            expanded.add(copy);
                        }
                    }
                    current = expanded;
                }
                case AddHeaderStep ahs -> current.forEach(m -> applyAddHeader(ahs, m));
                case RemoveHeaderStep rhs -> current.forEach(m -> applyRemoveHeader(rhs, m));
                case CopyToHeaderStep cths -> current.forEach(m -> applyCopyToHeader(cths, m));
                case RedirectTopicStep rts -> current.forEach(m -> applyRedirectTopic(rts, m));
                default -> {
                    // Other steps are identity stubs — message is unchanged.
                }
            }
        }

        return current;
    }

    /** Returns {@code true} when this is the no-op {@link #IDENTITY} sentinel. */
    public boolean isIdentity() {
        return this == IDENTITY;
    }

    /**
     * Returns the ordered list of user-defined steps (unmodifiable).
     * Useful for {@link com.joxette.replay.transform.steps.SqlPushdownAnalyzer}.
     */
    public List<TransformStep> steps() {
        return steps;
    }

    /**
     * Returns a new pipeline with {@code newSteps}, preserving this pipeline's
     * injector. Used after SQL pushdown analysis to create a pruned pipeline.
     */
    public TransformPipeline withSteps(List<TransformStep> newSteps) {
        return new TransformPipeline(newSteps, this.injector);
    }

    // -------------------------------------------------------------------------
    // Step dispatch helpers
    // -------------------------------------------------------------------------

    private static void applyAddHeader(AddHeaderStep step, ReplayMessage msg) {
        if (step.ifAbsent()
                && msg.headers.stream().anyMatch(h -> step.key().equals(h.key()))) {
            return; // skip — header already exists
        }
        String resolvedValue = resolveTemplate(step.value(), msg);
        msg.headers.add(new CassetteRecord.Header(step.key(), resolvedValue));
    }

    private static void applyRemoveHeader(RemoveHeaderStep step, ReplayMessage msg) {
        msg.headers.removeIf(h -> step.key().equals(h.key()));
    }

    private static void applyCopyToHeader(CopyToHeaderStep step, ReplayMessage msg) {
        Object extracted = extractField(step.source(), msg);
        if (extracted != null) {
            msg.headers.add(new CassetteRecord.Header(step.headerKey(), extracted.toString()));
        }
    }

    private static void applyRedirectTopic(RedirectTopicStep step, ReplayMessage msg) {
        msg.topic = resolveTemplate(step.topic(), msg);
    }

    // -------------------------------------------------------------------------
    // Template resolution  (${path} → extracted value)
    // -------------------------------------------------------------------------

    /**
     * Resolves {@code ${...}} placeholders in {@code template} against {@code msg}.
     * Returns the template unchanged if it contains no placeholders.
     */
    static String resolveTemplate(String template, ReplayMessage msg) {
        if (template == null || !template.contains("${")) return template;
        Matcher m = TEMPLATE_VAR.matcher(template);
        StringBuilder sb = new StringBuilder();
        int last = 0;
        while (m.find()) {
            sb.append(template, last, m.start());
            Object val = extractField(m.group(1), msg);
            sb.append(val != null ? val : "");
            last = m.end();
        }
        sb.append(template, last, template.length());
        return sb.toString();
    }

    // -------------------------------------------------------------------------
    // Field extraction from ReplayMessage by JSONPath-like path
    // -------------------------------------------------------------------------

    /**
     * Extracts a field value from {@code msg} by a JSONPath-like {@code path}.
     *
     * <p>Top-level fields (topic, partition, offset, timestamp, key, recorded_at)
     * are read directly. Paths under {@code $.value.*} decode the base64 message
     * value and evaluate the remainder as a JSONPath expression.
     */
    static Object extractField(String path, ReplayMessage msg) {
        return switch (path) {
            case "$.topic"       -> msg.topic;
            case "$.partition"   -> msg.partition;
            case "$.offset"      -> msg.offset;
            case "$.timestamp"   -> msg.timestamp;
            case "$.key"         -> msg.key;
            case "$.recorded_at" -> msg.recordedAt;
            default -> {
                if (!path.startsWith("$.value") || msg.value == null) yield null;
                try {
                    byte[] raw  = B64_DEC.decode(msg.value);
                    String json = new String(raw, StandardCharsets.UTF_8);
                    if (path.equals("$.value")) yield json;
                    // $.value.foo.bar → $.foo.bar
                    String jsonPath = "$" + path.substring("$.value".length());
                    yield JsonPath.read(json, jsonPath);
                } catch (PathNotFoundException e) {
                    yield null;
                } catch (Exception e) {
                    yield null;
                }
            }
        };
    }
}
