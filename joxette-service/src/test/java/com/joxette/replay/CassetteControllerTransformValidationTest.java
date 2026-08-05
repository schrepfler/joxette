package com.joxette.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.api.error.GlobalExceptionHandler;
import com.joxette.config.JoxetteProperties;
import com.joxette.management.ConfigRepository;
import com.joxette.management.KafkaTopicAdmin;
import com.joxette.recording.CassetteRecordingBus;
import com.joxette.replay.sink.kafka.KafkaRecordSinkFactory;
import com.joxette.replay.transform.GuardedStep;
import com.joxette.replay.transform.Predicate;
import com.joxette.replay.transform.ReplayMetadataInjector;
import com.joxette.replay.transform.TransformPreset;
import com.joxette.replay.transform.TransformPresetRepository;
import com.joxette.replay.transform.TransformStep;
import com.joxette.replay.transform.steps.AddHeaderStep;
import com.joxette.replay.transform.steps.ConditionalStep;
import com.joxette.replay.transform.steps.RenameFieldStep;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.format.support.FormattingConversionService;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Covers eager transform-pipeline validation in {@code CassetteController.validated()} /
 * {@code validateStep()} (added in commit {@code adfd64e}): malformed field references must
 * fail fast with HTTP 400 before any stream starts, across all three content-negotiated
 * response variants (JSON, SSE, NDJSON).
 *
 * <h2>{@code GuardedStep} coverage — how these tests reach it</h2>
 * <p>{@code GuardedStep} is the transparent wrapper {@code TransformStepDeserializer}
 * produces for any step JSON carrying a {@code "when"} field. The most natural way to
 * reproduce the bypass would be an HTTP request with {@code ?transform=...} containing a
 * {@code "when"} guard. <b>That path currently cannot construct a {@code GuardedStep} at
 * all</b>: {@code TransformStepJacksonModule} registers {@code TransformStepDeserializer}
 * via {@code SimpleModule.addDeserializer(TransformStep.class, ...)}, but because
 * {@code TransformStep} also carries {@code @JsonTypeInfo}/{@code @JsonSubTypes}, Jackson's
 * polymorphic type resolution resolves straight to the concrete subtype and deserializes
 * it directly — silently dropping the unrecognised {@code "when"} property (Spring Boot
 * disables {@code FAIL_ON_UNKNOWN_PROPERTIES}) and never invoking the custom deserializer.
 * Verified directly against the real {@code Jackson2ObjectMapperBuilder}-built ObjectMapper
 * with {@code TransformStepJacksonModule} installed, for both a root-level
 * {@code TransformStep} read and a {@code List<TransformStep>} read: both return the bare
 * concrete step with {@code "when"} gone, never a {@code GuardedStep}. This is a separate,
 * pre-existing Jackson-wiring gap — out of scope here — that deserves its own fix and is
 * called out in this task's report.
 *
 * <p>To still exercise {@code validateStep()}'s handling of {@code GuardedStep} through a
 * real, reachable production seam, these tests go through the {@code transform_preset} path:
 * {@code presetRepository} is mocked to return a {@link TransformPreset} whose {@code steps()}
 * contains a directly-constructed {@link GuardedStep} — exactly the shape a
 * {@code TransformPreset} loaded from the {@code transform_presets} table would hand to
 * {@code CassetteController#resolveTransformSteps}, and exactly how existing guard-behaviour
 * tests ({@code WhenGuardTest}, {@code TransformPipelineIntegrationTest}) already construct
 * {@code GuardedStep}. This proves {@code validateStep()}'s own logic correctly, independent
 * of the separate deserialization gap.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CassetteControllerTransformValidationTest {

    @Mock TopicReplayService        topicService;
    @Mock EntityReplayService       entityService;
    @Mock SseReplayHandler          sseHandler;
    @Mock CassetteLifecycleService  lifecycle;
    @Mock KafkaRecordSinkFactory    sinkFactory;
    @Mock ScheduledReplayService    scheduledReplayService;
    @Mock ReplayMetadataInjector    metadataInjector;
    @Mock TransformPresetRepository presetRepository;
    @Mock SequenceMatchService      sequenceMatchService;
    @Mock FieldSuggestionsService   fieldSuggestionsService;
    @Mock CassetteRecordingBus      recordingBus;
    @Mock KafkaTopicAdmin           kafkaTopicAdmin;
    @Mock ConfigRepository          configRepository;
    @SuppressWarnings("unchecked")
    @Mock ActorRef<ReplayCoordinatorActor.Cmd> replayCoordinator;
    @SuppressWarnings("unchecked")
    @Mock ActorSystem<Void>         actorSystem;

    private MockMvc mvc;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        JoxetteProperties properties = new JoxetteProperties();

        CassetteController controller = new CassetteController(
                topicService, entityService, sseHandler, lifecycle, sinkFactory,
                scheduledReplayService, metadataInjector, presetRepository, properties,
                mapper, sequenceMatchService, null, null,
                fieldSuggestionsService, recordingBus, kafkaTopicAdmin, configRepository,
                replayCoordinator, actorSystem,
                Executors.newVirtualThreadPerTaskExecutor(),
                new StateFoldService(mapper),
                new DiffService(mapper),
                new TimelineService(),
                new PortraitService());

        FormattingConversionService conversion = new FormattingConversionService();
        conversion.addConverter(String.class, Order.class, Order::parse);

        mvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .setConversionService(conversion)
                .build();
    }

    // =========================================================================
    // Pre-existing coverage (commit adfd64e): malformed field references in the
    // 7 directly-validated step types + conditional/gap_transform nesting.
    // =========================================================================

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedTransformSteps")
    void malformedTransformStep_returns400BeforeStreaming(String label, String transformJson) throws Exception {
        mvc.perform(get("/cassettes/topics/orders").param("transform", transformJson))
           .andExpect(status().isBadRequest())
           .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));
    }

    static Stream<Arguments> malformedTransformSteps() {
        return Stream.of(
            Arguments.of("rename_field: source missing '$.' prefix",
                "[{\"type\":\"rename_field\",\"source\":\"value.orderId\",\"new_name\":\"order_id\"}]"),
            Arguments.of("merge_patch: target missing '$.' prefix",
                "[{\"type\":\"merge_patch\",\"target\":\"value\",\"patch\":{}}]"),
            Arguments.of("add_computed_field: target missing '$.' prefix",
                "[{\"type\":\"add_computed_field\",\"target\":\"value.seq\",\"expression\":\"REPLAY_SEQUENCE\"}]"),
            Arguments.of("copy_to_header: source has invalid JSONPath syntax",
                "[{\"type\":\"copy_to_header\",\"source\":\"$.value.[[[\",\"headerKey\":\"x-id\"}]"),
            Arguments.of("redirect_topic: template placeholder has invalid JSONPath syntax",
                "[{\"type\":\"redirect_topic\",\"topic\":\"prefix-${$.value.[[[}\"}]"),
            Arguments.of("conditional: condition field has invalid JSONPath syntax",
                "[{\"type\":\"conditional\",\"condition\":{\"field\":\"$.value.[[[\",\"operator\":\"EQ\",\"value\":\"x\"},\"then_steps\":[]}]"),
            Arguments.of("gap_transform: nested predicate has invalid JSONPath syntax",
                "[{\"type\":\"gap_transform\",\"select\":{\"after\":{\"predicate\":{\"field\":\"$.value.[[[\",\"operator\":\"EQ\",\"value\":\"x\"},\"quantifier\":\"first\"},\"min_duration_ms\":3000},\"operation\":{\"op\":\"cut\"}}]"),
            // Finding I5: 4 more step types that call JsonStepHelper#parentAndLeaf internally
            // and silently no-op on IllegalArgumentException at runtime — same bug class as
            // the 7 steps above, missed by the original eager-validation pass.
            Arguments.of("delete_field: target missing '$.' prefix",
                "[{\"type\":\"delete_field\",\"target\":\"value.internal_debug_info\"}]"),
            Arguments.of("flatten_field: source missing '$.' prefix",
                "[{\"type\":\"flatten_field\",\"source\":\"value.metadata\"}]"),
            Arguments.of("key_from_value: expression missing '$.' prefix",
                "[{\"type\":\"key_from_value\",\"expression\":\"value.order_id\"}]"),
            Arguments.of("remap_key: template placeholder has invalid JSONPath syntax",
                "[{\"type\":\"remap_key\",\"value\":\"prefix-${$.value.[[[}\"}]")
        );
    }

    // =========================================================================
    // Point 6: eager validation also applies to the SSE and NDJSON variants,
    // not just the default JSON endpoint — 400 with no stream ever started.
    // =========================================================================

    @ParameterizedTest(name = "SSE: {0}")
    @MethodSource("malformedTransformSteps")
    void malformedTransformStep_sse_returns400BeforeStreaming(String label, String transformJson) throws Exception {
        mvc.perform(get("/cassettes/topics/orders")
                    .accept(MediaType.TEXT_EVENT_STREAM)
                    .param("transform", transformJson))
           .andExpect(status().isBadRequest())
           .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));

        // No stream must ever have been started: the SSE handler and the query service
        // are never touched once validation fails eagerly.
        verifyNoInteractions(sseHandler);
        verifyNoInteractions(topicService);
    }

    @ParameterizedTest(name = "NDJSON: {0}")
    @MethodSource("malformedTransformSteps")
    void malformedTransformStep_ndjson_returns400BeforeStreaming(String label, String transformJson) throws Exception {
        mvc.perform(get("/cassettes/topics/orders")
                    .accept(MediaType.parseMediaType("application/x-ndjson"))
                    .param("transform", transformJson))
           .andExpect(status().isBadRequest())
           .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));

        verifyNoInteractions(sseHandler);
        verifyNoInteractions(topicService);
    }

    // =========================================================================
    // CRITICAL finding: GuardedStep wrapping defeats eager validation.
    //
    // validateStep()'s instanceof chain had no branch for GuardedStep, so a step
    // wrapped by a "when" guard sailed through validation untouched regardless of
    // how malformed its delegate (or the guard predicate itself) was — reproducing
    // the pre-Task-E bug (200 then silent mid-stream failure). See class javadoc
    // for why these tests go through transform_preset (a real, reachable seam that
    // hands validateStep() a directly-constructed GuardedStep) rather than the
    // transform query param.
    // =========================================================================

    private void stubPreset(String name, TransformStep... steps) {
        when(presetRepository.findByName(eq(name)))
                .thenReturn(Optional.of(new TransformPreset(
                        name, null, List.of(steps), List.of(), null, null)));
    }

    @Test
    void guardedStep_malformedDelegate_returns400BeforeStreaming() throws Exception {
        // Exact reproduction from the finding: a when-guarded rename_field whose delegate
        // 'source' is missing the required '$.' prefix.
        var guarded = new GuardedStep(
                new Predicate.Leaf("$.timestamp", Predicate.Operator.IS_NOT_NULL, null),
                new RenameFieldStep("value.orderId", "y"));
        stubPreset("guarded-bad-delegate", guarded);

        mvc.perform(get("/cassettes/topics/orders").param("transform_preset", "guarded-bad-delegate"))
           .andExpect(status().isBadRequest())
           .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));

        verifyNoInteractions(topicService);
    }

    @Test
    void guardedStep_malformedGuardPredicate_returns400BeforeStreaming() throws Exception {
        // Delegate itself is fine (add_header has no field-path to validate); the guard
        // predicate's own field is the malformed JSONPath.
        var guarded = new GuardedStep(
                new Predicate.Leaf("$.value.[[[", Predicate.Operator.EQ, "z"),
                new AddHeaderStep("x-id", "y", false));
        stubPreset("guarded-bad-when", guarded);

        mvc.perform(get("/cassettes/topics/orders").param("transform_preset", "guarded-bad-when"))
           .andExpect(status().isBadRequest())
           .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));

        verifyNoInteractions(topicService);
    }

    @Test
    void guardedStep_nestedInConditionalThenBranch_malformedDelegate_returns400() throws Exception {
        // Per TransformStepDeserializer's javadoc, "when" wrapping applies recursively
        // inside ConditionalStep.thenSteps()/elseSteps() too, so a GuardedStep can appear
        // nested there, not just at the top level.
        var nestedGuarded = new GuardedStep(
                new Predicate.Leaf("$.value.status", Predicate.Operator.IS_NOT_NULL, null),
                new RenameFieldStep("value.orderId", "y"));
        var conditional = new ConditionalStep(
                new Predicate.Leaf("$.value.status", Predicate.Operator.EQ, "x"),
                List.of(nestedGuarded), List.of());
        stubPreset("guarded-nested-then", conditional);

        mvc.perform(get("/cassettes/topics/orders").param("transform_preset", "guarded-nested-then"))
           .andExpect(status().isBadRequest())
           .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));

        verifyNoInteractions(topicService);
    }

    @Test
    void guardedStep_nestedInConditionalElseBranch_malformedDelegate_returns400() throws Exception {
        var nestedGuarded = new GuardedStep(
                new Predicate.Leaf("$.timestamp", Predicate.Operator.IS_NOT_NULL, null),
                new RenameFieldStep("value.orderId", "y"));
        var conditional = new ConditionalStep(
                new Predicate.Leaf("$.value.status", Predicate.Operator.EQ, "x"),
                List.of(), List.of(nestedGuarded));
        stubPreset("guarded-nested-else", conditional);

        mvc.perform(get("/cassettes/topics/orders").param("transform_preset", "guarded-nested-else"))
           .andExpect(status().isBadRequest())
           .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));

        verifyNoInteractions(topicService);
    }

    @Test
    void guardedStep_malformedDelegate_sse_returns400BeforeStreaming() throws Exception {
        var guarded = new GuardedStep(
                new Predicate.Leaf("$.timestamp", Predicate.Operator.IS_NOT_NULL, null),
                new RenameFieldStep("value.orderId", "y"));
        stubPreset("guarded-bad-delegate-sse", guarded);

        mvc.perform(get("/cassettes/topics/orders")
                    .accept(MediaType.TEXT_EVENT_STREAM)
                    .param("transform_preset", "guarded-bad-delegate-sse"))
           .andExpect(status().isBadRequest())
           .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));

        verifyNoInteractions(sseHandler);
        verifyNoInteractions(topicService);
    }

    @Test
    void guardedStep_validGuardAndDelegate_stillReaches200() throws Exception {
        // Sanity check: a well-formed guarded step must NOT be rejected by the new
        // validation branch — only malformed content should 400.
        var guarded = new GuardedStep(
                new Predicate.Leaf("$.timestamp", Predicate.Operator.IS_NOT_NULL, null),
                new RenameFieldStep("$.value.orderId", "order_id"));
        stubPreset("guarded-valid", guarded);
        when(topicService.query(eq("orders"), eq(null), eq(null), eq(null), eq(null), eq(null),
                org.mockito.ArgumentMatchers.anyInt(), eq(null),
                org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.any()))
                .thenReturn(new PagedResponse<>(List.of(), null, false, null, null));

        mvc.perform(get("/cassettes/topics/orders").param("transform_preset", "guarded-valid"))
           .andExpect(status().isOk());
    }
}
