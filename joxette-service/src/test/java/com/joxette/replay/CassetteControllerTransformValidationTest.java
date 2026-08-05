package com.joxette.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.api.error.GlobalExceptionHandler;
import com.joxette.config.JoxetteProperties;
import com.joxette.management.ConfigRepository;
import com.joxette.management.KafkaTopicAdmin;
import com.joxette.recording.CassetteRecordingBus;
import com.joxette.replay.sink.kafka.KafkaRecordSinkFactory;
import com.joxette.replay.transform.ReplayMetadataInjector;
import com.joxette.replay.transform.TransformPresetRepository;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.format.support.FormattingConversionService;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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
                "[{\"type\":\"gap_transform\",\"select\":{\"after\":{\"predicate\":{\"field\":\"$.value.[[[\",\"operator\":\"EQ\",\"value\":\"x\"},\"quantifier\":\"first\"},\"min_duration_ms\":3000},\"operation\":{\"op\":\"cut\"}}]")
        );
    }
}
