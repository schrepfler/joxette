package com.joxette.api.error;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.compaction.CompactionController;
import com.joxette.compaction.CompactionRun;
import com.joxette.compaction.CompactionService;
import com.joxette.compaction.CompactionSingletonActor;
import com.joxette.compaction.RetentionService;
import com.joxette.config.JoxetteProperties;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

/**
 * MockMvc coverage of the RFC 7807 ProblemDetail contract for
 * {@link CompactionController}: 409 when compaction is already running, 503
 * when the database is unavailable, and 500 fallback for uncaught errors.
 *
 * <p>An actual Pekko {@link ActorSystem} (single-node, no clustering) is started
 * for the test class so that the controller's {@code AskPattern.ask} calls
 * resolve against real actor behaviors rather than mocks.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CompactionControllerProblemDetailTest {

    @Mock CompactionService compactionService;
    @Mock RetentionService retentionService;
    @Mock JoxetteProperties props;

    // Shared actor system — started once, terminated after all tests.
    private static ActorSystem<Void> actorSystem;

    // Actor that always reports "busy" — used for the 409 test.
    private ActorRef<CompactionSingletonActor.CompactionCommand> busySingleton;
    // Actor that accepts triggers — used for success / infra-error tests.
    private ActorRef<CompactionSingletonActor.CompactionCommand> acceptingSingleton;

    private MockMvc mvc;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeAll
    static void startActorSystem() {
        actorSystem = ActorSystem.create(Behaviors.empty(), "compaction-test");
    }

    @AfterAll
    static void stopActorSystem() {
        if (actorSystem != null) actorSystem.terminate();
    }

    @BeforeEach
    void setUp() {
        // "busy" actor: always replies CompactionBusy
        busySingleton = actorSystem.systemActorOf(
                Behaviors.receive(CompactionSingletonActor.CompactionCommand.class)
                        .onMessage(CompactionSingletonActor.TriggerCompaction.class, msg -> {
                            msg.replyTo().tell(new CompactionSingletonActor.CompactionBusy());
                            return Behaviors.same();
                        })
                        .build(),
                "busy-singleton-" + System.nanoTime(),
                org.apache.pekko.actor.typed.Props.empty());

        // "accepting" actor: always replies CompactionAccepted with a fake run
        CompactionRun fakeRun = new CompactionRun(
                1L, Instant.now(), null, "running", "manual", null, 0, 0, 0, 0, null);
        acceptingSingleton = actorSystem.systemActorOf(
                Behaviors.receive(CompactionSingletonActor.CompactionCommand.class)
                        .onMessage(CompactionSingletonActor.TriggerCompaction.class, msg -> {
                            msg.replyTo().tell(new CompactionSingletonActor.CompactionAccepted(fakeRun));
                            return Behaviors.same();
                        })
                        .build(),
                "accepting-singleton-" + System.nanoTime(),
                org.apache.pekko.actor.typed.Props.empty());
    }

    // =========================================================================
    // 409 — concurrent compaction trigger maps to the conflict problem
    // =========================================================================

    @Test
    void trigger_whileAlreadyRunning_returnsConflictProblem() throws Exception {
        CompactionController controller = new CompactionController(
                compactionService, retentionService, busySingleton, actorSystem, props);
        mvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();

        String body = mapper.writeValueAsString(Map.of("targets", java.util.List.of("order")));

        mvc.perform(post("/compaction/trigger").contentType(MediaType.APPLICATION_JSON).content(body))
           .andExpectAll(ProblemDetailAssertions.problemDetail(
                   409,
                   ErrorTypes.CONFLICT.toString(),
                   ErrorCodes.CONFLICT,
                   "/compaction/trigger"))
           .andExpect(jsonPath("$.detail").value("Compaction already in progress"));
    }

    // =========================================================================
    // 503 — DuckDB unavailable surfaces as upstream-unavailable
    // =========================================================================

    @Test
    void status_duckDbUnavailable_returnsUpstreamUnavailableProblem() throws Exception {
        CompactionController controller = new CompactionController(
                compactionService, retentionService, acceptingSingleton, actorSystem, props);
        mvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
        when(compactionService.getStatus()).thenThrow(new java.sql.SQLException("db connection failed"));

        mvc.perform(get("/compaction/status"))
           .andExpectAll(ProblemDetailAssertions.problemDetail(
                   503,
                   ErrorTypes.UPSTREAM_UNAVAILABLE.toString(),
                   ErrorCodes.UPSTREAM_UNAVAILABLE,
                   "/compaction/status"))
           .andExpect(jsonPath("$.title").value("Upstream Unavailable"));
    }

    // =========================================================================
    // 500 — uncaught RuntimeException, stack-trace-free body
    // =========================================================================

    @Test
    void history_runtime_returnsInternalProblemWithoutDetails() throws Exception {
        CompactionController controller = new CompactionController(
                compactionService, retentionService, acceptingSingleton, actorSystem, props);
        mvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
        when(compactionService.getHistory(20)).thenThrow(new RuntimeException("catastrophic: internal-boom"));

        mvc.perform(get("/compaction/history"))
           .andExpectAll(ProblemDetailAssertions.problemDetail(
                   500,
                   ErrorTypes.INTERNAL.toString(),
                   ErrorCodes.INTERNAL,
                   "/compaction/history"))
           .andExpect(ProblemDetailAssertions.noStackTraceLeak());
    }
}
