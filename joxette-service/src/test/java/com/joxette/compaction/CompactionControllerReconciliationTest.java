package com.joxette.compaction;

import com.joxette.lifecycle.BackgroundTaskRegistry;
import com.joxette.reconciliation.ReconciliationRun;
import com.joxette.reconciliation.ReconciliationService;
import com.joxette.reconciliation.ReconciliationStatus;
import com.joxette.config.JoxetteProperties;
import com.joxette.api.error.GlobalExceptionHandler;
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
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CompactionControllerReconciliationTest {

    @Mock CompactionService compactionService;
    @Mock RetentionService retentionService;
    @Mock ReconciliationService reconciliationService;
    @Mock JoxetteProperties props;
    @Mock CompactionLockManager lockManager;
    @Mock org.apache.pekko.actor.typed.ActorRef<CompactionSingletonActor.CompactionCommand> compactionSingleton;

    private static ActorSystem<Void> actorSystem;
    private MockMvc mvc;

    @BeforeAll
    static void startActorSystem() {
        actorSystem = ActorSystem.create(Behaviors.empty(), "compaction-recon-test");
    }

    @AfterAll
    static void stopActorSystem() {
        actorSystem.terminate();
    }

    @BeforeEach
    void setUp() {
        BackgroundTaskRegistry taskRegistry = new BackgroundTaskRegistry();
        taskRegistry.start();
        CompactionController controller = new CompactionController(
                compactionService, retentionService, reconciliationService,
                compactionSingleton, actorSystem, props, taskRegistry, lockManager);
        mvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    @Test
    void getReconciliationStatus_returnsServiceResult() throws Exception {
        ReconciliationStatus status = new ReconciliationStatus(null, Instant.parse("2026-08-07T04:00:00Z"), false);
        when(reconciliationService.getStatus()).thenReturn(status);

        mvc.perform(get("/compaction/reconciliation-status"))
           .andExpect(status().isOk())
           .andExpect(jsonPath("$.running").value(false))
           .andExpect(jsonPath("$.nextScheduledRun").value("2026-08-07T04:00:00Z"));
    }

    @Test
    void triggerReconciliation_returns202WithRun() throws Exception {
        ReconciliationRun run = new ReconciliationRun(1L, Instant.now(), null, RunStatus.RUNNING,
                TriggerSource.MANUAL, java.util.List.of(), 0, 0, 0, 0, 0, 0, false, null);
        when(reconciliationService.beginRun(TriggerSource.MANUAL, null, false)).thenReturn(run);

        mvc.perform(post("/compaction/trigger-reconciliation")
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content("{}"))
           .andExpect(status().isAccepted())
           .andExpect(jsonPath("$.id").value(1))
           .andExpect(jsonPath("$.status").value("running"));
    }

    @Test
    void triggerReconciliation_whileAlreadyRunning_returns409() throws Exception {
        when(reconciliationService.beginRun(TriggerSource.MANUAL, null, false))
                .thenThrow(com.joxette.api.error.ConflictException.reconciliationAlreadyRunning());

        mvc.perform(post("/compaction/trigger-reconciliation")
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content("{}"))
           .andExpect(status().isConflict())
           .andExpect(jsonPath("$.detail").value("Reconciliation run already in progress"));
    }
}
