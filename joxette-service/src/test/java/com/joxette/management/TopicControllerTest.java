package com.joxette.management;

import com.joxette.api.error.ResourceNotFoundException;
import com.joxette.recording.RecordingCoordinator;
import com.joxette.replay.MessageRouter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;

import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link TopicController} verifying that
 * {@link MessageRouter#reload()} is called after every mutating operation.
 */
@ExtendWith(MockitoExtension.class)
class TopicControllerTest {

    @Mock ConfigRepository config;
    @Mock RecordingCoordinator coordinator;
    @Mock MessageRouter router;
    @Mock KafkaTopicAdmin kafkaTopicAdmin;

    @InjectMocks TopicController controller;

    // =========================================================================
    // PUT /topics/{topic} — mode change triggers reload
    // =========================================================================

    @Test
    void updateTopic_modeChange_callsRouterReload() throws Exception {
        TopicConfig existing = new TopicConfig("orders", "general", false, false, null, "latest", null);
        TopicConfig updated  = new TopicConfig("orders", "both",    false, false, null, "latest", null);

        when(config.findTopic("orders")).thenReturn(Optional.of(existing));
        when(config.upsertTopic("orders", "both", false, "latest", null)).thenReturn(updated);

        ResponseEntity<TopicConfig> response =
                controller.updateTopic("orders", new TopicController.UpdateTopicRequest("both", null));

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().mode()).isEqualTo("both");
        verify(router).reload();
    }

    @Test
    void updateTopic_topicNotFound_doesNotCallReload() throws Exception {
        when(config.findTopic("missing")).thenReturn(Optional.empty());

        assertThatThrownBy(() ->
                controller.updateTopic("missing", new TopicController.UpdateTopicRequest("both", null)))
                .isInstanceOf(ResourceNotFoundException.class);

        verify(router, never()).reload();
    }

    @Test
    void updateTopic_routerReloadFailure_stillReturnsOk() throws Exception {
        TopicConfig existing = new TopicConfig("orders", "general", false, false, null, "latest", null);
        TopicConfig updated  = new TopicConfig("orders", "entity_only", false, false, null, "latest", null);

        when(config.findTopic("orders")).thenReturn(Optional.of(existing));
        when(config.upsertTopic("orders", "entity_only", false, "latest", null)).thenReturn(updated);
        doThrow(new SQLException("db error")).when(router).reload();

        ResponseEntity<TopicConfig> response =
                controller.updateTopic("orders", new TopicController.UpdateTopicRequest("entity_only", null));

        // reload failure is logged but not propagated
        assertThat(response.getStatusCode().value()).isEqualTo(200);
    }

    // =========================================================================
    // DELETE /topics/{topic} — reload after successful delete
    // =========================================================================

    @Test
    void deleteTopic_callsRouterReload() throws Exception {
        when(config.deleteTopic("orders")).thenReturn(true);

        ResponseEntity<Void> response = controller.deleteTopic("orders");

        assertThat(response.getStatusCode().value()).isEqualTo(204);
        verify(router).reload();
    }

    @Test
    void deleteTopic_notFound_doesNotCallReload() throws Exception {
        when(config.deleteTopic("missing")).thenReturn(false);

        assertThatThrownBy(() -> controller.deleteTopic("missing"))
                .isInstanceOf(ResourceNotFoundException.class);

        verify(router, never()).reload();
    }

    // =========================================================================
    // POST /topics — reload after creation so entity routing activates
    // =========================================================================

    @Test
    void createTopic_callsRouterReload() throws Exception {
        TopicConfig saved = new TopicConfig("payments", "both", false, false, null, "latest", null);

        when(config.findTopic("payments")).thenReturn(Optional.empty());
        when(config.upsertTopic("payments", "both", false, "latest", null)).thenReturn(saved);
        when(coordinator.activeTopics()).thenReturn(Set.of("payments"));

        ResponseEntity<TopicConfig> response =
                controller.createTopic(new TopicController.CreateTopicRequest("payments", "both", "latest", null, false, null, null));

        assertThat(response.getStatusCode().value()).isEqualTo(201);
        verify(router).reload();
    }
}
