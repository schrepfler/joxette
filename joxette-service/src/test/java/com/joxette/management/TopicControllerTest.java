package com.joxette.management;

import com.joxette.api.error.ResourceNotFoundException;
import com.joxette.config.events.ConfigEventBus;
import com.joxette.config.events.TopicConfigChanged;
import com.joxette.recording.RecordingCoordinator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link TopicController} verifying that a
 * {@link TopicConfigChanged} event is published after every mutating operation
 * so all recording-enabled nodes reconcile via {@link com.joxette.recording.RecordingConfigWatcher}.
 */
@ExtendWith(MockitoExtension.class)
class TopicControllerTest {

    @Mock ConfigRepository config;
    @Mock RecordingCoordinator coordinator;
    @Mock KafkaTopicAdmin kafkaTopicAdmin;
    @Mock ConfigEventBus eventBus;

    @InjectMocks TopicController controller;

    // =========================================================================
    // PUT /topics/{topic} — mode change publishes event
    // =========================================================================

    @Test
    void updateTopic_modeChange_publishesEvent() throws Exception {
        TopicConfig existing = new TopicConfig("orders", "general", false, false, null, "latest", null);
        TopicConfig updated  = new TopicConfig("orders", "both",    false, false, null, "latest", null);

        when(config.findTopic("orders")).thenReturn(Optional.of(existing));
        when(config.upsertTopic("orders", "both", false, "latest", null)).thenReturn(updated);

        ResponseEntity<TopicConfig> response =
                controller.updateTopic("orders", new TopicController.UpdateTopicRequest("both", null));

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().mode()).isEqualTo("both");

        ArgumentCaptor<TopicConfigChanged> cap = ArgumentCaptor.forClass(TopicConfigChanged.class);
        verify(eventBus).publishTopicConfig(cap.capture());
        assertThat(cap.getValue().topic()).isEqualTo("orders");
        assertThat(cap.getValue().changeType()).isEqualTo("updated");
    }

    @Test
    void updateTopic_topicNotFound_doesNotPublishEvent() throws Exception {
        when(config.findTopic("missing")).thenReturn(Optional.empty());

        assertThatThrownBy(() ->
                controller.updateTopic("missing", new TopicController.UpdateTopicRequest("both", null)))
                .isInstanceOf(ResourceNotFoundException.class);

        verify(eventBus, never()).publishTopicConfig(any());
    }

    // =========================================================================
    // DELETE /topics/{topic} — publishes deleted event
    // =========================================================================

    @Test
    void deleteTopic_publishesDeletedEvent() throws Exception {
        when(config.deleteTopic("orders")).thenReturn(true);

        ResponseEntity<Void> response = controller.deleteTopic("orders");

        assertThat(response.getStatusCode().value()).isEqualTo(204);

        ArgumentCaptor<TopicConfigChanged> cap = ArgumentCaptor.forClass(TopicConfigChanged.class);
        verify(eventBus).publishTopicConfig(cap.capture());
        assertThat(cap.getValue().changeType()).isEqualTo("deleted");
    }

    @Test
    void deleteTopic_notFound_doesNotPublishEvent() throws Exception {
        when(config.deleteTopic("missing")).thenReturn(false);

        assertThatThrownBy(() -> controller.deleteTopic("missing"))
                .isInstanceOf(ResourceNotFoundException.class);

        verify(eventBus, never()).publishTopicConfig(any());
    }

    // =========================================================================
    // POST /topics — publishes created event
    // =========================================================================

    @Test
    void createTopic_publishesCreatedEvent() throws Exception {
        TopicConfig saved = new TopicConfig("payments", "both", false, false, null, "latest", null);

        when(config.findTopic("payments")).thenReturn(Optional.empty());
        when(config.upsertTopic("payments", "both", false, "latest", null)).thenReturn(saved);

        ResponseEntity<TopicConfig> response =
                controller.createTopic(new TopicController.CreateTopicRequest(
                        "payments", "both", "latest", null, false, null, null));

        assertThat(response.getStatusCode().value()).isEqualTo(201);

        ArgumentCaptor<TopicConfigChanged> cap = ArgumentCaptor.forClass(TopicConfigChanged.class);
        verify(eventBus).publishTopicConfig(cap.capture());
        assertThat(cap.getValue().topic()).isEqualTo("payments");
        assertThat(cap.getValue().changeType()).isEqualTo("created");
    }
}
