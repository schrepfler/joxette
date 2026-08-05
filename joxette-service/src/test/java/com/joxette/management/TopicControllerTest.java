package com.joxette.management;

import com.joxette.api.error.ResourceNotFoundException;
import com.joxette.config.events.ConfigEventBus;
import com.joxette.config.events.TopicConfigChanged;
import com.joxette.db.SchemaManager;
import com.joxette.management.TopicMode;
import com.joxette.recording.RecordingCoordinator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
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
    @Mock SchemaManager schemaManager;

    @InjectMocks TopicController controller;

    // =========================================================================
    // PUT /topics/{topic} — mode change publishes event
    // =========================================================================

    @Test
    void updateTopic_modeChange_publishesEvent() throws Exception {
        TopicConfig existing = new TopicConfig("orders", TopicMode.GENERAL, false, false, null, "latest", null);
        TopicConfig updated  = new TopicConfig("orders", TopicMode.BOTH,    false, false, null, "latest", null);

        when(config.findTopic("orders")).thenReturn(Optional.of(existing));
        when(config.upsertTopic("orders", "both", false, "latest", null)).thenReturn(updated);

        ResponseEntity<TopicConfig> response =
                controller.updateTopic("orders", new TopicController.UpdateTopicRequest("both", null));

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().mode()).isEqualTo(TopicMode.BOTH);

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
    // PUT /topics/{topic} — general cassette table creation (findings 1 & 2)
    // =========================================================================

    /**
     * Covers finding 1: mode comparison must be case-insensitive, matching
     * {@link TopicMode#fromValue(String)} (used by {@link ConfigRepository#upsertTopic}),
     * not a raw-string equality check.
     */
    @ParameterizedTest
    @CsvSource({
            "general,     true",
            "both,        true",
            "entity_only, false",
            "General,     true",
            "BOTH,        true",
            "Entity_Only, false",
    })
    void updateTopic_createsGeneralTable_whenModeExplicitlySet(String mode, boolean expectCreateGeneralTable)
            throws Exception {
        TopicConfig existing = new TopicConfig("orders", TopicMode.ENTITY_ONLY, false, false, null, "latest", null);
        TopicConfig updated  = new TopicConfig("orders", TopicMode.fromValue(mode), false, false, null, "latest", null);

        when(config.findTopic("orders")).thenReturn(Optional.of(existing));
        when(config.upsertTopic("orders", mode, false, "latest", null)).thenReturn(updated);

        controller.updateTopic("orders", new TopicController.UpdateTopicRequest(mode, null));

        if (expectCreateGeneralTable) {
            verify(schemaManager).createGeneralTable("orders");
        } else {
            verify(schemaManager, never()).createGeneralTable(any());
        }
    }

    /**
     * Covers finding 2: when {@code mode} is omitted from the request body (a partial
     * update, e.g. brokerId-only), the effective mode must be resolved against the
     * topic's EXISTING persisted mode — not silently defaulted to GENERAL — so an
     * entity_only/both topic keeps (and gets a table created for, if needed) its real mode.
     */
    @ParameterizedTest
    @EnumSource(TopicMode.class)
    void updateTopic_modeOmitted_resolvesAgainstExistingMode(TopicMode existingMode) throws Exception {
        TopicConfig existing = new TopicConfig("orders", existingMode, false, false, null, "latest", "broker-1");
        TopicConfig updated  = new TopicConfig("orders", existingMode, false, false, null, "latest", "broker-2");

        when(config.findTopic("orders")).thenReturn(Optional.of(existing));
        when(config.upsertTopic("orders", existingMode.getValue(), false, "latest", "broker-2")).thenReturn(updated);

        ResponseEntity<TopicConfig> response =
                controller.updateTopic("orders", new TopicController.UpdateTopicRequest(null, "broker-2"));

        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().mode()).isEqualTo(existingMode);

        if (existingMode.writesGeneral()) {
            verify(schemaManager).createGeneralTable("orders");
        } else {
            verify(schemaManager, never()).createGeneralTable(any());
        }
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
        TopicConfig saved = new TopicConfig("payments", TopicMode.BOTH, false, false, null, "latest", null);

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

    /**
     * Covers finding 1: case-insensitive mode resolution on the create path too,
     * plus the baseline general/both/entity_only behavior.
     */
    @ParameterizedTest
    @CsvSource({
            "general,     true",
            "both,        true",
            "entity_only, false",
            "General,     true",
            "BOTH,        true",
            "Entity_Only, false",
    })
    void createTopic_createsGeneralTable_basedOnMode(String mode, boolean expectCreateGeneralTable) throws Exception {
        TopicConfig saved = new TopicConfig("payments", TopicMode.fromValue(mode), false, false, null, "latest", null);

        when(config.findTopic("payments")).thenReturn(Optional.empty());
        when(config.upsertTopic("payments", mode, false, "latest", null)).thenReturn(saved);

        controller.createTopic(new TopicController.CreateTopicRequest(
                "payments", mode, "latest", null, false, null, null));

        if (expectCreateGeneralTable) {
            verify(schemaManager).createGeneralTable("payments");
        } else {
            verify(schemaManager, never()).createGeneralTable(any());
        }
    }
}
