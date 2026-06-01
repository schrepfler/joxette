package com.joxette.management;

import com.joxette.api.error.ConflictException;
import com.joxette.api.error.ResourceNotFoundException;
import com.joxette.config.events.ConfigEventBus;
import com.joxette.config.events.TopicConfigChanged;
import com.joxette.recording.RecorderStatus;
import com.joxette.recording.RecordingCoordinator;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Management API for topic recording configuration.
 *
 * <p>Every mutating endpoint writes to the catalog and publishes a
 * {@link TopicConfigChanged} event. The {@link com.joxette.recording.RecordingConfigWatcher}
 * on every recording-enabled node receives the event and reconciles its local
 * Kafka consumers — no imperative start/stop calls needed here.
 *
 * <pre>
 * GET    /topics                  list all configured topics
 * POST   /topics                  add a topic (starts recording on all capable nodes)
 * GET    /topics/{topic}          get a single topic's config
 * PUT    /topics/{topic}          update mode
 * DELETE /topics/{topic}          remove topic and stop recording
 * POST   /topics/{topic}/pause    stop recording, keep config
 * POST   /topics/{topic}/resume   restart recording
 * POST   /topics/{topic}/restart  force restart on all recording nodes
 * </pre>
 */
@RestController
@RequestMapping("/topics")
public class TopicController {

    private static final Logger log = LoggerFactory.getLogger(TopicController.class);

    private final ConfigRepository config;
    private final RecordingCoordinator coordinator;
    private final KafkaTopicAdmin kafkaTopicAdmin;
    private final ConfigEventBus eventBus;

    public record CreateTopicRequest(
            @NotBlank String topic,
            String mode,
            String startFrom,
            String brokerId,
            Boolean createKafkaTopicIfAbsent,
            Integer numPartitions,
            Short replicationFactor
    ) {}
    public record UpdateTopicRequest(String mode, String brokerId) {}
    public record SetRetentionRequest(Integer days) {}
    public record AddMatcherRequest(@NotBlank String messageType, String idSource, String idExpression) {}

    public TopicController(ConfigRepository config,
                           @Lazy RecordingCoordinator coordinator,
                           KafkaTopicAdmin kafkaTopicAdmin,
                           ConfigEventBus eventBus) {
        this.config          = config;
        this.coordinator     = coordinator;
        this.kafkaTopicAdmin = kafkaTopicAdmin;
        this.eventBus        = eventBus;
    }

    private void publish(String topic, String changeType) {
        eventBus.publishTopicConfig(new TopicConfigChanged(topic, changeType));
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public List<TopicConfig> listTopics() throws SQLException {
        Map<String, RecorderStatus> running = coordinator.listRunning();
        return config.listTopics().stream()
                .map(tc -> new TopicConfig(tc.topic(), tc.mode(), tc.paused(),
                        running.containsKey(tc.topic()), tc.retentionDays(), tc.startFrom(), tc.brokerId()))
                .toList();
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<TopicConfig> createTopic(@Valid @RequestBody CreateTopicRequest body)
            throws SQLException {
        if (config.findTopic(body.topic()).isPresent()) {
            throw ConflictException.topicAlreadyExists(body.topic());
        }
        if (Boolean.TRUE.equals(body.createKafkaTopicIfAbsent())) {
            String brokerId = body.brokerId();
            if (!kafkaTopicAdmin.exists(brokerId, body.topic())) {
                kafkaTopicAdmin.createTopic(brokerId, body.topic(),
                        body.numPartitions(), body.replicationFactor());
            } else {
                log.info("Kafka topic '{}' already exists on broker '{}', skipping creation",
                        body.topic(), brokerId);
            }
        }
        String mode = body.mode() != null ? body.mode() : "general";
        String startFrom = body.startFrom() != null ? body.startFrom() : "latest";
        TopicConfig tc = config.upsertTopic(body.topic(), mode, false, startFrom, body.brokerId());
        publish(tc.topic(), "created");
        // Report active=false initially — watcher will start recorders asynchronously
        return ResponseEntity.status(201).body(
                new TopicConfig(tc.topic(), tc.mode(), tc.paused(), false,
                        tc.retentionDays(), tc.startFrom(), tc.brokerId()));
    }

    @GetMapping(value = "/{topic}", produces = MediaType.APPLICATION_JSON_VALUE)
    public TopicConfig getTopic(@PathVariable String topic) throws SQLException {
        return config.findTopic(topic)
                .orElseThrow(() -> ResourceNotFoundException.topic(topic));
    }

    @PutMapping(value = "/{topic}",
                consumes = MediaType.APPLICATION_JSON_VALUE,
                produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<TopicConfig> updateTopic(
            @PathVariable String topic,
            @Valid @RequestBody UpdateTopicRequest body) throws SQLException {
        config.findTopic(topic).orElseThrow(() -> ResourceNotFoundException.topic(topic));
        TopicConfig existing = config.findTopic(topic)
                .orElseThrow(() -> ResourceNotFoundException.topic(topic));
        TopicConfig tc = config.upsertTopic(topic, body.mode(), existing.paused(), "latest", body.brokerId());
        publish(topic, "updated");
        return ResponseEntity.ok(tc);
    }

    @DeleteMapping("/{topic}")
    public ResponseEntity<Void> deleteTopic(@PathVariable String topic) throws SQLException {
        boolean deleted = config.deleteTopic(topic);
        if (!deleted) {
            throw ResourceNotFoundException.topic(topic);
        }
        publish(topic, "deleted");
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{topic}/pause")
    public TopicConfig pauseTopic(@PathVariable String topic) throws SQLException {
        config.findTopic(topic).orElseThrow(() -> ResourceNotFoundException.topic(topic));
        config.setPaused(topic, true);
        publish(topic, "paused");
        return config.findTopic(topic).orElseThrow(() -> ResourceNotFoundException.topic(topic));
    }

    @PutMapping(value = "/{topic}/retention",
                consumes = MediaType.APPLICATION_JSON_VALUE,
                produces = MediaType.APPLICATION_JSON_VALUE)
    public TopicConfig setRetention(
            @PathVariable String topic,
            @Valid @RequestBody SetRetentionRequest body) throws SQLException {
        config.findTopic(topic).orElseThrow(() -> ResourceNotFoundException.topic(topic));
        return config.setTopicRetentionDays(topic, body.days());
    }

    @PostMapping("/{topic}/resume")
    public TopicConfig resumeTopic(@PathVariable String topic) throws SQLException {
        config.findTopic(topic).orElseThrow(() -> ResourceNotFoundException.topic(topic));
        config.setPaused(topic, false);
        publish(topic, "resumed");
        return config.findTopic(topic).orElseThrow(() -> ResourceNotFoundException.topic(topic));
    }

    @PostMapping("/{topic}/restart")
    public ResponseEntity<RecorderStatus> restartTopic(@PathVariable String topic) throws SQLException {
        config.findTopic(topic).orElseThrow(() -> ResourceNotFoundException.topic(topic));
        // Restart is still imperative on this node; watcher reconciliation on other nodes
        // will handle their own consumers based on the unchanged catalog state.
        coordinator.restartTopic(topic);
        RecorderStatus status = coordinator.listRunning().get(topic);
        return status != null ? ResponseEntity.ok(status) : ResponseEntity.accepted().build();
    }

    @GetMapping(value = "/status", produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, RecorderStatus> recorderStatus() {
        return coordinator.listRunning();
    }

    // -------------------------------------------------------------------------
    // Message-type matchers (general cassette tagging)
    // -------------------------------------------------------------------------

    @GetMapping(value = "/{topic}/matchers", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<TopicMatcherConfig> listMatchers(@PathVariable String topic) throws SQLException {
        config.findTopic(topic).orElseThrow(() -> ResourceNotFoundException.topic(topic));
        return config.listTopicMatchers(topic);
    }

    @PostMapping(value = "/{topic}/matchers",
                 consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<TopicMatcherConfig> addMatcher(
            @PathVariable String topic,
            @Valid @RequestBody AddMatcherRequest body) throws SQLException {
        config.findTopic(topic).orElseThrow(() -> ResourceNotFoundException.topic(topic));
        TopicMatcherConfig m = config.upsertTopicMatcher(
                topic, body.messageType(), body.idSource(), body.idExpression());
        publish(topic, "updated");
        return ResponseEntity.status(201).body(m);
    }

    @DeleteMapping("/{topic}/matchers/{messageType}")
    public ResponseEntity<Void> deleteMatcher(
            @PathVariable String topic,
            @PathVariable String messageType) throws SQLException {
        boolean deleted = config.deleteTopicMatcher(topic, messageType);
        if (!deleted) {
            throw ResourceNotFoundException.topicMatcher(topic, messageType);
        }
        publish(topic, "updated");
        return ResponseEntity.noContent().build();
    }
}
