package com.joxette.management;

import com.joxette.api.error.ConflictException;
import com.joxette.api.error.ResourceNotFoundException;
import com.joxette.recording.RecorderStatus;
import com.joxette.recording.RecordingCoordinator;
import com.joxette.replay.MessageRouter;
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
 * <pre>
 * GET    /topics                  list all configured topics
 * POST   /topics                  add a topic (starts recording immediately)
 * GET    /topics/{topic}          get a single topic's config
 * PUT    /topics/{topic}          update mode
 * DELETE /topics/{topic}          remove topic and stop recording
 * POST   /topics/{topic}/pause    stop recording, keep config
 * POST   /topics/{topic}/resume   restart recording
 * </pre>
 */
@RestController
@RequestMapping("/topics")
public class TopicController {

    private static final Logger log = LoggerFactory.getLogger(TopicController.class);

    private final ConfigRepository config;
    private final RecordingCoordinator coordinator;
    private final MessageRouter router;
    private final KafkaTopicAdmin kafkaTopicAdmin;

    public record CreateTopicRequest(
            @NotBlank String topic,
            String mode,
            String startFrom,
            String brokerId,
            /** When {@code true}, create the Kafka topic if it does not yet exist. */
            Boolean createKafkaTopicIfAbsent,
            /** Number of partitions for the new Kafka topic. Ignored when {@code createKafkaTopicIfAbsent} is false. */
            Integer numPartitions,
            /** Replication factor for the new Kafka topic. Ignored when {@code createKafkaTopicIfAbsent} is false. */
            Short replicationFactor
    ) {}
    public record UpdateTopicRequest(String mode, String brokerId) {}
    public record SetRetentionRequest(Integer days) {}
    public record AddMatcherRequest(@NotBlank String messageType, String idSource, String idExpression) {}

    public TopicController(ConfigRepository config, @Lazy RecordingCoordinator coordinator,
                           MessageRouter router, KafkaTopicAdmin kafkaTopicAdmin) {
        this.config          = config;
        this.coordinator     = coordinator;
        this.router          = router;
        this.kafkaTopicAdmin = kafkaTopicAdmin;
    }

    private void reloadRouter() {
        try {
            router.reload();
        } catch (SQLException e) {
            log.warn("MessageRouter reload failed after topic config change: {}", e.getMessage());
        }
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
                // ForbiddenException propagates to GlobalExceptionHandler as 403 if ACLs deny CREATE.
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
        coordinator.startTopic(tc.topic(), tc.startFrom());
        reloadRouter();
        boolean active = coordinator.activeTopics().contains(tc.topic());
        return ResponseEntity.status(201).body(
                new TopicConfig(tc.topic(), tc.mode(), tc.paused(), active, tc.retentionDays(), tc.startFrom(), tc.brokerId()));
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
        TopicConfig existing = config.findTopic(topic)
                .orElseThrow(() -> ResourceNotFoundException.topic(topic));
        TopicConfig tc = config.upsertTopic(topic, body.mode(), existing.paused(), "latest", body.brokerId());
        reloadRouter();
        return ResponseEntity.ok(tc);
    }

    @DeleteMapping("/{topic}")
    public ResponseEntity<Void> deleteTopic(@PathVariable String topic) throws SQLException {
        coordinator.stopTopic(topic);
        boolean deleted = config.deleteTopic(topic);
        if (!deleted) {
            throw ResourceNotFoundException.topic(topic);
        }
        reloadRouter();
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{topic}/pause")
    public TopicConfig pauseTopic(@PathVariable String topic) throws SQLException {
        config.findTopic(topic).orElseThrow(() -> ResourceNotFoundException.topic(topic));
        coordinator.stopTopic(topic);
        config.setPaused(topic, true);
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
        config.findTopic(topic).ifPresent(tc -> coordinator.startTopic(topic, tc.startFrom()));
        return config.findTopic(topic).orElseThrow(() -> ResourceNotFoundException.topic(topic));
    }

    @PostMapping("/{topic}/restart")
    public ResponseEntity<RecorderStatus> restartTopic(@PathVariable String topic) throws SQLException {
        config.findTopic(topic).orElseThrow(() -> ResourceNotFoundException.topic(topic));
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
        reloadRouter();
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
        reloadRouter();
        return ResponseEntity.noContent().build();
    }
}
