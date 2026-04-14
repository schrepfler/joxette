package com.joxette.management;

import com.joxette.recording.RecordingCoordinator;
import com.joxette.replay.MessageRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;


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

    record CreateTopicRequest(String topic, String mode, String startFrom) {}
    record UpdateTopicRequest(String mode) {}
    record SetRetentionRequest(Integer days) {}
    record AddMatcherRequest(String messageType, String idSource, String idExpression) {}

    public TopicController(ConfigRepository config, @Lazy RecordingCoordinator coordinator,
                           MessageRouter router) {
        this.config      = config;
        this.coordinator = coordinator;
        this.router      = router;
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
        Set<String> active = coordinator.activeTopics();
        return config.listTopics().stream()
                .map(tc -> new TopicConfig(tc.topic(), tc.mode(), tc.paused(),
                        active.contains(tc.topic()), tc.retentionDays(), tc.startFrom(), tc.brokerId()))
                .toList();
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<TopicConfig> createTopic(@RequestBody CreateTopicRequest body)
            throws SQLException {
        if (body.topic() == null || body.topic().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        if (config.findTopic(body.topic()).isPresent()) {
            return ResponseEntity.status(409).build();
        }
        String mode = body.mode() != null ? body.mode() : "general";
        String startFrom = body.startFrom() != null ? body.startFrom() : "latest";
        TopicConfig tc = config.upsertTopic(body.topic(), mode, false, startFrom);
        coordinator.startTopic(tc.topic(), tc.startFrom());
        reloadRouter();
        boolean active = coordinator.activeTopics().contains(tc.topic());
        return ResponseEntity.status(201).body(
                new TopicConfig(tc.topic(), tc.mode(), tc.paused(), active, tc.retentionDays(), tc.startFrom(), tc.brokerId()));
    }

    @GetMapping(value = "/{topic}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<TopicConfig> getTopic(@PathVariable String topic) throws SQLException {
        return config.findTopic(topic)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PutMapping(value = "/{topic}",
                consumes = MediaType.APPLICATION_JSON_VALUE,
                produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<TopicConfig> updateTopic(
            @PathVariable String topic,
            @RequestBody UpdateTopicRequest body) throws SQLException {
        if (config.findTopic(topic).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        // Preserve current paused state
        boolean paused = config.findTopic(topic).map(TopicConfig::paused).orElse(false);
        TopicConfig tc = config.upsertTopic(topic, body.mode(), paused);
        reloadRouter();
        return ResponseEntity.ok(tc);
    }

    @DeleteMapping("/{topic}")
    public ResponseEntity<Void> deleteTopic(@PathVariable String topic) throws SQLException {
        coordinator.stopTopic(topic);
        boolean deleted = config.deleteTopic(topic);
        if (deleted) {
            reloadRouter();
        }
        return deleted ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }

    @PostMapping("/{topic}/pause")
    public ResponseEntity<TopicConfig> pauseTopic(@PathVariable String topic) throws SQLException {
        if (config.findTopic(topic).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        coordinator.stopTopic(topic);
        config.setPaused(topic, true);
        return config.findTopic(topic).map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PutMapping(value = "/{topic}/retention",
                consumes = MediaType.APPLICATION_JSON_VALUE,
                produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<TopicConfig> setRetention(
            @PathVariable String topic,
            @RequestBody SetRetentionRequest body) throws SQLException {
        if (config.findTopic(topic).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(config.setTopicRetentionDays(topic, body.days()));
    }

    @PostMapping("/{topic}/resume")
    public ResponseEntity<TopicConfig> resumeTopic(@PathVariable String topic) throws SQLException {
        if (config.findTopic(topic).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        config.setPaused(topic, false);
        config.findTopic(topic).ifPresent(tc -> coordinator.startTopic(topic, tc.startFrom()));
        return config.findTopic(topic).map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    // -------------------------------------------------------------------------
    // Message-type matchers (general cassette tagging)
    // -------------------------------------------------------------------------

    @GetMapping(value = "/{topic}/matchers", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<TopicMatcherConfig>> listMatchers(
            @PathVariable String topic) throws SQLException {
        if (config.findTopic(topic).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(config.listTopicMatchers(topic));
    }

    @PostMapping(value = "/{topic}/matchers",
                 consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<TopicMatcherConfig> addMatcher(
            @PathVariable String topic,
            @RequestBody AddMatcherRequest body) throws SQLException {
        if (config.findTopic(topic).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        if (body.messageType() == null || body.messageType().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
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
        if (deleted) {
            reloadRouter();
        }
        return deleted ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> handleBadRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(ex.getMessage());
    }

    @ExceptionHandler(SQLException.class)
    public ResponseEntity<String> handleSqlError(SQLException ex) {
        return ResponseEntity.internalServerError().body("Database error: " + ex.getMessage());
    }
}
