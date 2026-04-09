package com.joxette.management;

import com.joxette.recording.RecordingCoordinator;
import com.joxette.replay.MessageRouter;
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

    private final ConfigRepository config;
    private final RecordingCoordinator coordinator;
    private final MessageRouter router;

    record CreateTopicRequest(String topic, String mode) {}
    record UpdateTopicRequest(String mode) {}

    public TopicController(ConfigRepository config, @Lazy RecordingCoordinator coordinator,
                           MessageRouter router) {
        this.config      = config;
        this.coordinator = coordinator;
        this.router      = router;
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public List<TopicConfig> listTopics() throws SQLException {
        Set<String> active = coordinator.activeTopics();
        return config.listTopics().stream()
                .map(tc -> new TopicConfig(tc.topic(), tc.mode(), tc.paused(), active.contains(tc.topic())))
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
        TopicConfig tc = config.upsertTopic(body.topic(), mode, false);
        coordinator.startTopic(tc.topic());
        boolean active = coordinator.activeTopics().contains(tc.topic());
        return ResponseEntity.status(201).body(new TopicConfig(tc.topic(), tc.mode(), tc.paused(), active));
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
        router.reload();
        return ResponseEntity.ok(tc);
    }

    @DeleteMapping("/{topic}")
    public ResponseEntity<Void> deleteTopic(@PathVariable String topic) throws SQLException {
        coordinator.stopTopic(topic);
        boolean deleted = config.deleteTopic(topic);
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

    @PostMapping("/{topic}/resume")
    public ResponseEntity<TopicConfig> resumeTopic(@PathVariable String topic) throws SQLException {
        if (config.findTopic(topic).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        config.setPaused(topic, false);
        coordinator.startTopic(topic);
        return config.findTopic(topic).map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
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
