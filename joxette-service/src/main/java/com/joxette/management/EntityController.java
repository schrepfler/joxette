package com.joxette.management;

import com.joxette.api.error.ConflictException;
import com.joxette.api.error.ResourceNotFoundException;
import com.joxette.config.events.ConfigEventBus;
import com.joxette.config.events.EntityConfigChanged;
import com.joxette.db.SchemaManager;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.List;

/**
 * Management API for entity-type configuration and source mappings.
 *
 * <p>Every mutating operation publishes an {@link EntityConfigChanged} event.
 * The {@link com.joxette.recording.EntityConfigWatcher} on every node receives it
 * and reloads the local {@link com.joxette.replay.MessageRouter} routing tables
 * immediately — no direct router reload needed here.
 *
 * <pre>
 * GET    /entities                      list all entity types
 * POST   /entities                      create entity type (creates DuckDB table)
 * GET    /entities/{type}               get entity type config
 * PUT    /entities/{type}               update bucket count
 * DELETE /entities/{type}               delete entity type and drop its table
 * GET    /entities/{type}/sources       list source-topic mappings
 * POST   /entities/{type}/sources       add / update a source-topic mapping
 * DELETE /entities/{type}/sources/{t}   remove a source-topic mapping
 * </pre>
 */
@RestController
@RequestMapping("/entities")
public class EntityController {

    private static final Logger log = LoggerFactory.getLogger(EntityController.class);

    private final ConfigRepository config;
    private final SchemaManager schemaManager;
    private final ConfigEventBus eventBus;

    public record CreateEntityRequest(@NotBlank String type, int buckets) {}
    public record UpdateEntityRequest(int buckets) {}
    public record AddSourceRequest(
            @NotBlank String topic,
            String mode,
            List<EntitySourceConfig.MatcherConfig> matchers
    ) {}
    public record SetRetentionRequest(Integer days) {}
    public record AddMatcherRequest(@NotBlank String messageType, String idSource, String idExpression) {}

    public EntityController(ConfigRepository config, SchemaManager schemaManager,
                            ConfigEventBus eventBus) {
        this.config        = config;
        this.schemaManager = schemaManager;
        this.eventBus      = eventBus;
    }

    private void publish(String entityType, String changeType) {
        eventBus.publishEntityConfig(new EntityConfigChanged(entityType, changeType));
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public List<EntityTypeConfig> listEntityTypes() throws SQLException {
        return config.listEntityTypes();
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<EntityTypeConfig> createEntityType(
            @Valid @RequestBody CreateEntityRequest body) throws SQLException {
        SchemaManager.validateEntityType(body.type());
        if (config.findEntityType(body.type()).isPresent()) {
            throw ConflictException.entityTypeAlreadyExists(body.type());
        }
        int buckets = body.buckets() > 0 ? body.buckets() : 256;
        schemaManager.createEntityTable(body.type());
        EntityTypeConfig etc = config.upsertEntityType(body.type(), buckets);
        publish(body.type(), "created");
        return ResponseEntity.status(201).body(etc);
    }

    @GetMapping(value = "/{type}", produces = MediaType.APPLICATION_JSON_VALUE)
    public EntityTypeConfig getEntityType(@PathVariable String type) throws SQLException {
        return config.findEntityType(type)
                .orElseThrow(() -> ResourceNotFoundException.entityType(type));
    }

    @PutMapping(value = "/{type}",
                consumes = MediaType.APPLICATION_JSON_VALUE,
                produces = MediaType.APPLICATION_JSON_VALUE)
    public EntityTypeConfig updateEntityType(
            @PathVariable String type,
            @Valid @RequestBody UpdateEntityRequest body) throws SQLException {
        config.findEntityType(type).orElseThrow(() -> ResourceNotFoundException.entityType(type));
        EntityTypeConfig updated = config.upsertEntityType(type, body.buckets());
        publish(type, "updated");
        return updated;
    }

    @PutMapping(value = "/{type}/retention",
                consumes = MediaType.APPLICATION_JSON_VALUE,
                produces = MediaType.APPLICATION_JSON_VALUE)
    public EntityTypeConfig setRetention(
            @PathVariable String type,
            @Valid @RequestBody SetRetentionRequest body) throws SQLException {
        config.findEntityType(type).orElseThrow(() -> ResourceNotFoundException.entityType(type));
        return config.setEntityRetentionDays(type, body.days());
    }

    @DeleteMapping("/{type}")
    public ResponseEntity<Void> deleteEntityType(@PathVariable String type) throws SQLException {
        boolean deleted = config.deleteEntityType(type);
        if (!deleted) {
            throw ResourceNotFoundException.entityType(type);
        }
        schemaManager.dropEntityTable(type);
        publish(type, "deleted");
        return ResponseEntity.noContent().build();
    }

    // -------------------------------------------------------------------------
    // Source mappings
    // -------------------------------------------------------------------------

    @GetMapping(value = "/{type}/sources", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<EntitySourceConfig> listSources(@PathVariable String type) throws SQLException {
        config.findEntityType(type).orElseThrow(() -> ResourceNotFoundException.entityType(type));
        return config.listSources(type);
    }

    @PostMapping(value = "/{type}/sources",
                 consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<EntitySourceConfig> addSource(
            @PathVariable String type,
            @Valid @RequestBody AddSourceRequest body) throws SQLException {
        config.findEntityType(type).orElseThrow(() -> ResourceNotFoundException.entityType(type));
        EntitySourceConfig src = config.upsertSource(type, body.topic(), body.mode(), body.matchers());
        publish(type, "source_added");
        return ResponseEntity.status(201).body(src);
    }

    @DeleteMapping("/{type}/sources/{topic}")
    public ResponseEntity<Void> deleteSource(
            @PathVariable String type,
            @PathVariable String topic) throws SQLException {
        boolean deleted = config.deleteSource(type, topic);
        if (!deleted) {
            throw ResourceNotFoundException.entitySource(type, topic);
        }
        publish(type, "source_deleted");
        return ResponseEntity.noContent().build();
    }

    // -------------------------------------------------------------------------
    // Per-matcher management  (POST/DELETE …/sources/{topic}/matchers)
    // -------------------------------------------------------------------------

    @PostMapping(value = "/{type}/sources/{topic}/matchers",
                 consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<EntitySourceConfig.MatcherConfig> addMatcher(
            @PathVariable String type,
            @PathVariable String topic,
            @Valid @RequestBody AddMatcherRequest body) throws SQLException {
        config.findEntityType(type).orElseThrow(() -> ResourceNotFoundException.entityType(type));
        EntitySourceConfig.MatcherConfig matcher = config.addEntityMatcher(
                type, topic,
                new EntitySourceConfig.MatcherConfig(body.messageType(), body.idSource(), body.idExpression()));
        publish(type, "source_added");
        return ResponseEntity.status(201).body(matcher);
    }

    @DeleteMapping("/{type}/sources/{topic}/matchers/{messageType}")
    public ResponseEntity<Void> deleteMatcher(
            @PathVariable String type,
            @PathVariable String topic,
            @PathVariable String messageType) throws SQLException {
        boolean deleted = config.deleteEntityMatcher(type, topic, messageType);
        if (!deleted) {
            throw ResourceNotFoundException.entityMatcher(type, topic, messageType);
        }
        publish(type, "source_deleted");
        return ResponseEntity.noContent().build();
    }
}
