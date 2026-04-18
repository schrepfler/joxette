package com.joxette.management;

import com.joxette.db.SchemaManager;
import com.joxette.replay.MessageRouter;
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
 *
 * <p>Every mutating operation ({@code POST}, {@code PUT}, {@code DELETE}) calls
 * {@link MessageRouter#reload()} so the in-process recording pipeline picks up
 * the change immediately without a restart.
 */
@RestController
@RequestMapping("/entities")
public class EntityController {

    private static final Logger log = LoggerFactory.getLogger(EntityController.class);

    private final ConfigRepository config;
    private final SchemaManager schemaManager;
    private final MessageRouter messageRouter;

    record CreateEntityRequest(String type, int buckets) {}
    record UpdateEntityRequest(int buckets) {}
    record AddSourceRequest(
            String topic,
            String mode,
            List<EntitySourceConfig.MatcherConfig> matchers
    ) {}
    record SetRetentionRequest(Integer days) {}

    public EntityController(ConfigRepository config, SchemaManager schemaManager,
                            MessageRouter messageRouter) {
        this.config        = config;
        this.schemaManager = schemaManager;
        this.messageRouter = messageRouter;
    }

    // -------------------------------------------------------------------------
    // Helper — reload router after any mutation and swallow non-fatal errors
    // -------------------------------------------------------------------------

    private void reloadRouter() {
        try {
            messageRouter.reload();
        } catch (SQLException e) {
            log.warn("MessageRouter reload failed after config change: {}", e.getMessage());
        }
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public List<EntityTypeConfig> listEntityTypes() throws SQLException {
        return config.listEntityTypes();
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<EntityTypeConfig> createEntityType(
            @RequestBody CreateEntityRequest body) throws SQLException {
        if (body.type() == null || body.type().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        // Validate the type name before touching the DB.
        SchemaManager.validateEntityType(body.type());
        if (config.findEntityType(body.type()).isPresent()) {
            return ResponseEntity.status(409).build();
        }
        int buckets = body.buckets() > 0 ? body.buckets() : 256;
        schemaManager.createEntityTable(body.type());
        EntityTypeConfig etc = config.upsertEntityType(body.type(), buckets);
        reloadRouter();
        return ResponseEntity.status(201).body(etc);
    }

    @GetMapping(value = "/{type}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<EntityTypeConfig> getEntityType(@PathVariable String type)
            throws SQLException {
        return config.findEntityType(type)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PutMapping(value = "/{type}",
                consumes = MediaType.APPLICATION_JSON_VALUE,
                produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<EntityTypeConfig> updateEntityType(
            @PathVariable String type,
            @RequestBody UpdateEntityRequest body) throws SQLException {
        if (config.findEntityType(type).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        EntityTypeConfig updated = config.upsertEntityType(type, body.buckets());
        reloadRouter();
        return ResponseEntity.ok(updated);
    }

    @PutMapping(value = "/{type}/retention",
                consumes = MediaType.APPLICATION_JSON_VALUE,
                produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<EntityTypeConfig> setRetention(
            @PathVariable String type,
            @RequestBody SetRetentionRequest body) throws SQLException {
        if (config.findEntityType(type).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(config.setEntityRetentionDays(type, body.days()));
    }

    @DeleteMapping("/{type}")
    public ResponseEntity<Void> deleteEntityType(@PathVariable String type) throws SQLException {
        boolean deleted = config.deleteEntityType(type);
        if (deleted) {
            schemaManager.dropEntityTable(type);
            reloadRouter();
        }
        return deleted ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }

    // -------------------------------------------------------------------------
    // Source mappings
    // -------------------------------------------------------------------------

    @GetMapping(value = "/{type}/sources", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<EntitySourceConfig>> listSources(@PathVariable String type)
            throws SQLException {
        if (config.findEntityType(type).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(config.listSources(type));
    }

    @PostMapping(value = "/{type}/sources",
                 consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<EntitySourceConfig> addSource(
            @PathVariable String type,
            @RequestBody AddSourceRequest body) throws SQLException {
        if (config.findEntityType(type).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        if (body.topic() == null || body.topic().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        EntitySourceConfig src = config.upsertSource(type, body.topic(), body.mode(), body.matchers());
        reloadRouter();
        return ResponseEntity.status(201).body(src);
    }

    @DeleteMapping("/{type}/sources/{topic}")
    public ResponseEntity<Void> deleteSource(
            @PathVariable String type,
            @PathVariable String topic) throws SQLException {
        boolean deleted = config.deleteSource(type, topic);
        if (deleted) {
            reloadRouter();
        }
        return deleted ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }

    // -------------------------------------------------------------------------
    // Per-matcher management  (POST/DELETE …/sources/{topic}/matchers)
    // -------------------------------------------------------------------------

    record AddMatcherRequest(String messageType, String idSource, String idExpression) {}

    @PostMapping(value = "/{type}/sources/{topic}/matchers",
                 consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<EntitySourceConfig.MatcherConfig> addMatcher(
            @PathVariable String type,
            @PathVariable String topic,
            @RequestBody AddMatcherRequest body) throws SQLException {
        if (config.findEntityType(type).isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        if (body.messageType() == null || body.messageType().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        EntitySourceConfig.MatcherConfig matcher = config.addEntityMatcher(
                type, topic,
                new EntitySourceConfig.MatcherConfig(body.messageType(), body.idSource(), body.idExpression()));
        reloadRouter();
        return ResponseEntity.status(201).body(matcher);
    }

    @DeleteMapping("/{type}/sources/{topic}/matchers/{messageType}")
    public ResponseEntity<Void> deleteMatcher(
            @PathVariable String type,
            @PathVariable String topic,
            @PathVariable String messageType) throws SQLException {
        boolean deleted = config.deleteEntityMatcher(type, topic, messageType);
        if (deleted) {
            reloadRouter();
        }
        return deleted ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }

    // -------------------------------------------------------------------------
    // Error handling
    // -------------------------------------------------------------------------

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> handleBadRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(ex.getMessage());
    }

    @ExceptionHandler(SQLException.class)
    public ResponseEntity<String> handleSqlError(SQLException ex) {
        return ResponseEntity.internalServerError().body("Database error: " + ex.getMessage());
    }
}
