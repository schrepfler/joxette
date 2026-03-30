package com.joxette.management;

import com.joxette.replay.SchemaManager;
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
 */
@RestController
@RequestMapping("/entities")
public class EntityController {

    private final ConfigRepository config;
    private final SchemaManager schemaManager;

    record CreateEntityRequest(String type, int buckets) {}
    record UpdateEntityRequest(int buckets) {}
    record AddSourceRequest(String topic, String idSource, String idExpression) {}

    public EntityController(ConfigRepository config, SchemaManager schemaManager) {
        this.config        = config;
        this.schemaManager = schemaManager;
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
        return ResponseEntity.ok(config.upsertEntityType(type, body.buckets()));
    }

    @DeleteMapping("/{type}")
    public ResponseEntity<Void> deleteEntityType(@PathVariable String type) throws SQLException {
        boolean deleted = config.deleteEntityType(type);
        if (deleted) {
            schemaManager.dropEntityTable(type);
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
        String idSource = body.idSource() != null ? body.idSource() : "value";
        EntitySourceConfig src = config.upsertSource(type, body.topic(), idSource, body.idExpression());
        return ResponseEntity.status(201).body(src);
    }

    @DeleteMapping("/{type}/sources/{topic}")
    public ResponseEntity<Void> deleteSource(
            @PathVariable String type,
            @PathVariable String topic) throws SQLException {
        boolean deleted = config.deleteSource(type, topic);
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
