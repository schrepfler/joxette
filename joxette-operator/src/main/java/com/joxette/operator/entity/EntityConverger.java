package com.joxette.operator.entity;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.joxette.operator.rest.JoxetteRestClient;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Reconciles one {@link EntityTypeSpec} into a cluster's {@code /entities} REST
 * API: create/update the entity type, then upsert each declared source mapping
 * (server-side {@code POST .../sources} is an upsert). Pure-ish — only talks to
 * the injected {@link JoxetteRestClient} — so it is testable against a stub HTTP
 * server without a Kubernetes context.
 */
public class EntityConverger {

    private final JoxetteRestClient client;

    public EntityConverger(JoxetteRestClient client) {
        this.client = client;
    }

    /** @return number of source mappings applied (for status.sourceCount). */
    public int converge(EntityTypeSpec spec) {
        Optional<ObjectNode> current = client.getJson("/entities/" + spec.getType());
        if (current.isEmpty()) {
            requireCreate(client.postJson("/entities", createBody(spec)), spec.getType());
        } else if (bucketsDrifted(current.get(), spec)) {
            requireOk(client.putJson("/entities/" + spec.getType(), updateBody(spec)), spec.getType());
        }
        // Upsert each source (idempotent server-side).
        for (EntityTypeSpec.Source source : spec.getSources()) {
            requireCreate(
                    client.postJson("/entities/" + spec.getType() + "/sources", sourceBody(source)),
                    spec.getType());
        }
        return spec.getSources().size();
    }

    /** Applies the deletion policy on CR removal. */
    public JoxetteRestClient.ChangeResult onDelete(EntityTypeSpec spec) {
        if (spec.getDeletionPolicy() == EntityTypeSpec.DeletionPolicy.Delete) {
            client.delete("/entities/" + spec.getType());
            return JoxetteRestClient.ChangeResult.DELETED;
        }
        return JoxetteRestClient.ChangeResult.UNCHANGED; // Orphan
    }

    static boolean bucketsDrifted(ObjectNode current, EntityTypeSpec spec) {
        int live = current.hasNonNull("buckets") ? current.get("buckets").asInt() : -1;
        return live != spec.getBuckets();
    }

    static Map<String, Object> createBody(EntityTypeSpec spec) {
        Map<String, Object> b = new LinkedHashMap<>();
        b.put("type", spec.getType());
        b.put("buckets", spec.getBuckets());
        return b;
    }

    static Map<String, Object> updateBody(EntityTypeSpec spec) {
        Map<String, Object> b = new LinkedHashMap<>();
        b.put("buckets", spec.getBuckets());
        return b;
    }

    static Map<String, Object> sourceBody(EntityTypeSpec.Source source) {
        Map<String, Object> b = new LinkedHashMap<>();
        b.put("topic", source.getTopic());
        b.put("mode", source.getMode());
        List<Map<String, Object>> matchers = new ArrayList<>();
        for (EntityTypeSpec.Matcher m : source.getMatchers()) {
            Map<String, Object> mb = new LinkedHashMap<>();
            mb.put("messageType", m.getMessageType());
            mb.put("idSource", m.getIdSource());
            mb.put("idExpression", m.getIdExpression());
            matchers.add(mb);
        }
        b.put("matchers", matchers);
        return b;
    }

    private void requireCreate(int sc, String type) {
        if (sc != 201 && sc != 200 && sc != 409) {
            throw new com.joxette.operator.rest.JoxetteRestException(
                    "POST /entities (type '" + type + "') -> HTTP " + sc);
        }
    }

    private void requireOk(int sc, String type) {
        if (sc / 100 != 2) {
            throw new com.joxette.operator.rest.JoxetteRestException(
                    "PUT /entities/" + type + " -> HTTP " + sc);
        }
    }
}
