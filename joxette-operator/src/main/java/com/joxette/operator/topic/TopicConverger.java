package com.joxette.operator.topic;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.joxette.operator.rest.JoxetteRestClient;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Reconciles one {@link RecordedTopicSpec} into a cluster's {@code /topics} REST
 * API by diffing desired vs current and issuing the minimal POST/PUT. Idempotent:
 * a no-op spec produces {@code UNCHANGED}.
 *
 * <p>Kept separate from the JOSDK reconciler so the converge decision is unit-
 * testable against a stub client without a Kubernetes context.
 */
public class TopicConverger {

    private final JoxetteRestClient client;

    public TopicConverger(JoxetteRestClient client) {
        this.client = client;
    }

    /** Creates the topic if absent, or updates it if mode/brokerId drifted. */
    public JoxetteRestClient.ChangeResult converge(RecordedTopicSpec spec) {
        Optional<ObjectNode> current = client.getJson("/topics/" + spec.getTopic());
        if (current.isEmpty()) {
            int sc = client.postJson("/topics", createBody(spec));
            requireCreate(sc, spec.getTopic());
            return JoxetteRestClient.ChangeResult.CREATED;
        }
        if (drifted(current.get(), spec)) {
            int sc = client.putJson("/topics/" + spec.getTopic(), updateBody(spec));
            requireOk(sc, spec.getTopic());
            return JoxetteRestClient.ChangeResult.UPDATED;
        }
        return JoxetteRestClient.ChangeResult.UNCHANGED;
    }

    /** Applies the deletion policy on CR removal. */
    public JoxetteRestClient.ChangeResult onDelete(RecordedTopicSpec spec) {
        return switch (spec.getDeletionPolicy()) {
            case Delete -> {
                client.delete("/topics/" + spec.getTopic());
                yield JoxetteRestClient.ChangeResult.DELETED;
            }
            case Pause -> {
                int sc = client.post("/topics/" + spec.getTopic() + "/pause");
                // 404 → already gone; treat as no-op.
                yield sc == 404 ? JoxetteRestClient.ChangeResult.NOT_FOUND
                        : JoxetteRestClient.ChangeResult.UPDATED;
            }
            case Orphan -> JoxetteRestClient.ChangeResult.UNCHANGED;
        };
    }

    /** True if the live topic's mode or brokerId differs from the spec. */
    static boolean drifted(ObjectNode current, RecordedTopicSpec spec) {
        String liveMode = text(current, "mode");
        String liveBroker = text(current, "brokerId");
        boolean modeDrift = spec.getMode() != null && !spec.getMode().equals(liveMode);
        boolean brokerDrift = spec.getBrokerId() != null && !spec.getBrokerId().equals(liveBroker);
        return modeDrift || brokerDrift;
    }

    static Map<String, Object> createBody(RecordedTopicSpec spec) {
        Map<String, Object> b = new LinkedHashMap<>();
        b.put("topic", spec.getTopic());
        b.put("mode", spec.getMode());
        b.put("startFrom", spec.getStartFrom());
        if (spec.getBrokerId() != null) {
            b.put("brokerId", spec.getBrokerId());
        }
        return b;
    }

    static Map<String, Object> updateBody(RecordedTopicSpec spec) {
        Map<String, Object> b = new LinkedHashMap<>();
        b.put("mode", spec.getMode());
        if (spec.getBrokerId() != null) {
            b.put("brokerId", spec.getBrokerId());
        }
        return b;
    }

    private static String text(ObjectNode n, String field) {
        return n.hasNonNull(field) ? n.get(field).asText() : null;
    }

    private void requireCreate(int sc, String topic) {
        // 201 expected; 409 means it was created concurrently — also fine.
        if (sc != 201 && sc != 200 && sc != 409) {
            throw new com.joxette.operator.rest.JoxetteRestException(
                    "POST /topics for '" + topic + "' -> HTTP " + sc);
        }
    }

    private void requireOk(int sc, String topic) {
        if (sc / 100 != 2) {
            throw new com.joxette.operator.rest.JoxetteRestException(
                    "PUT /topics/" + topic + " -> HTTP " + sc);
        }
    }
}
