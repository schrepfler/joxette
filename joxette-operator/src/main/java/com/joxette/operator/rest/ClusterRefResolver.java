package com.joxette.operator.rest;

/**
 * Resolves a {@code clusterRef} (a JoxetteCluster name in the CR's namespace) to
 * the base URL of that cluster's REST API — the in-cluster Service DNS the
 * JoxetteCluster reconciler creates: {@code http://<name>.<namespace>.svc:8080}.
 */
public final class ClusterRefResolver {

    private static final int HTTP_PORT = 8080;

    private ClusterRefResolver() {
    }

    /** @return base URL, e.g. {@code http://prod.joxette.svc:8080}. */
    public static String baseUrl(String clusterName, String namespace) {
        if (clusterName == null || clusterName.isBlank()) {
            throw new IllegalArgumentException("clusterRef.name is required");
        }
        return "http://" + clusterName + "." + namespace + ".svc:" + HTTP_PORT;
    }
}
