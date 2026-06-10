package com.joxette.operator.cluster;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

/**
 * {@code JoxetteCluster} custom resource — one Joxette installation.
 *
 * <p>Owns the workloads (StatefulSet for an embedded catalog, or per-tier
 * Deployments for a shared catalog), the Service, ConfigMap, ServiceAccount, and
 * (in {@code pekko-management} mode) the pod RBAC. The reconciler enforces the
 * catalog single-writer guardrail — see {@link JoxetteClusterReconciler}.
 *
 * <p>API group/version: {@code joxette.dev/v1alpha1}.
 */
@Group("joxette.dev")
@Version("v1alpha1")
@Kind("JoxetteCluster")
@ShortNames("jox")
public class JoxetteCluster
        extends CustomResource<JoxetteClusterSpec, JoxetteClusterStatus>
        implements Namespaced {
}
