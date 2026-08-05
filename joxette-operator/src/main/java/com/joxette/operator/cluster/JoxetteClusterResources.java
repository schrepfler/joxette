package com.joxette.operator.cluster;

import com.joxette.operator.cluster.JoxetteClusterSpec.CatalogBackend;
import com.joxette.operator.cluster.JoxetteClusterSpec.ClusteringMode;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudgetBuilder;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the desired Kubernetes objects for a {@link JoxetteCluster}, mirroring the
 * Helm chart: embedded backend → one all-in-one {@link StatefulSet}; shared backend
 * → per-tier {@link Deployment}s. Plus a {@link Service} (and a headless Service in
 * pekko-management mode). Pure — no cluster access — so it is unit-testable.
 *
 * <p>Assumes the spec has already passed {@link CatalogGuardrail}.
 */
public final class JoxetteClusterResources {

    private static final int HTTP_PORT = 8080;

    private JoxetteClusterResources() {
    }

    public static List<HasMetadata> build(JoxetteCluster cluster) {
        JoxetteClusterSpec spec = cluster.getSpec();
        String name = cluster.getMetadata().getName();
        boolean embedded = spec.getCatalog().getBackend() == CatalogBackend.embedded;

        List<HasMetadata> out = new ArrayList<>();
        out.add(serviceAccount(name));
        if (spec.getClustering().getMode() == ClusteringMode.pekko_management) {
            // kubernetes-api discovery + lease-backed SBR need pod read + lease CRUD.
            out.add(pekkoRole(name));
            out.add(pekkoRoleBinding(name, cluster.getMetadata().getNamespace()));
        }
        out.add(mainService(name, spec));
        if (embedded) {
            out.add(headlessService(name, spec));
            out.add(embeddedStatefulSet(name, spec));
            out.add(embeddedPdb(name));
        } else {
            if (spec.getClustering().getMode() == ClusteringMode.pekko_management) {
                out.add(headlessService(name, spec));
            }
            if (spec.getTiers().getRecorder().isEnabled()) {
                out.add(tierDeployment(name, spec, "recorder",
                        List.of("recorder", "entity-router"), false,
                        spec.getTiers().getRecorder().getReplicas(),
                        spec.getTiers().getRecorder().getResources()));
            }
            if (spec.getTiers().getReplay().isEnabled()) {
                out.add(tierDeployment(name, spec, "replay",
                        List.of("replay"), true,
                        spec.getTiers().getReplay().getReplicas(),
                        spec.getTiers().getReplay().getResources()));
            }
            if (spec.getTiers().getCompaction().isEnabled()) {
                // Compaction is pinned to a single active node regardless of spec.
                out.add(tierDeployment(name, spec, "compaction",
                        List.of("compaction"), false, 1,
                        spec.getTiers().getCompaction().getResources()));
            }
        }
        return out;
    }

    // ---- workloads ----------------------------------------------------------

    private static StatefulSet embeddedStatefulSet(String name, JoxetteClusterSpec spec) {
        String dir = parentDir(spec.getCatalog().getEmbedded().getPath());
        var pvc = new PersistentVolumeClaimBuilder()
                .withNewMetadata().withName("catalog").endMetadata()
                .withNewSpec()
                .withAccessModes("ReadWriteOnce")
                .withStorageClassName(blankToNull(spec.getCatalog().getEmbedded().getStorageClass()))
                .withNewResources()
                .addToRequests("storage", new Quantity(spec.getCatalog().getEmbedded().getPvcSize()))
                .endResources()
                .endSpec()
                .build();

        return new StatefulSetBuilder()
                .withNewMetadata()
                .withName(name)
                .withLabels(labels(name))
                .endMetadata()
                .withNewSpec()
                .withServiceName(name + "-headless")
                .withReplicas(1)
                .withNewSelector().withMatchLabels(selector(name)).endSelector()
                .withNewTemplate()
                .withNewMetadata().withLabels(labels(name)).endMetadata()
                .withNewSpec()
                .withServiceAccountName(name)
                .addNewContainer()
                .withName("joxette")
                .withImage(spec.getImage())
                .withPorts(containerPorts(spec))
                .withEnv(env(name, spec, List.of("all"), true))
                .withStartupProbe(startupProbe())
                .withLivenessProbe(livenessProbe())
                .withReadinessProbe(readinessProbe())
                .withResources(resources(spec.getTiers().getRecorder().getResources()))
                .addNewVolumeMount().withName("catalog").withMountPath(dir).endVolumeMount()
                .endContainer()
                .endSpec()
                .endTemplate()
                .withVolumeClaimTemplates(pvc)
                .endSpec()
                .build();
    }

    /**
     * PodDisruptionBudget for the single-writer embedded catalog StatefulSet.
     * {@code maxUnavailable: 0} is intentional, not a percentage — the embedded
     * catalog always runs replicas:1 (see {@link CatalogGuardrail}: replicas>1
     * here is rejected outright), so a percentage-based budget is meaningless.
     * This states the real intent: never voluntarily evict the only pod.
     */
    private static PodDisruptionBudget embeddedPdb(String name) {
        return new PodDisruptionBudgetBuilder()
                .withNewMetadata().withName(name).withLabels(labels(name)).endMetadata()
                .withNewSpec()
                .withNewMaxUnavailable(0)
                .withNewSelector().withMatchLabels(selector(name)).endSelector()
                .endSpec()
                .build();
    }

    private static Deployment tierDeployment(String name, JoxetteClusterSpec spec,
                                             String tier, List<String> roles,
                                             boolean replayEnabled, int replicas,
                                             JoxetteClusterSpec.Resources tierResources) {
        Map<String, String> tierLabels = labels(name);
        tierLabels.put("app.kubernetes.io/component", tier);
        Map<String, String> tierSelector = selector(name);
        tierSelector.put("app.kubernetes.io/component", tier);

        return new DeploymentBuilder()
                .withNewMetadata()
                .withName(name + "-" + tier)
                .withLabels(tierLabels)
                .endMetadata()
                .withNewSpec()
                .withReplicas(replicas)
                .withNewSelector().withMatchLabels(tierSelector).endSelector()
                .withNewTemplate()
                .withNewMetadata().withLabels(tierLabels).endMetadata()
                .withNewSpec()
                .withServiceAccountName(name)
                .addNewContainer()
                .withName("joxette")
                .withImage(spec.getImage())
                .withPorts(containerPorts(spec))
                .withEnv(env(name, spec, roles, replayEnabled))
                .withStartupProbe(startupProbe())
                .withLivenessProbe(livenessProbe())
                .withReadinessProbe(readinessProbe())
                .withResources(resources(tierResources))
                .endContainer()
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();
    }

    // ---- probes + resources ---------------------------------------------------

    /** Mirrors deploy/helm/joxette/templates/_helpers.tpl `joxette.probes` (default probe paths/timings). */
    private static Probe startupProbe() {
        return new ProbeBuilder()
                .withNewHttpGet().withPath("/actuator/health").withPort(new IntOrString("http")).endHttpGet()
                .withFailureThreshold(30)
                .withPeriodSeconds(5)
                .build();
    }

    private static Probe livenessProbe() {
        return new ProbeBuilder()
                .withNewHttpGet().withPath("/actuator/health/liveness").withPort(new IntOrString("http")).endHttpGet()
                .withPeriodSeconds(15)
                .build();
    }

    private static Probe readinessProbe() {
        return new ProbeBuilder()
                .withNewHttpGet().withPath("/actuator/health/readiness").withPort(new IntOrString("http")).endHttpGet()
                .withPeriodSeconds(10)
                .build();
    }

    private static ResourceRequirements resources(JoxetteClusterSpec.Resources r) {
        return new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity(r.getRequestCpu()))
                .addToRequests("memory", new Quantity(r.getRequestMemory()))
                .addToLimits("memory", new Quantity(r.getLimitMemory()))
                .build();
    }

    // ---- service account + RBAC ---------------------------------------------

    private static ServiceAccount serviceAccount(String name) {
        return new ServiceAccountBuilder()
                .withNewMetadata().withName(name).withLabels(labels(name)).endMetadata()
                .build();
    }

    private static Role pekkoRole(String name) {
        return new RoleBuilder()
                .withNewMetadata().withName(name + "-pekko").withLabels(labels(name)).endMetadata()
                .addNewRule()
                .withApiGroups("")
                .withResources("pods")
                .withVerbs("get", "watch", "list")
                .endRule()
                .addNewRule()
                .withApiGroups("coordination.k8s.io")
                .withResources("leases")
                .withVerbs("get", "create", "update", "list", "watch", "delete")
                .endRule()
                .build();
    }

    private static RoleBinding pekkoRoleBinding(String name, String namespace) {
        return new RoleBindingBuilder()
                .withNewMetadata().withName(name + "-pekko").withLabels(labels(name)).endMetadata()
                .withNewRoleRef()
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind("Role")
                .withName(name + "-pekko")
                .endRoleRef()
                .addNewSubject()
                .withKind("ServiceAccount")
                .withName(name)
                .withNamespace(namespace)
                .endSubject()
                .build();
    }

    // ---- services -----------------------------------------------------------

    private static Service mainService(String name, JoxetteClusterSpec spec) {
        return new ServiceBuilder()
                .withNewMetadata().withName(name).withLabels(labels(name)).endMetadata()
                .withNewSpec()
                .withSelector(selector(name))
                .addNewPort().withName("http").withPort(HTTP_PORT).withNewTargetPort("http").endPort()
                .endSpec()
                .build();
    }

    private static Service headlessService(String name, JoxetteClusterSpec spec) {
        boolean mgmt = spec.getClustering().getMode() == ClusteringMode.pekko_management;
        var builder = new ServiceBuilder()
                .withNewMetadata().withName(name + "-headless").withLabels(labels(name)).endMetadata()
                .withNewSpec()
                .withClusterIP("None")
                .withSelector(selector(name));
        if (mgmt) {
            builder = builder.withPublishNotReadyAddresses(true)
                    .addNewPort().withName("management")
                    .withPort(spec.getClustering().getManagementPort())
                    .withNewTargetPort("management").endPort();
        } else {
            builder = builder.addNewPort().withName("http").withPort(HTTP_PORT)
                    .withNewTargetPort("http").endPort();
        }
        return builder.endSpec().build();
    }

    // ---- shared bits --------------------------------------------------------

    private static List<ContainerPort> containerPorts(JoxetteClusterSpec spec) {
        List<ContainerPort> ports = new ArrayList<>();
        ports.add(new ContainerPortBuilder().withName("http").withContainerPort(HTTP_PORT).build());
        if (spec.getClustering().getMode() == ClusteringMode.pekko_management) {
            ports.add(new ContainerPortBuilder().withName("management")
                    .withContainerPort(spec.getClustering().getManagementPort()).build());
        }
        return ports;
    }

    private static List<EnvVar> env(String name, JoxetteClusterSpec spec,
                                    List<String> roles, boolean replayEnabled) {
        List<EnvVar> env = new ArrayList<>();
        String catalogPath = spec.getCatalog().getBackend() == CatalogBackend.embedded
                ? spec.getCatalog().getEmbedded().getPath()
                : spec.getCatalog().getUri();
        add(env, "JOXETTE_CATALOG_PATH", catalogPath);
        add(env, "JOXETTE_CATALOG_OBJECT-STORAGE-PATH", spec.getCatalog().getObjectStoragePath());
        add(env, "JOXETTE_ROLES", String.join(",", roles));
        add(env, "JOXETTE_REPLAY_ENABLED", String.valueOf(replayEnabled));
        add(env, "JOXETTE_CLUSTERING_MODE", clusteringModeValue(spec));
        add(env, "JOXETTE_KAFKA_BOOTSTRAP-SERVERS", spec.getKafka().getBootstrapServers());
        add(env, "JOXETTE_KAFKA_CONSUMER-GROUP", spec.getKafka().getConsumerGroup());
        add(env, "JOXETTE_KAFKA_GROUP-PROTOCOL", spec.getKafka().getGroupProtocol());
        if (spec.getClustering().getMode() == ClusteringMode.pekko_management) {
            add(env, "JOXETTE_CLUSTERING_MANAGEMENT-PORT",
                    String.valueOf(spec.getClustering().getManagementPort()));
            add(env, "JOXETTE_CLUSTERING_SERVICE-NAME", name);
            add(env, "JOXETTE_CLUSTERING_REQUIRED-CONTACT-POINT-NR",
                    String.valueOf(spec.getTiers().getRecorder().getReplicas()));
            env.add(new EnvVarBuilder().withName("POD_IP")
                    .withNewValueFrom().withNewFieldRef().withFieldPath("status.podIP")
                    .endFieldRef().endValueFrom().build());
        }
        if (notBlank(spec.getObjectStore().getEndpoint())) {
            add(env, "JOXETTE_S3_ENDPOINT", spec.getObjectStore().getEndpoint());
        }
        add(env, "JOXETTE_S3_REGION", spec.getObjectStore().getRegion());
        if (notBlank(spec.getObjectStore().getSecretRef())) {
            env.add(secretEnv("JOXETTE_S3_ACCESS-KEY", spec.getObjectStore().getSecretRef(), "access-key"));
            env.add(secretEnv("JOXETTE_S3_SECRET-KEY", spec.getObjectStore().getSecretRef(), "secret-key"));
        }
        spec.getExtraEnv().forEach((k, v) -> add(env, k, v));
        return env;
    }

    private static String clusteringModeValue(JoxetteClusterSpec spec) {
        // Enum constant is pekko_management (Java identifier); the property value is the hyphenated form.
        return spec.getClustering().getMode() == ClusteringMode.pekko_management
                ? "pekko-management" : "catalog";
    }

    private static EnvVar secretEnv(String name, String secret, String key) {
        return new EnvVarBuilder().withName(name)
                .withNewValueFrom().withNewSecretKeyRef().withName(secret).withKey(key)
                .endSecretKeyRef().endValueFrom().build();
    }

    private static void add(List<EnvVar> env, String name, String value) {
        if (value != null) {
            env.add(new EnvVarBuilder().withName(name).withValue(value).build());
        }
    }

    private static Map<String, String> labels(String name) {
        Map<String, String> l = new LinkedHashMap<>();
        l.put("app.kubernetes.io/name", name);
        l.put("app.kubernetes.io/managed-by", "joxette-operator");
        return l;
    }

    private static Map<String, String> selector(String name) {
        Map<String, String> s = new LinkedHashMap<>();
        s.put("app.kubernetes.io/name", name);
        return s;
    }

    private static String parentDir(String path) {
        int i = path.lastIndexOf('/');
        return i > 0 ? path.substring(0, i) : "/data";
    }

    private static boolean notBlank(String s) {
        return s != null && !s.isBlank();
    }

    private static String blankToNull(String s) {
        return notBlank(s) ? s : null;
    }
}
