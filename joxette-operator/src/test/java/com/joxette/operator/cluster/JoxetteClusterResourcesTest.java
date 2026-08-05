package com.joxette.operator.cluster;

import com.joxette.operator.cluster.JoxetteClusterSpec.CatalogBackend;
import com.joxette.operator.cluster.JoxetteClusterSpec.ClusteringMode;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class JoxetteClusterResourcesTest {

    private static JoxetteCluster cluster(JoxetteClusterSpec spec) {
        JoxetteCluster c = new JoxetteCluster();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("prod");
        meta.setNamespace("joxette");
        c.setMetadata(meta);
        c.setSpec(spec);
        return c;
    }

    private static Map<String, String> envOf(io.fabric8.kubernetes.api.model.Container c) {
        return c.getEnv().stream()
                .filter(e -> e.getValue() != null)
                .collect(java.util.stream.Collectors.toMap(
                        io.fabric8.kubernetes.api.model.EnvVar::getName,
                        io.fabric8.kubernetes.api.model.EnvVar::getValue));
    }

    @Test
    void embeddedProducesSingleStatefulSetWithPvcAndAllRoles() {
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(CatalogBackend.embedded);

        List<HasMetadata> objs = JoxetteClusterResources.build(cluster(spec));

        List<StatefulSet> sts = objs.stream().filter(StatefulSet.class::isInstance)
                .map(StatefulSet.class::cast).toList();
        assertThat(sts).hasSize(1);
        StatefulSet ss = sts.get(0);
        assertThat(ss.getSpec().getReplicas()).isEqualTo(1);
        assertThat(ss.getSpec().getVolumeClaimTemplates()).hasSize(1);

        var container = ss.getSpec().getTemplate().getSpec().getContainers().get(0);
        Map<String, String> env = envOf(container);
        assertThat(env).containsEntry("JOXETTE_ROLES", "all");
        assertThat(env).containsEntry("JOXETTE_CLUSTERING_MODE", "catalog");
        assertThat(env).containsEntry("JOXETTE_CATALOG_PATH", "/data/joxette.ducklake");
        // No embedded Deployments.
        assertThat(objs).noneMatch(Deployment.class::isInstance);
    }

    @Test
    void sharedBackendProducesPerTierDeploymentsWithCompactionPinned() {
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(CatalogBackend.postgresql);
        spec.getCatalog().setUri("postgresql://pg/joxette");
        spec.getTiers().getRecorder().setReplicas(3);
        spec.getTiers().getReplay().setReplicas(2);
        spec.getTiers().getCompaction().setReplicas(5); // must be pinned down to 1

        List<Deployment> deps = JoxetteClusterResources.build(cluster(spec)).stream()
                .filter(Deployment.class::isInstance).map(Deployment.class::cast).toList();

        assertThat(deps).extracting(d -> d.getMetadata().getName())
                .containsExactlyInAnyOrder("prod-recorder", "prod-replay", "prod-compaction");

        Map<String, Integer> replicas = deps.stream().collect(java.util.stream.Collectors.toMap(
                d -> d.getMetadata().getName(), d -> d.getSpec().getReplicas()));
        assertThat(replicas).containsEntry("prod-recorder", 3)
                .containsEntry("prod-replay", 2)
                .containsEntry("prod-compaction", 1);
        // No StatefulSet for shared backends.
        assertThat(JoxetteClusterResources.build(cluster(spec)))
                .noneMatch(StatefulSet.class::isInstance);
    }

    @Test
    void pekkoManagementAddsHeadlessServiceAndPodIpEnv() {
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(CatalogBackend.postgresql);
        spec.getCatalog().setUri("postgresql://pg/joxette");
        spec.getClustering().setMode(ClusteringMode.pekko_management);

        List<HasMetadata> objs = JoxetteClusterResources.build(cluster(spec));

        // A headless Service named prod-headless with the management port exists.
        assertThat(objs).filteredOn(Service.class::isInstance).map(Service.class::cast)
                .anyMatch(s -> "prod-headless".equals(s.getMetadata().getName())
                        && "None".equals(s.getSpec().getClusterIP()));

        // Recorder deployment carries POD_IP via the downward API + clustering env.
        Deployment recorder = objs.stream().filter(Deployment.class::isInstance)
                .map(Deployment.class::cast)
                .filter(d -> d.getMetadata().getName().equals("prod-recorder")).findFirst().orElseThrow();
        var container = recorder.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertThat(container.getEnv()).anyMatch(e ->
                "POD_IP".equals(e.getName()) && e.getValueFrom() != null
                && e.getValueFrom().getFieldRef() != null);
        assertThat(envOf(container)).containsEntry("JOXETTE_CLUSTERING_MODE", "pekko-management")
                .containsEntry("JOXETTE_CLUSTERING_SERVICE-NAME", "prod");
    }

    @Test
    void everyClusterGetsAServiceAccountWiredIntoPods() {
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(CatalogBackend.embedded);

        List<HasMetadata> objs = JoxetteClusterResources.build(cluster(spec));
        assertThat(objs).filteredOn(ServiceAccount.class::isInstance).map(ServiceAccount.class::cast)
                .anyMatch(sa -> "prod".equals(sa.getMetadata().getName()));

        StatefulSet ss = objs.stream().filter(StatefulSet.class::isInstance)
                .map(StatefulSet.class::cast).findFirst().orElseThrow();
        assertThat(ss.getSpec().getTemplate().getSpec().getServiceAccountName()).isEqualTo("prod");
    }

    @Test
    void catalogModeHasNoPekkoRbac() {
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(CatalogBackend.embedded);

        assertThat(JoxetteClusterResources.build(cluster(spec)))
                .noneMatch(Role.class::isInstance)
                .noneMatch(RoleBinding.class::isInstance);
    }

    @Test
    void pekkoManagementGrantsPodAndLeaseRbac() {
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(CatalogBackend.postgresql);
        spec.getCatalog().setUri("postgresql://pg/joxette");
        spec.getClustering().setMode(ClusteringMode.pekko_management);

        List<HasMetadata> objs = JoxetteClusterResources.build(cluster(spec));

        Role role = objs.stream().filter(Role.class::isInstance).map(Role.class::cast)
                .findFirst().orElseThrow();
        // pods: get/watch/list ; leases: create/update/...
        assertThat(role.getRules()).anyMatch(r -> r.getResources().contains("pods")
                && r.getVerbs().containsAll(List.of("get", "watch", "list")));
        assertThat(role.getRules()).anyMatch(r -> r.getResources().contains("leases")
                && r.getVerbs().contains("create"));

        RoleBinding rb = objs.stream().filter(RoleBinding.class::isInstance)
                .map(RoleBinding.class::cast).findFirst().orElseThrow();
        assertThat(rb.getRoleRef().getName()).isEqualTo("prod-pekko");
        assertThat(rb.getSubjects()).anyMatch(s -> "prod".equals(s.getName())
                && "ServiceAccount".equals(s.getKind()) && "joxette".equals(s.getNamespace()));
    }

    @Test
    void catalogModeSharedBackendHasNoHeadlessService() {
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(CatalogBackend.quack);
        spec.getCatalog().setUri("quack://q:5432/joxette");

        assertThat(JoxetteClusterResources.build(cluster(spec)))
                .filteredOn(Service.class::isInstance).map(Service.class::cast)
                .noneMatch(s -> s.getMetadata().getName().endsWith("-headless"));
    }

    @Test
    void embeddedStatefulSetHasProbesAndResourceLimits() {
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(CatalogBackend.embedded);

        StatefulSet ss = JoxetteClusterResources.build(cluster(spec)).stream()
                .filter(StatefulSet.class::isInstance).map(StatefulSet.class::cast)
                .findFirst().orElseThrow();
        Container container = ss.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(container.getStartupProbe()).isNotNull();
        assertThat(container.getStartupProbe().getHttpGet().getPath()).isEqualTo("/actuator/health");
        assertThat(container.getLivenessProbe()).isNotNull();
        assertThat(container.getLivenessProbe().getHttpGet().getPath()).isEqualTo("/actuator/health/liveness");
        assertThat(container.getReadinessProbe()).isNotNull();
        assertThat(container.getReadinessProbe().getHttpGet().getPath()).isEqualTo("/actuator/health/readiness");

        assertThat(container.getResources()).isNotNull();
        assertThat(container.getResources().getRequests())
                .containsEntry("cpu", new Quantity("1"))
                .containsEntry("memory", new Quantity("2Gi"));
        assertThat(container.getResources().getLimits())
                .containsEntry("memory", new Quantity("2Gi"));
    }

    /**
     * Covers all 3 tiers (not just replay) so a copy-paste bug at a {@code build()}
     * call site that swaps which tier's {@link JoxetteClusterSpec.Resources} gets
     * passed to {@code tierDeployment(...)} — e.g. compaction accidentally getting
     * replay's resources — would fail this test even though every tier "has some
     * resources block", which is all the single-tier version used to check.
     */
    static Stream<Arguments> tierResourceExpectations() {
        return Stream.of(
                Arguments.of("prod-recorder", "1", "2Gi"),
                Arguments.of("prod-replay", "500m", "1Gi"),
                Arguments.of("prod-compaction", "500m", "1Gi"));
    }

    @ParameterizedTest(name = "{0}: cpu={1} memory={2}")
    @MethodSource("tierResourceExpectations")
    void tierDeploymentsHaveProbesAndTheirOwnTiersResourceLimits(
            String deploymentName, String expectedCpu, String expectedMemory) {
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(CatalogBackend.postgresql);
        spec.getCatalog().setUri("postgresql://pg/joxette");

        List<Deployment> deps = JoxetteClusterResources.build(cluster(spec)).stream()
                .filter(Deployment.class::isInstance).map(Deployment.class::cast).toList();

        Deployment deployment = deps.stream()
                .filter(d -> d.getMetadata().getName().equals(deploymentName)).findFirst().orElseThrow();
        Container container = deployment.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(container.getStartupProbe()).isNotNull();
        assertThat(container.getLivenessProbe()).isNotNull();
        assertThat(container.getReadinessProbe()).isNotNull();
        assertThat(container.getResources().getRequests())
                .containsEntry("cpu", new Quantity(expectedCpu))
                .containsEntry("memory", new Quantity(expectedMemory));
        assertThat(container.getResources().getLimits())
                .containsEntry("memory", new Quantity(expectedMemory));
    }

    @Test
    void embeddedBackendGetsAZeroMaxUnavailablePdb() {
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(CatalogBackend.embedded);

        PodDisruptionBudget pdb = JoxetteClusterResources.build(cluster(spec)).stream()
                .filter(PodDisruptionBudget.class::isInstance).map(PodDisruptionBudget.class::cast)
                .findFirst().orElseThrow();

        assertThat(pdb.getMetadata().getName()).isEqualTo("prod");
        assertThat(pdb.getSpec().getMaxUnavailable().getIntVal()).isEqualTo(0);
        assertThat(pdb.getSpec().getSelector().getMatchLabels())
                .containsEntry("app.kubernetes.io/name", "prod");
    }

    @Test
    void partialRequestMemoryOverrideDerivesMatchingLimitMemory() {
        // Simulates a CR that overrides only requestMemory (e.g.
        // spec.tiers.recorder.resources: {requestMemory: 8Gi}) without touching
        // limitMemory. Before the fix, limits.memory stayed at the Resources
        // class's raw field default ("1Gi"), producing limits < requests, which
        // the API server rejects outright.
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(CatalogBackend.embedded);
        spec.getTiers().getRecorder().getResources().setRequestMemory("8Gi");

        StatefulSet ss = JoxetteClusterResources.build(cluster(spec)).stream()
                .filter(StatefulSet.class::isInstance).map(StatefulSet.class::cast)
                .findFirst().orElseThrow();
        Container container = ss.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(container.getResources().getRequests()).containsEntry("memory", new Quantity("8Gi"));
        assertThat(container.getResources().getLimits()).containsEntry("memory", new Quantity("8Gi"));
    }

    @Test
    void explicitLimitMemoryOverrideIsPreservedEvenWhenDifferentFromRequestMemory() {
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(CatalogBackend.embedded);
        spec.getTiers().getRecorder().getResources().setRequestMemory("2Gi");
        spec.getTiers().getRecorder().getResources().setLimitMemory("4Gi");

        StatefulSet ss = JoxetteClusterResources.build(cluster(spec)).stream()
                .filter(StatefulSet.class::isInstance).map(StatefulSet.class::cast)
                .findFirst().orElseThrow();
        Container container = ss.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(container.getResources().getRequests()).containsEntry("memory", new Quantity("2Gi"));
        assertThat(container.getResources().getLimits()).containsEntry("memory", new Quantity("4Gi"));
    }

    @Test
    void sharedBackendGetsNoPdb() {
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(CatalogBackend.postgresql);
        spec.getCatalog().setUri("postgresql://pg/joxette");

        assertThat(JoxetteClusterResources.build(cluster(spec)))
                .noneMatch(PodDisruptionBudget.class::isInstance);
    }
}
