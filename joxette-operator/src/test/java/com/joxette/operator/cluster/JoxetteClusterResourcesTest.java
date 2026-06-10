package com.joxette.operator.cluster;

import com.joxette.operator.cluster.JoxetteClusterSpec.CatalogBackend;
import com.joxette.operator.cluster.JoxetteClusterSpec.ClusteringMode;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

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
}
