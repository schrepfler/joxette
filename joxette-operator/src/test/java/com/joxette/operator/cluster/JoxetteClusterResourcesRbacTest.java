package com.joxette.operator.cluster;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.joxette.operator.cluster.JoxetteClusterSpec.CatalogBackend;
import com.joxette.operator.cluster.JoxetteClusterSpec.ClusteringMode;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.PolicyRule;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleEntry;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards against Critical 1 from the ops-deployment-readiness review: every
 * Kubernetes object kind {@link JoxetteClusterResources#build} can emit must have
 * a matching {@code (apiGroup, resource)} grant in the operator's real RBAC
 * manifest ({@code deploy/operator/rbac.yaml}), or the reconciler's
 * server-side-apply gets a 403 and JOSDK retries forever without ever setting
 * status. This parses the actual manifest file (not a hand-copied snapshot of
 * it), so a future edit that drops or narrows a rule fails this test instead of
 * only failing at runtime against a real cluster.
 *
 * <p>Verbs are intentionally not asserted here — {@code build()}'s output only
 * tells us which object <em>kinds</em> exist, not which verbs the reconciler
 * needs against them (get/list/watch for the informer cache, create/update/patch
 * for server-side-apply, delete for pruning on spec changes that disable a
 * tier). The apiGroup/resource coverage is the check this test can make
 * mechanically; verb sufficiency is exercised functionally by
 * {@link JoxetteClusterReconcilerTest} against the fabric8 mock server.
 */
class JoxetteClusterResourcesRbacTest {

    /**
     * Kind -&gt; (apiGroup, plural resource) for every top-level object
     * {@link JoxetteClusterResources#build} can return. PersistentVolumeClaim is
     * intentionally absent: it's embedded in the StatefulSet's
     * {@code volumeClaimTemplates}, never a standalone object build() returns, so
     * it needs no separate RBAC grant.
     */
    private static final Map<Class<? extends HasMetadata>, SimpleEntry<String, String>> RBAC_SHAPE = Map.of(
            StatefulSet.class, entry("apps", "statefulsets"),
            Deployment.class, entry("apps", "deployments"),
            Service.class, entry("", "services"),
            ServiceAccount.class, entry("", "serviceaccounts"),
            Role.class, entry("rbac.authorization.k8s.io", "roles"),
            RoleBinding.class, entry("rbac.authorization.k8s.io", "rolebindings"),
            PodDisruptionBudget.class, entry("policy", "poddisruptionbudgets"));

    private static SimpleEntry<String, String> entry(String apiGroup, String resource) {
        return new SimpleEntry<>(apiGroup, resource);
    }

    @Test
    void everyBuiltResourceKindIsGrantedByTheOperatorClusterRole() throws IOException {
        ClusterRole role = loadOperatorClusterRole();
        Set<SimpleEntry<String, String>> granted = role.getRules().stream()
                .flatMap(JoxetteClusterResourcesRbacTest::expand)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<Class<? extends HasMetadata>> kindsBuilt = allBuiltKinds();
        // Sanity check on the test itself: if build() stops returning anything
        // recognizable (e.g. every spec permutation below starts throwing), this
        // catches a silently-vacuous test rather than a false "pass".
        assertThat(kindsBuilt).contains(StatefulSet.class, Deployment.class, PodDisruptionBudget.class);

        for (Class<? extends HasMetadata> kind : kindsBuilt) {
            SimpleEntry<String, String> shape = RBAC_SHAPE.get(kind);
            assertThat(shape)
                    .as("no RBAC_SHAPE mapping registered in this test for %s — add one so RBAC coverage "
                            + "for it is actually verified", kind.getSimpleName())
                    .isNotNull();
            assertThat(granted)
                    .as("%s needs (apiGroup=%s, resource=%s) granted in deploy/operator/rbac.yaml, or the "
                            + "reconciler's server-side-apply 403s for every cluster using this shape",
                            kind.getSimpleName(), shape.getKey(), shape.getValue())
                    .contains(shape);
        }
    }

    private static Stream<SimpleEntry<String, String>> expand(PolicyRule rule) {
        List<String> groups = rule.getApiGroups() == null ? List.of() : rule.getApiGroups();
        List<String> resources = rule.getResources() == null ? List.of() : rule.getResources();
        return groups.stream().flatMap(g -> resources.stream().map(r -> entry(g, r)));
    }

    /** Union of object kinds across every spec shape build() can produce. */
    private static Set<Class<? extends HasMetadata>> allBuiltKinds() {
        Set<Class<? extends HasMetadata>> kinds = new LinkedHashSet<>();
        kindsOf(embeddedSpec()).forEach(kinds::add);
        kindsOf(sharedSpec()).forEach(kinds::add);
        kindsOf(pekkoManagementSpec()).forEach(kinds::add);
        return kinds;
    }

    private static Stream<Class<? extends HasMetadata>> kindsOf(JoxetteClusterSpec spec) {
        JoxetteCluster c = new JoxetteCluster();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("prod");
        meta.setNamespace("joxette");
        c.setMetadata(meta);
        c.setSpec(spec);
        return JoxetteClusterResources.build(c).stream().map(HasMetadata::getClass);
    }

    private static JoxetteClusterSpec embeddedSpec() {
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(CatalogBackend.embedded);
        return spec;
    }

    private static JoxetteClusterSpec sharedSpec() {
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(CatalogBackend.postgresql);
        spec.getCatalog().setUri("postgresql://pg/joxette");
        return spec;
    }

    private static JoxetteClusterSpec pekkoManagementSpec() {
        JoxetteClusterSpec spec = sharedSpec();
        spec.getClustering().setMode(ClusteringMode.pekko_management);
        return spec;
    }

    /**
     * Parses the first YAML document in {@code deploy/operator/rbac.yaml} (the
     * {@code ClusterRole}; the {@code ClusterRoleBinding} document is ignored) via
     * the real repo file, located by walking up from the working directory —
     * Surefire's default cwd is the module basedir ({@code joxette-operator}), so
     * the manifest is one level up, but this walks further just in case the
     * runner's cwd differs (e.g. an IDE running from the repo root).
     */
    private static ClusterRole loadOperatorClusterRole() throws IOException {
        Path manifest = findRepoFile("deploy/operator/rbac.yaml");
        String content = Files.readString(manifest);
        String firstDocument = Pattern.compile("(?m)^---\\s*$").split(content, 2)[0];
        return new YAMLMapper().readValue(firstDocument, ClusterRole.class);
    }

    private static Path findRepoFile(String relativePath) {
        Path dir = Paths.get("").toAbsolutePath();
        for (int i = 0; i < 6; i++) {
            Path candidate = dir.resolve(relativePath);
            if (Files.exists(candidate)) {
                return candidate;
            }
            Path parent = dir.getParent();
            if (parent == null) {
                break;
            }
            dir = parent;
        }
        throw new IllegalStateException(
                "Could not locate " + relativePath + " by walking up from " + Paths.get("").toAbsolutePath());
    }
}
