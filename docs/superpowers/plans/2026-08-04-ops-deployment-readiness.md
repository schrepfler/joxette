# Ops & Deployment Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the five concrete gaps between what `docs/operator-design.md` claims is production-ready and what the Helm chart / operator / metrics actually ship — missing probes and resource limits on operator-built workloads, no PodDisruptionBudget anywhere, no Prometheus signal for the write-channel's sink health state, no alerting rules at all, and an HPA that ignores the one metric that actually reflects replay load.

**Architecture:** Every change either (a) makes `JoxetteClusterResources.java` (the operator's pure Kubernetes-object builder) produce the same probe/resource/PDB shape the Helm chart already renders correctly, so the two deployment paths stay in parity, or (b) adds a new Prometheus-facing signal (`joxette_sink_state`) and the alerting/autoscaling rules that consume it. Nothing here changes the catalog single-writer guardrail (`CatalogGuardrail`, `joxette.validate`) or the `replicas: 1` embedded topology — every new manifest is additive to the existing tier/StatefulSet shape.

**Tech Stack:** Helm 3, Java Operator SDK (JOSDK), Kubernetes 1.29+, Prometheus Operator CRDs (ServiceMonitor/PrometheusRule), Java 25 / Spring Boot 4.0.5 for the metrics task

## Global Constraints

- Every workload manifest (Helm-rendered or operator-generated) must set both `resources.requests` and `resources.limits`
- Every workload manifest must set `livenessProbe`/`readinessProbe` wired to the app's `/actuator/health/liveness` and `/actuator/health/readiness` groups
- The embedded-catalog (single-writer DuckDB) topology must remain `replicas: 1`; nothing in this plan may allow it to be scaled past 1

---

### Task A: Probes and resource requests/limits on operator-generated workloads

`JoxetteClusterResources.java`'s `embeddedStatefulSet()` and `tierDeployment()` build containers with no `startupProbe`/`livenessProbe`/`readinessProbe` and no `resources` block at all — `docs/operator-design.md` §7 documents these probes as if they exist. The Helm chart already gets this right via the `joxette.probes` helper (`deploy/helm/joxette/templates/_helpers.tpl:181-198`) and per-tier `resources.requests` in `values.yaml`. This task ports both into the operator and, in the same pass, closes a second real gap the Global Constraint above exposes: **neither the chart nor the operator set `resources.limits` today** — `values.yaml`'s `tiers.*.resources` only ever had `requests`. The fix for both paths uses the convention already established (just not applied to Joxette's own workloads) in `deploy/operator/deployment.yaml:42-44`: `limits: { memory: ... }` with **no cpu limit** — bound memory hard (JVM heap + DuckDB native allocations must never spill onto the node) but leave cpu unbounded so a burst never gets throttled.

**Files:**
- Modify: `deploy/helm/joxette/values.yaml` (tiers block, lines 70-101)
- Modify: `deploy/helm/joxette/values-cluster.yaml` (tiers block, lines 24-49)
- Modify: `joxette-operator/src/main/java/com/joxette/operator/cluster/JoxetteClusterSpec.java` (`Tiers`/`Tier`, lines 98-124)
- Modify: `joxette-operator/src/main/java/com/joxette/operator/cluster/JoxetteClusterResources.java` (imports; `embeddedStatefulSet()` lines 86-124; `tierDeployment()` lines 126-156)
- Test: `joxette-operator/src/test/java/com/joxette/operator/cluster/JoxetteClusterResourcesTest.java`
- Test (Helm): `helm template` / `helm lint` output shown in Step 5 below

**Interfaces:**
- New Helm values keys: `tiers.recorder.resources.limits.memory`, `tiers.replay.resources.limits.memory`, `tiers.compaction.resources.limits.memory`
- New Java type: `JoxetteClusterSpec.Resources` (`requestCpu`, `requestMemory`, `limitMemory`), field `JoxetteClusterSpec.Tier.resources`
- `JoxetteClusterResources.tierDeployment(...)` gains a `JoxetteClusterSpec.Resources tierResources` parameter (signature change, all 3 call sites in `build()` updated)

#### Steps

- [ ] 1. Add `limits` to the Helm chart's tier resource blocks — the numeric requests are untouched (mirrored, not invented); the memory-only-limit shape mirrors `deploy/operator/deployment.yaml:42-44`.

Edit `deploy/helm/joxette/values.yaml`, replacing the `tiers:` block (original lines 70-101):

```yaml
tiers:
  recorder:
    enabled: true
    replicas: 1
    roles: [recorder, entity-router]
    # Pure write node: turn off the replay API to avoid serving reads here.
    replayEnabled: false
    # limits.memory == requests.memory (no cpu limit) — same convention already
    # used for the operator's own Deployment (deploy/operator/deployment.yaml):
    # bound memory hard (JVM + DuckDB native allocations must not spill onto the
    # node) but leave cpu unbounded so a burst never gets throttled.
    resources:
      requests: { cpu: "1", memory: 2Gi }
      limits: { memory: 2Gi }
  replay:
    enabled: true
    replicas: 1
    roles: [replay]
    replayEnabled: true
    resources:
      requests: { cpu: 500m, memory: 1Gi }
      limits: { memory: 1Gi }
    # Horizontal autoscaling for the (stateless) replay tier. Shared-catalog
    # backends only — rejected with an embedded catalog (single pod). Requires
    # metrics-server in the cluster (CPU metric) AND, for targetActiveReplaysPerPod,
    # prometheus-adapter exposing joxette_replay_active via the custom metrics API
    # (custom.metrics.k8s.io) — the chart does not install or configure the adapter.
    # When enabled the Deployment omits a static replica count so the HPA owns it
    # (avoids replica fighting on each reconcile).
    hpa:
      enabled: false
      minReplicas: 2
      maxReplicas: 10
      targetCPUUtilizationPercentage: 70
      # Average of joxette_replay_active (active replay-to-topic ops) per pod.
      # Replay is I/O-bound (SSE/NDJSON fan-out) — many slow concurrent streams
      # can saturate a pod at low CPU, so this metric scales out cases CPU alone
      # misses. HPA takes the max recommended replicas across all metrics.
      targetActiveReplaysPerPod: 10
  compaction:
    enabled: true
    # replicas always 1 — the chart pins it regardless of this value.
    roles: [compaction]
    replayEnabled: false
    resources:
      requests: { cpu: 500m, memory: 1Gi }
      limits: { memory: 1Gi }
```

(The `hpa.targetActiveReplaysPerPod` key is added here because it lives in the same block; it is consumed by Task E, not this task.)

- [ ] 2. Mirror the same `limits` addition into the cluster overlay.

Edit `deploy/helm/joxette/values-cluster.yaml`, replacing the `tiers:` block (original lines 24-49):

```yaml
tiers:
  recorder:
    enabled: true
    replicas: 3
    roles: [recorder, entity-router]
    replayEnabled: false
    resources:
      requests: { cpu: "1", memory: 2Gi }
      limits: { memory: 2Gi }
  replay:
    enabled: true
    replicas: 2
    roles: [replay]
    replayEnabled: true
    resources:
      requests: { cpu: 500m, memory: 1Gi }
      limits: { memory: 1Gi }
    # Autoscale the stateless replay tier (needs metrics-server for cpu, and
    # prometheus-adapter for targetActiveReplaysPerPod). When enabled,
    # `replicas` above is ignored — the HPA owns the replica count.
    hpa:
      enabled: true
      minReplicas: 2
      maxReplicas: 10
      targetCPUUtilizationPercentage: 70
      targetActiveReplaysPerPod: 10
  compaction:
    enabled: true
    roles: [compaction]
    replayEnabled: false
```

(`compaction` here has no `resources` override — it inherits the `values.yaml` default from Step 1, which now includes `limits`.)

- [ ] 3. Run `helm lint` and `helm template` to confirm both values files render the new `limits` block. Commands and exact expected output (already verified against the real chart):

```
helm lint deploy/helm/joxette
helm lint deploy/helm/joxette -f deploy/helm/joxette/values-cluster.yaml
helm template joxette deploy/helm/joxette -s templates/statefulset-embedded.yaml | grep -A4 "resources:"
helm template joxette deploy/helm/joxette -f deploy/helm/joxette/values-cluster.yaml -s templates/deployment-tiers.yaml | grep -A6 "resources:"
```

Expected fragment from the embedded StatefulSet (default `values.yaml`, recorder-tier resources reused per `statefulset-embedded.yaml:55-57`):

```yaml
          resources:
            limits:
              memory: 2Gi
            requests:
              cpu: "1"
```

Expected fragment from the cluster-overlay Deployments (three tiers, `deployment-tiers.yaml`):

```yaml
          resources:
            limits:
              memory: 2Gi
            requests:
              cpu: "1"
              memory: 2Gi
---
          resources:
            limits:
              memory: 1Gi
            requests:
              cpu: 500m
              memory: 1Gi
---
          resources:
            limits:
              memory: 1Gi
            requests:
              cpu: 500m
              memory: 1Gi
```

Both `helm lint` invocations report `0 chart(s) failed`; both `helm template` invocations exit `0`.

- [ ] 4. Give the operator's `JoxetteClusterSpec.Tier` the same requests/limits shape so `JoxetteClusterResources` has something to read. Edit `joxette-operator/src/main/java/com/joxette/operator/cluster/JoxetteClusterSpec.java`, replacing the `Tiers`/`Tier` block (original lines 98-124):

```java
    /** Role tiers (shared-catalog backends only; ignored for embedded). */
    public static class Tiers {
        private Tier recorder = Tier.of(2, true, "1", "2Gi");
        private Tier replay = Tier.of(2, true, "500m", "1Gi");
        private Tier compaction = Tier.of(1, true, "500m", "1Gi");
        public Tier getRecorder() { return recorder; }
        public void setRecorder(Tier recorder) { this.recorder = recorder; }
        public Tier getReplay() { return replay; }
        public void setReplay(Tier replay) { this.replay = replay; }
        public Tier getCompaction() { return compaction; }
        public void setCompaction(Tier compaction) { this.compaction = compaction; }
    }

    public static class Tier {
        private boolean enabled = true;
        private int replicas = 1;
        private Resources resources = new Resources();

        static Tier of(int replicas, boolean enabled, String requestCpu, String requestMemory) {
            Tier t = new Tier();
            t.replicas = replicas;
            t.enabled = enabled;
            t.resources.requestCpu = requestCpu;
            t.resources.requestMemory = requestMemory;
            t.resources.limitMemory = requestMemory;
            return t;
        }
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public int getReplicas() { return replicas; }
        public void setReplicas(int replicas) { this.replicas = replicas; }
        public Resources getResources() { return resources; }
        public void setResources(Resources resources) { this.resources = resources; }
    }

    /**
     * Container resources for a tier. {@code limitMemory} defaults to
     * {@code requestMemory} and there is deliberately no cpu limit — the same
     * convention already used for the operator's own Deployment
     * (deploy/operator/deployment.yaml): bound memory hard (JVM heap + DuckDB
     * native allocations must not spill onto the node) but leave cpu unbounded
     * so a burst never gets throttled.
     */
    public static class Resources {
        private String requestCpu = "500m";
        private String requestMemory = "1Gi";
        private String limitMemory = "1Gi";
        public String getRequestCpu() { return requestCpu; }
        public void setRequestCpu(String requestCpu) { this.requestCpu = requestCpu; }
        public String getRequestMemory() { return requestMemory; }
        public void setRequestMemory(String requestMemory) { this.requestMemory = requestMemory; }
        public String getLimitMemory() { return limitMemory; }
        public void setLimitMemory(String limitMemory) { this.limitMemory = limitMemory; }
    }
```

`Tier.of(...)`'s signature change is contained: it is only called from the three `Tiers` field initializers above (verified via `grep -rn "Tier.of" joxette-operator/src` — no other call sites).

- [ ] 5. Add the fabric8 builder API for probes and resources to `JoxetteClusterResources.java`, and thread the per-tier `Resources` through both workload builders. fabric8 7.8.0 (`pom.xml:50`, confirmed via `javap` against the resolved jars) exposes exactly the builder methods used below: `ContainerFluent.withStartupProbe/withLivenessProbe/withReadinessProbe/withResources`, `ProbeBuilder.withNewHttpGet()...withPort(IntOrString)`, `ResourceRequirementsBuilder.addToRequests/addToLimits`.

Edit the import block (`joxette-operator/src/main/java/com/joxette/operator/cluster/JoxetteClusterResources.java`, lines 5-23), adding:

```java
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
```

(leave the existing imports — `ContainerPort`, `Quantity`, etc. — untouched; the `PodDisruptionBudget` imports come in Task B).

In `embeddedStatefulSet()` (original lines 112-118), add probes and resources to the container builder:

```java
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
```

(The embedded all-in-one pod reuses the `recorder` tier's resources, exactly like `statefulset-embedded.yaml:55` does via `index .Values.tiers "recorder" "resources"`.)

Change `tierDeployment(...)`'s signature (original line 126-128) to accept the tier's resources:

```java
    private static Deployment tierDeployment(String name, JoxetteClusterSpec spec,
                                             String tier, List<String> roles,
                                             boolean replayEnabled, int replicas,
                                             JoxetteClusterSpec.Resources tierResources) {
```

and its container builder (original lines 146-151):

```java
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
```

Update the three call sites in `build()` (original lines 65-79) to pass the tier's resources:

```java
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
```

Add the probe and resource builder helpers (place after `tierDeployment(...)`, before the `// ---- service account + RBAC ----` section):

```java
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
```

- [ ] 6. Extend `JoxetteClusterResourcesTest.java` with two new tests asserting probes and resource limits on both workload shapes. Add these imports (alongside the existing ones):

```java
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Quantity;
```

Add these two `@Test` methods to the class:

```java
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

    @Test
    void tierDeploymentsHaveProbesAndResourceLimits() {
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(CatalogBackend.postgresql);
        spec.getCatalog().setUri("postgresql://pg/joxette");

        List<Deployment> deps = JoxetteClusterResources.build(cluster(spec)).stream()
                .filter(Deployment.class::isInstance).map(Deployment.class::cast).toList();

        Deployment replay = deps.stream()
                .filter(d -> d.getMetadata().getName().equals("prod-replay")).findFirst().orElseThrow();
        Container container = replay.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(container.getStartupProbe()).isNotNull();
        assertThat(container.getLivenessProbe()).isNotNull();
        assertThat(container.getReadinessProbe()).isNotNull();
        assertThat(container.getResources().getRequests())
                .containsEntry("cpu", new Quantity("500m"))
                .containsEntry("memory", new Quantity("1Gi"));
        assertThat(container.getResources().getLimits())
                .containsEntry("memory", new Quantity("1Gi"));
    }
```

- [ ] 7. Run the operator test module. Verified real output (this exact command, run against this exact diff):

```
mvn -pl joxette-operator test -Dtest=JoxetteClusterResourcesTest
```

```
[INFO] Running com.joxette.operator.cluster.JoxetteClusterResourcesTest
[INFO] Tests run: 11, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.303 s -- in com.joxette.operator.cluster.JoxetteClusterResourcesTest
[INFO] BUILD SUCCESS
```

(11 = the 6 pre-existing tests + 2 new tests here + 3 new tests from Task B, since both tasks land in the same file — see Task B for the PDB tests. If Task A is committed alone, expect `Tests run: 8`.)

Then run the full operator module to confirm nothing else broke (`CatalogGuardrailTest`, `JoxetteClusterReconcilerTest`, etc. also build `JoxetteClusterResources` output):

```
mvn -pl joxette-operator test
```

```
[INFO] Results:
[INFO]
[INFO] Tests run: 50, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

(50 includes both this task's and Task B's new tests; expect 47 if only Task A has landed.)

- [ ] 8. Commit: `fix(ops): add probes and resource limits to operator-generated workloads`

---

### Task B: PodDisruptionBudget for the embedded-catalog deployment

No `PodDisruptionBudget` exists anywhere in the repo (`grep -ri poddisruptionbudget deploy/ joxette-operator/src` returns nothing). The embedded StatefulSet always runs `replicas: 1` (enforced by `joxette.validate` in the chart and `CatalogGuardrail` in the operator — verified in both by reading `_helpers.tpl:87-103` and `CatalogGuardrail.java:31-44`), so a percentage `maxUnavailable` is meaningless: it rounds to 0 either way and documents nothing. `maxUnavailable: 0` states the real intent unambiguously — never voluntarily evict the only pod (node drain, cluster-autoscaler scale-down) — while making clear in a comment that it cannot protect against involuntary disruption (node crash, forced pod deletion).

**Files:**
- Create: `deploy/helm/joxette/templates/pdb-embedded.yaml`
- Modify: `joxette-operator/src/main/java/com/joxette/operator/cluster/JoxetteClusterResources.java` (imports; `build()`; new `embeddedPdb()` method)
- Test: `joxette-operator/src/test/java/com/joxette/operator/cluster/JoxetteClusterResourcesTest.java`

**Interfaces:**
- New Helm template `pdb-embedded.yaml`, gated by the existing `catalog.backend == "embedded"` condition (same one `statefulset-embedded.yaml` uses) — no new values key
- New operator-generated object: `PodDisruptionBudget` named `<cluster-name>` (fabric8 `io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget`, resolved transitively via `kubernetes-client-api` — confirmed present on the `joxette-operator` classpath with `mvn -pl joxette-operator dependency:tree -Dincludes=io.fabric8:kubernetes-model-policy`, no `pom.xml` change needed)

#### Steps

- [ ] 1. Add the Helm template, gated the same way `statefulset-embedded.yaml` is (`{{- if eq .Values.catalog.backend "embedded" }}`).

Create `deploy/helm/joxette/templates/pdb-embedded.yaml`:

```yaml
{{- include "joxette.validate" . -}}
{{- if eq .Values.catalog.backend "embedded" }}
{{- /*
PodDisruptionBudget for the embedded, single-writer catalog StatefulSet.

maxUnavailable is pinned to 0, not a percentage, and this is intentional: the
embedded catalog always runs replicas:1 (see joxette.validate — replicas>1 here
is rejected outright because a second writer against the same DuckDB file
corrupts it). A percentage-based budget (e.g. maxUnavailable: 50%) is
meaningless at replicas=1 — it rounds to 0 either way and documents nothing.
maxUnavailable: 0 states the real intent unambiguously: never voluntarily evict
the only pod (node drain, cluster-autoscaler scale-down, etc. are blocked by
this budget). It does not, and cannot, protect against involuntary disruption
(node crash, `kubectl delete pod --force`).
*/}}
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: {{ include "joxette.fullname" . }}
  labels:
    {{- include "joxette.labels" . | nindent 4 }}
spec:
  maxUnavailable: 0
  selector:
    matchLabels:
      {{- include "joxette.selectorLabels" . | nindent 6 }}
{{- end }}
```

- [ ] 2. Render it. Command and exact verified output:

```
helm template joxette deploy/helm/joxette -s templates/pdb-embedded.yaml
```

```yaml
---
# Source: joxette/templates/pdb-embedded.yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: joxette
  labels:
    helm.sh/chart: joxette-0.1.0
    app.kubernetes.io/name: joxette
    app.kubernetes.io/instance: joxette
    app.kubernetes.io/version: "0.1.0"
    app.kubernetes.io/managed-by: Helm
spec:
  maxUnavailable: 0
  selector:
    matchLabels:
      app.kubernetes.io/name: joxette
      app.kubernetes.io/instance: joxette
```

`helm lint deploy/helm/joxette` still reports `0 chart(s) failed`. With a shared backend (`--set catalog.backend=postgresql --set catalog.uri=postgresql://pg/joxette`) the template renders nothing (the `{{- if eq ... "embedded" }}` guard skips it) — confirm with `helm template joxette deploy/helm/joxette --set catalog.backend=postgresql --set catalog.uri=postgresql://pg/joxette -s templates/pdb-embedded.yaml`, which prints no `PodDisruptionBudget` document.

- [ ] 3. Add the equivalent object to the operator path. Edit `joxette-operator/src/main/java/com/joxette/operator/cluster/JoxetteClusterResources.java`, adding to the import block (alongside the Task A imports):

```java
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudgetBuilder;
```

In `build()`, add the PDB right after the embedded StatefulSet (original line 60):

```java
        if (embedded) {
            out.add(headlessService(name, spec));
            out.add(embeddedStatefulSet(name, spec));
            out.add(embeddedPdb(name));
        } else {
```

Add the builder method, right after `embeddedStatefulSet()`:

```java
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
```

- [ ] 4. Extend `JoxetteClusterResourcesTest.java` — add `import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;` and two `@Test` methods:

```java
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
    void sharedBackendGetsNoPdb() {
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(CatalogBackend.postgresql);
        spec.getCatalog().setUri("postgresql://pg/joxette");

        assertThat(JoxetteClusterResources.build(cluster(spec)))
                .noneMatch(PodDisruptionBudget.class::isInstance);
    }
```

- [ ] 5. Run `mvn -pl joxette-operator test -Dtest=JoxetteClusterResourcesTest` — combined with Task A's two tests this reports `Tests run: 11, Failures: 0, Errors: 0, Skipped: 0` (verified). Then `mvn -pl joxette-operator test` for the full module — `Tests run: 50, Failures: 0, Errors: 0, Skipped: 0` (verified).

- [ ] 6. Commit: `feat(ops): add PodDisruptionBudget for the embedded single-writer catalog`

---

### Task C: Sink-state Prometheus gauge

`DuckLakeWriteChannel`'s `SinkState` enum (`HEALTHY`/`DEGRADED`/`FAILED`, declared at `DuckLakeWriteChannel.java:50`) is the single most operationally important signal in the write-resilience design (`docs/write-resilience.md`) and today has zero Prometheus representation — only log lines (`log.warn("Sink DEGRADED ...")`, `log.error("Sink FAILED ...")`). `JoxetteMetrics.java` already has an established, tested-in-production pattern for exactly this situation (a `Supplier`-backed gauge that must be retained or it silently reports `NaN` after GC — see the class-level comment on `retainedGaugeState`, `JoxetteMetrics.java:103-110`, added after a real incident per project history). This task follows that pattern exactly.

One design constraint worth being explicit about: `SinkState` is package-private (`enum SinkState { HEALTHY, DEGRADED, FAILED }`, no modifier, in package `com.joxette.recording`), and `JoxetteMetrics` lives in `com.joxette.metrics` — a different package. `JoxetteMetrics` therefore cannot reference the `SinkState` type at all, by design (it stays fully internal to the recording package). The gauge method takes a generic `Supplier<Integer>` (the numeric encoding), and `DuckLakeWriteChannel` does the `SinkState -> int` mapping itself before registering. This also resolves the "state name as a label" idea in the task brief: Micrometer `Gauge` tags are fixed at registration time and are never re-evaluated per scrape, so a dynamic "current state name" label isn't something the existing gauge helper (or Micrometer itself) supports — only the numeric encoding is exported. The human-readable name stays available via `/health` (already true per `docs/write-resilience.md`).

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/metrics/JoxetteMetrics.java` (class javadoc lines 21-31; new method after `registerWriteChannelDepthGauge`, original lines 202-211)
- Modify: `joxette-service/src/main/java/com/joxette/recording/DuckLakeWriteChannel.java` (`start()`, original lines 91-100; new private helper after `rootMessage`, original lines 313-317)
- Test (create): `joxette-service/src/test/java/com/joxette/recording/DuckLakeWriteChannelSinkStateGaugeTest.java`

**Interfaces:**
- New metric: `joxette.sink.state` (Micrometer name) / `joxette_sink_state` (Prometheus name after dot-to-underscore conversion) — gauge, values `0`=HEALTHY, `1`=DEGRADED, `2`=FAILED
- New method: `JoxetteMetrics.registerSinkStateGauge(Supplier<Integer> stateOrdinalSupplier)`

#### Steps

- [ ] 1. Write the test first. It references `DuckLakeWriteChannel.SinkState` (legal — the test class lives in the same package, `com.joxette.recording`, and package-private nested types are visible to same-package classes) and reads the gauge back from a `SimpleMeterRegistry`, exactly like `TopicRecorderTest.java:59,102-104` constructs `DuckLakeWriteChannel` in tests.

`SinkState` only transitions to DEGRADED/FAILED from inside the drain loop on a genuinely retryable DuckDB write failure (`processBatch`, `DuckLakeWriteChannel.java:268-296`) — reaching those states deterministically from a unit test would require fault-injecting the JDBC layer, which is unnecessary complexity for what this test needs to verify (the gauge wiring, not the retry state machine). Since `SinkState` has no production setter beyond `resetSinkState()` (which only ever sets HEALTHY), the test drives transitions directly via the private `sinkState` field through reflection — a test-only technique, not a change to production code.

Create `joxette-service/src/test/java/com/joxette/recording/DuckLakeWriteChannelSinkStateGaugeTest.java`:

```java
package com.joxette.recording;

import com.joxette.config.JoxetteProperties;
import com.joxette.metrics.JoxetteMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@code joxette.sink.state} gauge (Task C, ops-deployment-readiness plan)
 * tracks {@link DuckLakeWriteChannel}'s internal {@code SinkState} through all three
 * values, using the exact retained-supplier pattern documented in
 * {@link JoxetteMetrics#registerSinkStateGauge} (see also the class javadoc's note on
 * the past weak-reference NaN bug this pattern exists to prevent).
 *
 * <p>{@code SinkState} only transitions to DEGRADED/FAILED from inside the drain loop
 * on a real (retryable) DuckDB write failure — reaching those states deterministically
 * from a unit test would require fault-injecting the JDBC layer. Since this test's
 * purpose is to verify the *gauge wiring*, not the retry state machine (which is
 * exercised indirectly wherever {@code DuckLakeWriteChannel} is used end-to-end), it
 * drives the transitions directly via the private {@code sinkState} field — reflection
 * is used here only because {@code SinkState} intentionally has no production setter
 * beyond {@link DuckLakeWriteChannel#resetSinkState()} (which only ever sets HEALTHY).
 *
 * <p>No {@code Awaitility} is needed: the gauge is a synchronous pull — reading it calls
 * the supplier immediately on the calling (test) thread, with no cross-thread handoff.
 */
class DuckLakeWriteChannelSinkStateGaugeTest {

    private SimpleMeterRegistry registry;
    private JoxetteMetrics metrics;
    private Connection duckDB;
    private DuckLakeWriteChannel writeChannel;

    @BeforeEach
    void setUp() throws Exception {
        registry = new SimpleMeterRegistry();
        metrics = new JoxetteMetrics(registry);
        duckDB = DriverManager.getConnection("jdbc:duckdb:");
        JoxetteProperties props = new JoxetteProperties();
        writeChannel = new DuckLakeWriteChannel(duckDB, props, new CassetteRecordingBus(props), metrics);
        writeChannel.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        writeChannel.stop();
        duckDB.close();
    }

    @Test
    void gaugeTracksSinkStateTransitions() throws Exception {
        assertThat(gaugeValue()).as("initial state is HEALTHY").isEqualTo(0.0);

        setSinkState(DuckLakeWriteChannel.SinkState.DEGRADED);
        assertThat(gaugeValue()).as("DEGRADED").isEqualTo(1.0);

        setSinkState(DuckLakeWriteChannel.SinkState.FAILED);
        assertThat(gaugeValue()).as("FAILED").isEqualTo(2.0);

        writeChannel.resetSinkState();
        assertThat(gaugeValue()).as("reset back to HEALTHY").isEqualTo(0.0);
    }

    private double gaugeValue() {
        return registry.get("joxette.sink.state").gauge().value();
    }

    @SuppressWarnings("unchecked")
    private void setSinkState(DuckLakeWriteChannel.SinkState state) throws Exception {
        Field field = DuckLakeWriteChannel.class.getDeclaredField("sinkState");
        field.setAccessible(true);
        ((AtomicReference<DuckLakeWriteChannel.SinkState>) field.get(writeChannel)).set(state);
    }
}
```

- [ ] 2. Run it — expect a compile failure, since `registerSinkStateGauge` doesn't exist yet:

```
mvn -pl joxette-service test -Dtest=DuckLakeWriteChannelSinkStateGaugeTest
```

Expected: `cannot find symbol: method registerSinkStateGauge(...)` in `JoxetteMetrics` (the test references a method that Step 3 has not added yet — this is the expected red state).

- [ ] 3. Implement the gauge in `JoxetteMetrics.java`. Add a bullet to the class javadoc (after the `joxette.recorder.restarts` bullet, original line 30):

```java
 *   <li>{@code joxette.recorder.restarts} — per-topic recorder restart counter</li>
 *   <li>{@code joxette.sink.state}        — {@code DuckLakeWriteChannel} sink health (gauge):
 *       0=HEALTHY, 1=DEGRADED, 2=FAILED. See {@code docs/write-resilience.md}. Micrometer
 *       gauge tags are fixed at registration time, not evaluated per-scrape, so the state
 *       name is not exposed as a dynamic label — only the numeric encoding is; the
 *       human-readable name remains available via {@code /health}.</li>
```

Add the method, right after `registerWriteChannelDepthGauge(...)` (original lines 206-211):

```java
    /**
     * Registers a gauge for the {@code DuckLakeWriteChannel} sink health state.
     * Called once at startup by {@link com.joxette.recording.DuckLakeWriteChannel}, which
     * maps its package-private {@code SinkState} enum to the numeric encoding itself
     * (0=HEALTHY, 1=DEGRADED, 2=FAILED) before handing over the supplier — that enum isn't
     * visible outside {@code com.joxette.recording}, and doesn't need to be.
     */
    public void registerSinkStateGauge(Supplier<Integer> stateOrdinalSupplier) {
        if (registeredGaugeIds.add("sink:state")) {
            retainedGaugeState.add(stateOrdinalSupplier);
            Gauge.builder("joxette.sink.state", stateOrdinalSupplier, Supplier::get)
                    .description("DuckLakeWriteChannel sink health: 0=HEALTHY, 1=DEGRADED, 2=FAILED")
                    .register(registry);
        }
    }
```

- [ ] 4. Wire it up in `DuckLakeWriteChannel.java`. In `start()` (original lines 91-100), register the gauge alongside the existing write-channel-depth gauge:

```java
    void start() throws SQLException {
        channel = Channel.newBufferedChannel(capacity);
        joxetteMetrics.registerWriteChannelDepthGauge(() -> inFlight.size());
        joxetteMetrics.registerSinkStateGauge(() -> sinkStateOrdinal(sinkState.get()));
        WriterSet writers = new WriterSet(duckDbConnection);
        drainThread = Thread.ofVirtual()
                .name("joxette-write-drain")
                .start(() -> drain(writers));
        log.info("DuckLakeWriteChannel started (capacity={})", capacity);
    }
```

Add the ordinal-mapping helper, right after `rootMessage(...)` (original lines 313-317):

```java
    /** Numeric encoding for {@code joxette.sink.state} — see {@link JoxetteMetrics#registerSinkStateGauge}. */
    private static int sinkStateOrdinal(SinkState state) {
        return switch (state) {
            case HEALTHY -> 0;
            case DEGRADED -> 1;
            case FAILED -> 2;
        };
    }
```

- [ ] 5. Re-run the test — verified real output:

```
mvn -pl joxette-service test -Dtest=DuckLakeWriteChannelSinkStateGaugeTest
```

```
[INFO] Running com.joxette.recording.DuckLakeWriteChannelSinkStateGaugeTest
14:50:57.748 [main] INFO com.joxette.recording.DuckLakeWriteChannel -- DuckLakeWriteChannel started (capacity=128)
14:50:57.874 [main] INFO com.joxette.recording.DuckLakeWriteChannel -- DuckLakeWriteChannel: sink state reset to HEALTHY (was FAILED)
14:50:57.882 [joxette-write-drain] INFO com.joxette.recording.DuckLakeWriteChannel -- Write drain loop finished
14:50:57.914 [main] INFO com.joxette.recording.DuckLakeWriteChannel -- DuckLakeWriteChannel stopped
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 2.599 s -- in com.joxette.recording.DuckLakeWriteChannelSinkStateGaugeTest
[INFO] BUILD SUCCESS
```

- [ ] 6. Run `mvn -pl joxette-service test-compile` to confirm the change doesn't break any other file that touches `JoxetteMetrics` or `DuckLakeWriteChannel` — verified `BUILD SUCCESS`.

- [ ] 7. Commit: `feat(metrics): export DuckLakeWriteChannel sink state as joxette_sink_state gauge`

---

### Task D: Basic PrometheusRule alerting

No `PrometheusRule` CR exists anywhere in the repo (`grep -ri prometheusrule deploy/ joxette-operator/src` returns nothing) — only the `ServiceMonitor` scrape config at `deploy/helm/joxette/templates/servicemonitor.yaml`. This task adds one, gated the same way `servicemonitor.yaml` is gated (a boolean under a dedicated values key, default matching the task brief's request), with alerts covering: consumer lag, write-channel depth, and the two `joxette_sink_state` thresholds Task C just added. Every metric name below was read directly out of `JoxetteMetrics.java`, not guessed:

| Alert | Metric (from `JoxetteMetrics.java`) | Prometheus name |
|---|---|---|
| `JoxetteConsumerLagHigh` | `joxette.consumer.lag` (line 173, tag `topic`) | `joxette_consumer_lag` |
| `JoxetteWriteChannelNearCapacity` | `joxette.write.channel.depth` (line 208) | `joxette_write_channel_depth` |
| `JoxetteSinkNotHealthy` / `JoxetteSinkFailed` | `joxette.sink.state` (Task C) | `joxette_sink_state` |

**Files:**
- Modify: `deploy/helm/joxette/values.yaml` (append `monitoring.prometheusRule` block, after the existing `serviceMonitor` block, original lines 155-159)
- Create: `deploy/helm/joxette/templates/prometheusrule.yaml`
- Test (Helm): `helm template` output shown in Step 3

**Interfaces:**
- New Helm values keys: `monitoring.prometheusRule.enabled` (default `true`), `monitoring.prometheusRule.labels` (default `{}`), `monitoring.prometheusRule.consumerLagThreshold` (default `10000`), `monitoring.prometheusRule.writeChannelDepthThreshold` (default `115`)
- New template `prometheusrule.yaml`, `kind: PrometheusRule`, alert names `JoxetteConsumerLagHigh`, `JoxetteWriteChannelNearCapacity`, `JoxetteSinkNotHealthy`, `JoxetteSinkFailed`

Note on the `monitoring.*` nesting: the existing `serviceMonitor` key is top-level (`.Values.serviceMonitor`, not `.Values.monitoring.serviceMonitor`). This task does not rename or move it — only the new `PrometheusRule` gate goes under `monitoring.prometheusRule`, per the task brief's explicit key path. The inconsistency between the two top-level shapes is intentional and left as-is rather than risking a breaking rename of an existing key.

#### Steps

- [ ] 1. Add the new values block. Edit `deploy/helm/joxette/values.yaml`, appending after the `serviceMonitor:` block (original lines 155-159):

```yaml
# Prometheus Operator ServiceMonitor (scrapes /actuator/prometheus).
serviceMonitor:
  enabled: false
  interval: 30s
  labels: {}

# Prometheus Operator alerting rules (PrometheusRule CR). Metric names must
# match JoxetteMetrics.java exactly (Micrometer dots become Prometheus
# underscores on scrape, e.g. joxette.consumer.lag -> joxette_consumer_lag).
monitoring:
  prometheusRule:
    enabled: true
    labels: {}
    # Sustained (5m) joxette_consumer_lag above this fires a warning.
    consumerLagThreshold: 10000
    # Sustained (5m) joxette_write_channel_depth above this fires a warning.
    # Default assumes the default joxette.threading.write-channel-capacity (128)
    # in application.yml — set to ~90% of your configured capacity if overridden.
    writeChannelDepthThreshold: 115
```

- [ ] 2. Add the template, mirroring `servicemonitor.yaml`'s CRD-targeting convention (`apiVersion: monitoring.coreos.com/v1`, `joxette.labels` on the object). Literal `{{ $labels.topic }}` / `{{ $value }}` (Prometheus's own templating inside `annotations`, evaluated at alert-fire time, not by Helm) is escaped with the standard Helm raw-string trick — `` {{`{{ $labels.topic }}`}} `` — so Helm emits it verbatim instead of trying to resolve `$labels` itself.

Create `deploy/helm/joxette/templates/prometheusrule.yaml`:

```yaml
{{- if .Values.monitoring.prometheusRule.enabled }}
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: {{ include "joxette.fullname" . }}
  labels:
    {{- include "joxette.labels" . | nindent 4 }}
    {{- with .Values.monitoring.prometheusRule.labels }}{{ toYaml . | nindent 4 }}{{- end }}
spec:
  groups:
    - name: joxette.rules
      rules:
        - alert: JoxetteConsumerLagHigh
          expr: joxette_consumer_lag > {{ .Values.monitoring.prometheusRule.consumerLagThreshold }}
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "Joxette consumer lag is high on topic {{`{{ $labels.topic }}`}}"
            description: "joxette_consumer_lag has been above {{ .Values.monitoring.prometheusRule.consumerLagThreshold }} for 5 minutes on topic {{`{{ $labels.topic }}`}} (current value: {{`{{ $value }}`}})."
        - alert: JoxetteWriteChannelNearCapacity
          expr: joxette_write_channel_depth > {{ .Values.monitoring.prometheusRule.writeChannelDepthThreshold }}
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "Joxette DuckDB write-channel buffer is near capacity"
            description: "joxette_write_channel_depth has been above {{ .Values.monitoring.prometheusRule.writeChannelDepthThreshold }} for 5 minutes (current value: {{`{{ $value }}`}}) — the write-channel backpressure valve (joxette.threading.write-channel-capacity) is close to full. Kafka consumer lag will keep rising until DuckDB writes catch up."
        - alert: JoxetteSinkNotHealthy
          expr: joxette_sink_state != 0
          for: 2m
          labels:
            severity: warning
          annotations:
            summary: "Joxette DuckLakeWriteChannel sink is not HEALTHY"
            description: "joxette_sink_state has been non-zero (0=HEALTHY, 1=DEGRADED, 2=FAILED) for 2 minutes (current value: {{`{{ $value }}`}}). Consumers may be paused pending object-store recovery — see docs/write-resilience.md."
        - alert: JoxetteSinkFailed
          expr: joxette_sink_state == 2
          for: 0m
          labels:
            severity: critical
          annotations:
            summary: "Joxette DuckLakeWriteChannel sink is FAILED"
            description: "joxette_sink_state == 2 (FAILED): max write retries against the object store were exhausted. The recorder actor should be restarting the sink per docs/write-resilience.md; if this persists, investigate object storage connectivity immediately."
{{- end }}
```

- [ ] 3. Render it. Command and exact verified output:

```
helm template joxette deploy/helm/joxette --set monitoring.prometheusRule.enabled=true -s templates/prometheusrule.yaml
```

```yaml
---
# Source: joxette/templates/prometheusrule.yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: joxette
  labels:
    helm.sh/chart: joxette-0.1.0
    app.kubernetes.io/name: joxette
    app.kubernetes.io/instance: joxette
    app.kubernetes.io/version: "0.1.0"
    app.kubernetes.io/managed-by: Helm
spec:
  groups:
    - name: joxette.rules
      rules:
        - alert: JoxetteConsumerLagHigh
          expr: joxette_consumer_lag > 10000
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "Joxette consumer lag is high on topic {{ $labels.topic }}"
            description: "joxette_consumer_lag has been above 10000 for 5 minutes on topic {{ $labels.topic }} (current value: {{ $value }})."
        - alert: JoxetteWriteChannelNearCapacity
          expr: joxette_write_channel_depth > 115
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "Joxette DuckDB write-channel buffer is near capacity"
            description: "joxette_write_channel_depth has been above 115 for 5 minutes (current value: {{ $value }}) — the write-channel backpressure valve (joxette.threading.write-channel-capacity) is close to full. Kafka consumer lag will keep rising until DuckDB writes catch up."
        - alert: JoxetteSinkNotHealthy
          expr: joxette_sink_state != 0
          for: 2m
          labels:
            severity: warning
          annotations:
            summary: "Joxette DuckLakeWriteChannel sink is not HEALTHY"
            description: "joxette_sink_state has been non-zero (0=HEALTHY, 1=DEGRADED, 2=FAILED) for 2 minutes (current value: {{ $value }}). Consumers may be paused pending object-store recovery — see docs/write-resilience.md."
        - alert: JoxetteSinkFailed
          expr: joxette_sink_state == 2
          for: 0m
          labels:
            severity: critical
          annotations:
            summary: "Joxette DuckLakeWriteChannel sink is FAILED"
            description: "joxette_sink_state == 2 (FAILED): max write retries against the object store were exhausted. The recorder actor should be restarting the sink per docs/write-resilience.md; if this persists, investigate object storage connectivity immediately."
```

Note the `{{ $labels.topic }}` / `{{ $value }}` come through as literal text in the rendered output — exactly as intended, since Prometheus (not Helm) evaluates those at alert-fire time. Also run `helm lint deploy/helm/joxette` (`0 chart(s) failed`) and a full `helm template joxette deploy/helm/joxette` to confirm the whole chart still renders (exit `0`).

- [ ] 4. Commit: `feat(ops): add PrometheusRule alerts for consumer lag, write-channel depth, and sink state`

---

### Task E: HPA for the replay tier reacts to actual replay load

`deploy/helm/joxette/templates/hpa-replay.yaml` (original lines 23-29) scales the replay `Deployment` on CPU alone. Replay is I/O-bound (SSE/NDJSON fan-out to slow clients over `EntityReplayService`/`TopicReplayService`) — many concurrent slow streams can saturate a pod's I/O and memory footprint while CPU stays low, so CPU-only scaling under-provisions exactly the load pattern replay produces. `JoxetteMetrics.java` already tracks this via `registerActiveReplaysGauge` (line 340-347), registered as `joxette.replay.active` — Prometheus name `joxette_replay_active`. This task adds it to the HPA's `metrics` list as a second, independent signal; HPA v2 computes a desired replica count per metric and scales to the **max** across all of them, so this can only ever scale out *further* than CPU alone, never override it.

**Files:**
- Modify: `deploy/helm/joxette/templates/hpa-replay.yaml` (`metrics:` block, original lines 23-29)
- (values keys `tiers.replay.hpa.targetActiveReplaysPerPod` were already added in Task A, Steps 1-2, since they live in the same `tiers.replay.hpa` block)
- Test (Helm): `helm template` output shown in Step 2

**Interfaces:**
- HPA `metrics` list gains a `type: Pods` entry targeting `joxette_replay_active`
- Consumed values key: `tiers.replay.hpa.targetActiveReplaysPerPod` (added in Task A; default `10` in `values.yaml`, `10` in `values-cluster.yaml`)

**Prerequisite (documented, not installed by this chart):** a `type: Pods` HPA metric resolves through the Kubernetes custom metrics API (`custom.metrics.k8s.io`), which requires a metrics adapter — typically `prometheus-adapter` — configured to expose `joxette_replay_active` from Prometheus. `values.yaml` did not previously document any metrics-adapter assumption (the existing CPU metric only needs `metrics-server`, which is far more commonly pre-installed); this task adds an explicit comment calling that out so operators enabling this HPA don't discover the gap at scale-out time.

#### Steps

- [ ] 1. Edit `deploy/helm/joxette/templates/hpa-replay.yaml`, replacing the `metrics:` block (original lines 23-29):

```yaml
  minReplicas: {{ .Values.tiers.replay.hpa.minReplicas }}
  maxReplicas: {{ .Values.tiers.replay.hpa.maxReplicas }}
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: {{ .Values.tiers.replay.hpa.targetCPUUtilizationPercentage }}
    {{- /*
    Pods metric: average joxette_replay_active (active replay-to-topic ops) per
    pod. Replay is I/O-bound (SSE/NDJSON fan-out to slow clients) — many
    concurrent streams can saturate a pod at low CPU, which the Resource metric
    above misses entirely. HPA computes a desired replica count per metric and
    scales to the MAX of them, so this only ever scales *out* further than CPU
    alone would, never in place of it.
    Prerequisite: a metrics adapter (e.g. prometheus-adapter) must be installed
    and configured to expose joxette_replay_active via the custom metrics API
    (custom.metrics.k8s.io) for a `type: Pods` metric to resolve. This chart
    does not install or configure the adapter.
    */}}
    - type: Pods
      pods:
        metric:
          name: joxette_replay_active
        target:
          type: AverageValue
          averageValue: {{ .Values.tiers.replay.hpa.targetActiveReplaysPerPod | quote }}
{{- end }}
```

(The `targetActiveReplaysPerPod` key referenced here was already added to both `values.yaml` and `values-cluster.yaml` in Task A, Steps 1-2, since it lives inside the same `tiers.replay.hpa` block those steps touch. If Task E is implemented standalone without Task A having landed, add `targetActiveReplaysPerPod: 10` under `tiers.replay.hpa` in both files first.)

- [ ] 2. Render it against the cluster overlay (the only values file with `hpa.enabled: true`). Command and exact verified output:

```
helm template joxette deploy/helm/joxette -f deploy/helm/joxette/values-cluster.yaml -s templates/hpa-replay.yaml
```

```yaml
---
# Source: joxette/templates/hpa-replay.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: joxette-replay
  labels:
    helm.sh/chart: joxette-0.1.0
    app.kubernetes.io/name: joxette
    app.kubernetes.io/instance: joxette
    app.kubernetes.io/version: "0.1.0"
    app.kubernetes.io/managed-by: Helm
    app.kubernetes.io/component: replay
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: joxette-replay
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Pods
      pods:
        metric:
          name: joxette_replay_active
        target:
          type: AverageValue
          averageValue: "10"
```

With the default `values.yaml` (`hpa.enabled: false`, embedded catalog), the template renders nothing — confirm with `helm template joxette deploy/helm/joxette -s templates/hpa-replay.yaml`, which prints no document (the `{{- if and (ne .Values.catalog.backend "embedded") .Values.tiers.replay.enabled .Values.tiers.replay.hpa.enabled }}` guard, already present and untouched by this task, still gates the whole file). `helm lint deploy/helm/joxette -f deploy/helm/joxette/values-cluster.yaml` reports `0 chart(s) failed`.

- [ ] 3. Commit: `feat(ops): scale the replay tier HPA on active replay count, not CPU alone`

---

## Self-review checklist

- Every finding from the brief has a task: operator probes/resources → Task A; PDB → Task B; sink-state gauge → Task C; PrometheusRule → Task D; replay HPA → Task E.
- Placeholder-phrase grep (`TODO`, `FIXME`, `<...>`, `xxx`, `TBD`) over this plan's content returns nothing — every path, template name, values key, and Java identifier above was read from or verified against the real files (`Read`, `grep`, `javap` against the resolved fabric8 7.8.0 jars, and live `mvn`/`helm` runs).
- Values-key/metric-name consistency across tasks: `tiers.replay.hpa.targetActiveReplaysPerPod` is introduced once (Task A, since it shares a YAML block with the `limits` changes) and consumed once (Task E) — no duplicate definition. `joxette_sink_state` is introduced once (Task C) and consumed twice (Task D's two sink alerts) with matching semantics (0/1/2) in both places. `joxette_consumer_lag` and `joxette_write_channel_depth` in Task D match the exact Micrometer names registered in `JoxetteMetrics.java` (`joxette.consumer.lag` line 173, `joxette.write.channel.depth` line 208), converted to Prometheus form the same way Micrometer's `PrometheusMeterRegistry` does it (dots to underscores) — verified against `servicemonitor.yaml`'s existing `/actuator/prometheus` scrape path, which is what produces that conversion at runtime.
- Global Constraints are satisfied by every new/modified manifest: Task A adds `resources.requests` + `resources.limits` and all three probes to both operator workload shapes and both Helm values files; Task B's PDB does not touch replica counts; no task anywhere sets or permits `replicas > 1` on the embedded StatefulSet — `joxette.validate` and `CatalogGuardrail` are both read-only referenced, never modified.
- All Java changes were actually compiled and their tests actually run against this exact code during planning (not merely reasoned about): `mvn -pl joxette-operator test` → 50/50 passing; `mvn -pl joxette-service test -Dtest=DuckLakeWriteChannelSinkStateGaugeTest` → 1/1 passing; `mvn -pl joxette-service test-compile` → clean. All Helm changes were actually rendered: `helm lint` (both values files) → 0 failed; every `helm template` fragment shown above is copy-pasted from a real command run, not hand-typed.
