# Joxette Kubernetes Operator — Design

> **Status: design proposal.** No operator code exists yet. This document
> specifies the CRDs, reconciliation behaviour, and deployment model for a
> Joxette operator built with the [Java Operator SDK
> (JOSDK)](https://javaoperatorsdk.io/). It assumes familiarity with
> [`clustering-deployment.md`](clustering-deployment.md), which describes the
> concurrency model, instance roles, and the catalog single-writer constraint
> that this operator must respect.

---

## 1. Why an operator

Joxette has real operational shape that a plain Helm chart cannot safely encode:

- **A hard scaling gate** — an embedded DuckDB catalog is single-writer per file;
  a second pod against the same file corrupts it. Whether horizontal scaling is
  even *legal* depends on the catalog backend URI. An operator can **enforce**
  this; a chart can only document it.
- **Role-tiered topology** — `recorder` / `entity-router` / `compaction` /
  `replay` map to different workload shapes and scaling rules (compaction must be
  a single active node; replay is freely horizontal).
- **Declarative recording config** — which topics and entity types are recorded
  is runtime state managed through the REST API. An operator can reconcile that
  from `RecordedTopic` / `EntityType` custom resources, giving GitOps control.
- **Status surfacing** — `/health` and `/instances/cluster-state` expose
  consumer lag, recorder liveness, and cluster membership that belong in CR
  status and Kubernetes events.

The operator is **namespaced** and built on JOSDK so it can reuse Joxette's
served OpenAPI (`/v3/api-docs`) to generate its REST client.

---

## 2. Two clustering tracks

Joxette's Pekko setup today self-joins a one-member cluster per process
(`seed-nodes = []` + a programmatic `Join`; see `PekkoConfig`). That has a direct
consequence: the `ClusterSingleton` compaction actor is **per-process**, and
cross-process compaction safety actually comes from the `compaction_locks`
catalog table — *not* from Pekko. The operator must understand this, and it
supports two modes via `spec.clustering.mode`:

| | **Track A — `catalog`** (works on current code) | **Track B — `pekko-management`** (recommended target) |
|---|---|---|
| Cluster formation | each pod self-joins; no shared cluster | Cluster Bootstrap via `kubernetes-api` discovery |
| Compaction exclusivity | `compaction_locks` table **+ operator pins one compaction pod** | Lease-backed `ClusterSingleton` (cluster-wide, correct) |
| `/instances/topology` | one member per pod | true multi-member topology |
| App changes required | **none** | add pekko-management deps, start the extensions, drop the self-join, expose the management port, grant RBAC (see [§8 Phase 0](#8-phase-0--app-side-prerequisites)) |
| Horizontal scaling | shared catalog (Quack/Postgres) + Kafka consumer group | same, plus genuine singleton failover |

**The catalog single-writer rule is enforced in both modes** — it is independent
of Pekko. Track B does not change *whether* you can scale; it changes how
compaction exclusivity and cluster membership are achieved once you do.

The operator targets Track B but ships Track A as the zero-app-change default so
it is useful against the current build.

---

## 3. Custom resources — `joxette.dev/v1alpha1`

Three kinds. `JoxetteCluster` owns Kubernetes workloads; `RecordedTopic` and
`EntityType` reconcile into a running cluster's REST API.

### 3.1 `JoxetteCluster`

Owns the Deployments/StatefulSet, Service, ConfigMap, ServiceAccount, RBAC, and
(optionally) ServiceMonitor for one Joxette installation.

```yaml
apiVersion: joxette.dev/v1alpha1
kind: JoxetteCluster
metadata:
  name: prod
  namespace: joxette
spec:
  image: ghcr.io/acme/joxette-service:0.1.0      # see Phase 0 — image must exist
  clustering:
    mode: pekko-management                        # pekko-management | catalog

  catalog:
    backend: postgresql                           # embedded | quack | postgresql
    # embedded:
    #   path: /data/joxette.ducklake
    #   pvc: { size: 20Gi, storageClass: gp3 }
    uri: postgresql://pg.joxette.svc/joxette      # quack:// or postgresql://
    objectStoragePath: s3://joxette-data/         # Parquet data path (always remote)

  objectStore:                                    # S3 for catalog httpfs + snapshot export
    endpoint: https://s3.us-east-1.amazonaws.com  # omit for AWS default
    region: us-east-1
    bucket: joxette-data
    secretRef: { name: joxette-s3 }               # keys: access-key, secret-key

  kafka:
    bootstrapServers: kafka-bootstrap.kafka.svc:9092
    consumerGroup: joxette-recorder
    groupProtocol: consumer                       # KIP-848 cooperative
    secretRef: { name: joxette-kafka }            # optional SASL/SSL material

  tiers:
    recorder:   { enabled: true,  replicas: 3, resources: { requests: { cpu: 1, memory: 2Gi } } }
    replay:     { enabled: true,  replicas: 2, resources: { requests: { cpu: 500m, memory: 1Gi } } }
    compaction: { enabled: true }                 # replicas always 1 (operator-pinned)

  podOverrides:                                   # applied to every tier
    nodeSelector: {}
    tolerations: []
    annotations: {}

status:
  phase: Ready                                    # Pending | Progressing | Ready | Degraded | Rejected
  catalogBackend: POSTGRESQL
  observedGeneration: 7
  tiers:
    recorder:   { desired: 3, ready: 3 }
    replay:     { desired: 2, ready: 2 }
    compaction: { desired: 1, ready: 1 }
  clusterHealth:                                  # polled from /health and /instances/cluster-state
    activeRecorders: ["orders.events", "payments.events"]
    instanceCount: 6
    alive: 6
    stale: 0
    totalConsumerLag: 1423
  conditions:
    - { type: Ready,        status: "True",  reason: AllTiersReady }
    - { type: Progressing,  status: "False", reason: Stable }
    - { type: Degraded,     status: "False" }
    - { type: CatalogConstraintViolated, status: "False" }
```

### 3.2 `RecordedTopic`

Declarative topic recording, reconciled into the cluster's `/topics` API.

```yaml
apiVersion: joxette.dev/v1alpha1
kind: RecordedTopic
metadata:
  name: orders-events
  namespace: joxette
spec:
  clusterRef: { name: prod }
  topic: orders.events
  mode: both                      # general | entity_only | both
  startFrom: latest               # latest | earliest
  brokerId: default               # null → default broker
  createIfAbsent:                 # optional — maps to CreateTopicRequest fields
    partitions: 12
    replicationFactor: 3
  matchers:                       # general-cassette message-type tagging
    - { messageType: OrderCreated, source: value, expression: "$.type" }
  deletionPolicy: Pause           # Delete | Pause | Orphan
status:
  registered: true
  recorderRunning: true
  consumerLag: 12
  observedGeneration: 3
  conditions:
    - { type: Ready, status: "True", reason: Recording }
```

### 3.3 `EntityType`

```yaml
apiVersion: joxette.dev/v1alpha1
kind: EntityType
metadata:
  name: order
  namespace: joxette
spec:
  clusterRef: { name: prod }
  type: order
  buckets: 256
  sources:
    - topic: orders.events
      mode: both
      matchers:
        - { messageType: OrderCreated, source: value, expression: "$.order_id" }
    - topic: payments.events
      mode: entity_only
      matchers:
        - { messageType: PaymentSettled, source: value, expression: "$.payment.order_id" }
  deletionPolicy: Orphan
status:
  registered: true
  sourceCount: 2
  observedGeneration: 1
  conditions:
    - { type: Ready, status: "True", reason: Configured }
```

> **Why not `joxette.bootstrap`?** The YAML `bootstrap` block seeds config tables
> **only when they are empty**. It is not a reconciliation surface. Declarative,
> drift-correcting management therefore goes through the REST API, which is what
> `RecordedTopic`/`EntityType` reconcile into.

---

## 4. Tier → workload mapping

| Tier | `joxette.roles` | Workload (shared catalog) | Workload (embedded) | Scaling |
|---|---|---|---|---|
| recorder | `recorder, entity-router` | `Deployment` | folded into the single all-in-one `StatefulSet` | up to Kafka partition count (consumer group balances; KIP-848) |
| replay | `replay` | `Deployment` + `Service` | folded in | horizontal / HPA-friendly (stateless) |
| compaction | `compaction` | `Deployment`, `replicas: 1` | folded in | always 1 |

`JoxetteCluster` owns, via **JOSDK Dependent Resources**:

- the tier workloads above;
- a **Service** exposing HTTP `8080` (the REST API target for `RecordedTopic`/
  `EntityType` reconcilers; resolved as `<name>.<ns>.svc:8080`). Track B adds a
  **headless** Service so each pod is individually addressable for Pekko
  remoting/discovery;
- a **ConfigMap** with the non-secret `joxette.*` tree plus a `pekko.conf` /
  `application.conf` overlay (Track B: discovery + lease + canonical hostname);
- a **ServiceAccount** + **Role/RoleBinding**. Track B grants the pods
  `pods: [get, watch, list]` (discovery) and
  `coordination.k8s.io/leases: [get, create, update, list]` (SBR + singleton lease);
- an optional **ServiceMonitor** scraping `/actuator/prometheus`.

Secrets (Kafka SASL/SSL, S3 keys) are **referenced via `secretRef`, never inlined** —
projected as env vars, or mounted as files for SSL keystores/truststores.

Config flows as env vars using Spring relaxed binding, e.g. `JOXETTE_ROLES`,
`JOXETTE_CATALOG_PATH`, `JOXETTE_KAFKA_BOOTSTRAP-SERVERS`, `JOXETTE_PEKKO_REMOTE-PORT`.

---

## 5. The catalog / scaling guardrail (core safety)

This is the operator's reason to exist. Enforcement is two-layered:

1. **Primary — a validating admission webhook** (JOSDK admission-controller
   support) rejects an unsafe `JoxetteCluster` at apply time with a clear message.
2. **Backstop — reconcile-time refusal**: if a spec slips past the webhook, the
   reconciler sets `phase: Rejected` + `CatalogConstraintViolated=True`, emits a
   Warning Event, and **does not** create/scale the offending workload.

| `catalog.backend` | recorder replicas | replay replicas | compaction | Verdict |
|---|---|---|---|---|
| `embedded` | n/a — **single all-in-one pod only** | n/a | n/a | Collapse all tiers into **one `roles: [all]` pod**: `StatefulSet`, `replicas: 1`, `strategy: Recreate`, PVC at `catalog.path`'s parent. Any split tier or `replicas > 1` is **rejected** (a second reader or writer corrupts the file). |
| `quack` / `postgresql` | `1..N` | `1..N` | exactly `1` | Allowed. Recorder replicas above the topic partition count are accepted with a Warning (idle consumers). |
| any | — | — | `replicas > 1` | **Rejected** in Track A; pinned to `1` in Track B (the lease-backed singleton makes >1 safe, but one active node is still the intended design — extra replicas would just stand by). |

Embedded ⇒ `StatefulSet` + PVC + `Recreate`. Shared catalog ⇒ stateless
`Deployment`s, no PVC (the catalog is remote; object storage is always remote).

---

## 6. Reconciliation design (JOSDK)

### 6.1 `JoxetteClusterReconciler`

- `@ControllerConfiguration` with a set of **Dependent Resources**
  (`Deployment`/`StatefulSet`, `Service`, `ConfigMap`, `ServiceAccount`,
  `Role`, `RoleBinding`, optional `ServiceMonitor`). The workload bundle is
  selected from `catalog.backend` × `clustering.mode`.
- Catalog guardrail runs first (webhook + the reconcile backstop in §5).
- **Status polling**: a `PerResourcePollingEventSource` calls the cluster's
  `/health` and `/instances/cluster-state` on an interval and folds the results
  into `status.clusterHealth` and per-tier `ready` counts.
- Standard `Ready` / `Progressing` / `Degraded` conditions + `observedGeneration`.

### 6.2 `RecordedTopicReconciler` / `EntityTypeReconciler`

These reconcile into the **REST API**, not Kubernetes objects:

1. **Resolve** `clusterRef` → Service DNS `<name>.<ns>.svc:8080`. If the
   referenced `JoxetteCluster` is not `Ready`, `UpdateControl.rescheduleAfter(...)`
   and set `Progressing` (`reason: ClusterNotReady`).
2. **Diff & converge** — `GET` current config, compare to spec, then `POST`/`PUT`/
   `DELETE` to converge. All Joxette config writes are upsert-idempotent
   (`PUT /topics/{t}`, `PUT /entities/{type}`, source/matcher sub-resources).
3. **Status** — write `registered`, plus live `recorderRunning` / `consumerLag`
   from `/topics/status`; set `observedGeneration` and conditions.
4. **Finalizer** — on CR delete, apply `deletionPolicy`:
   `Delete` → `DELETE /topics/{t}` (or `/entities/{type}`),
   `Pause` → `POST /topics/{t}/pause`,
   `Orphan` → remove the finalizer and leave server state untouched.
5. **Drift detection** — the periodic resync re-asserts desired state, correcting
   any out-of-band REST edits; `GET /config/runtime` provides a cheap
   counts-level cross-check.

REST client: generated from the served OpenAPI (`/v3/api-docs`, "Joxette API"
v0.1.0), or a thin hand-rolled client over the dozen endpoints used.

Cross-cutting: informer-backed event sources, exponential retry/backoff on REST
failures, and conditions that never silently swallow an upstream error (failed
converge → `Degraded` with the HTTP status/detail).

---

## 7. Supporting concerns

**Operator RBAC** (the operator's own ServiceAccount):
`apps`: `deployments`, `statefulsets` (CRUD);
`core`: `services`, `configmaps`, `serviceaccounts`, `events` (CRUD), `pods` (read);
`rbac.authorization.k8s.io`: `roles`, `rolebindings` (CRUD — to provision Track B
pod RBAC);
`monitoring.coreos.com`: `servicemonitors` (CRUD, optional);
`joxette.dev`: the three CRs + their `/status`.

**Probes** (HTTP `8080`):
`startupProbe` → `/actuator/health` (generous `failureThreshold` for catalog
`ATTACH`); `readinessProbe` → `/actuator/health/readiness` (the `catalog`
component gates traffic); `livenessProbe` → `/actuator/health/liveness`. Set
`management.endpoint.health.probes.enabled=true` if not on auto-detected
Kubernetes. Track B additionally exposes pekko-management health at `/ready` and
`/alive` on port **7626** (supplementary — readiness for traffic still uses 8080).

**Shutdown**: `terminationGracePeriodSeconds: 45` — Joxette runs an ordered
~20–30 s SIGTERM teardown (interrupt SSE/background VTs → stop recorders → drain
write channel → Pekko coordinated-shutdown).

**Secrets**: Kafka SASL/SSL and S3 keys come from referenced `Secret`s only.

**Multi-tenancy**: all CRs are namespaced; `clusterRef` is namespace-local. The
operator can watch all namespaces or be scoped to one.

**Upgrades / rollout**: shared-catalog tiers use `RollingUpdate`; the embedded
StatefulSet uses `Recreate` (single writer — no overlap permitted). An `image`
bump is an ordinary spec change → reconcile.

---

## 8. Phase 0 — app-side prerequisites (DONE)

These were **changes to `joxette-service`**, not the operator. Both are now
implemented and on `main`.

### 8.1 Container image (required for either track) — ✅ done

The runtime image is produced by **Cloud Native Buildpacks** via
`mvn -pl joxette-service spring-boot:build-image` (no Dockerfile). The
spring-boot-maven-plugin `<image>` config selects JDK 25 (`BP_JVM_VERSION=25`,
BellSoft Liberica) and bakes the preview launcher flags into `JAVA_TOOL_OPTIONS`
via the buildpack BPE mechanism (`--add-opens` in `=` form — the JVM splits
`JAVA_TOOL_OPTIONS` on whitespace). Verified booting on Java 25.0.3 with preview
features. The `docker-compose.yml` service references the built image by name
(`joxette-service:${project.version}`); the operator's `spec.image` points at a
published build of it.

### 8.2 Pekko Management integration (Track B only) — ✅ done

Implemented via `joxette.clustering.mode` (commit `9a9bfa7`). Default
`catalog` keeps the self-join (local dev, tests, embedded catalog);
`pekko-management` engages Cluster Bootstrap. The whole Pekko tree is pinned to
**`2.0.0-M1`** — the only Pekko Management 2.x release on Maven Central — so core
and management share one milestone (Pekko's
[binary-compatibility rules](https://pekko.apache.org/docs/pekko/current/common/binary-compatibility-rules.html)
give no cross-milestone guarantee, so a single pinned milestone is required, not
a mix). Core was moved `M3 → M1` to match; the full suite passed unchanged.

- **Dependencies** (the `_2.13` Scala suffix matches Pekko core's): `pekko-management-cluster-http_2.13`,
  `pekko-management-cluster-bootstrap_2.13`, `pekko-discovery-kubernetes-api_2.13`,
  `pekko-lease-kubernetes_2.13`, all `2.0.0-M1`, managed in the parent POM via
  the `pekko.management.version` property.
- **`PekkoConfig`**: in `pekko-management` mode it overlays the HOCON below onto
  `pekko.conf`, starts `PekkoManagement.get(system).start()` then
  `ClusterBootstrap.get(system).start()`, and **skips the self-join** (membership
  is discovery-driven; `seed-nodes` stays `[]`). In `catalog` mode the original
  self-join path runs unchanged.
- **HOCON overlay** (built by `PekkoConfig.buildManagementOverlay`, unit-tested in
  `PekkoConfigClusteringTest`):
  ```hocon
  pekko.management.http.port = ${joxette.clustering.management-port}   # default 7626
  pekko.management.cluster.bootstrap.contact-point-discovery {
    discovery-method          = kubernetes-api
    service-name              = ${joxette.clustering.service-name}
    required-contact-point-nr = ${joxette.clustering.required-contact-point-nr}
  }
  pekko.discovery.kubernetes-api.pod-label-selector = "app.kubernetes.io/name=%s"  # serviceName
  pekko.cluster.downing-provider-class = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
  pekko.cluster.split-brain-resolver.active-strategy = lease-majority
  pekko.cluster.split-brain-resolver.lease-majority.lease-implementation = "pekko.coordination.lease.kubernetes"
  pekko.coordination.lease.kubernetes.lease-class = "org.apache.pekko.coordination.lease.kubernetes.NativeKubernetesLease"
  pekko.remote.artery.canonical.hostname = ${POD_IP}     # downward API; omitted if POD_IP unset
  ```
- **Config knobs** (`joxette.clustering.*`): `mode`, `management-port` (7626),
  `service-name` (`joxette`), `pod-label-selector` (`app.kubernetes.io/name=%s`),
  `required-contact-point-nr` (1).
- **Ports**: management `7626`; remoting port via `joxette.pekko.remote-port`.
- **Pod RBAC** (provisioned by the operator, see §4): `pods: [get, watch, list]`
  and `coordination.k8s.io/leases: [get, create, update, list]`.
- **Effect**: pods form one shared cluster; the `ClusterSingleton` compaction
  actor becomes genuinely cluster-wide (lease-backed), `/instances/topology`
  reflects real membership, and `compaction_locks` becomes a redundant backstop
  rather than the primary mechanism.

---

## 9. End-to-end example

A production cluster (Track B, PostgreSQL catalog) with two recorded topics and
one entity type:

```yaml
apiVersion: joxette.dev/v1alpha1
kind: JoxetteCluster
metadata: { name: prod, namespace: joxette }
spec:
  image: ghcr.io/acme/joxette-service:0.1.0
  clustering: { mode: pekko-management }
  catalog:
    backend: postgresql
    uri: postgresql://pg.joxette.svc/joxette
    objectStoragePath: s3://joxette-data/
  objectStore: { region: us-east-1, bucket: joxette-data, secretRef: { name: joxette-s3 } }
  kafka:
    bootstrapServers: kafka-bootstrap.kafka.svc:9092
    consumerGroup: joxette-recorder
    groupProtocol: consumer
    secretRef: { name: joxette-kafka }
  tiers:
    recorder:   { enabled: true, replicas: 3 }
    replay:     { enabled: true, replicas: 2 }
    compaction: { enabled: true }
---
apiVersion: joxette.dev/v1alpha1
kind: RecordedTopic
metadata: { name: orders-events, namespace: joxette }
spec:
  clusterRef: { name: prod }
  topic: orders.events
  mode: both
  deletionPolicy: Pause
---
apiVersion: joxette.dev/v1alpha1
kind: RecordedTopic
metadata: { name: payments-events, namespace: joxette }
spec:
  clusterRef: { name: prod }
  topic: payments.events
  mode: entity_only
  deletionPolicy: Pause
---
apiVersion: joxette.dev/v1alpha1
kind: EntityType
metadata: { name: order, namespace: joxette }
spec:
  clusterRef: { name: prod }
  type: order
  buckets: 256
  sources:
    - { topic: orders.events,   mode: both,        matchers: [ { messageType: OrderCreated,  source: value, expression: "$.order_id" } ] }
    - { topic: payments.events, mode: entity_only, matchers: [ { messageType: PaymentSettled, source: value, expression: "$.payment.order_id" } ] }
```

Reconcile sequence: the `JoxetteClusterReconciler` provisions three Deployments
(recorder×3, replay×2, compaction×1), the Service/ConfigMap/RBAC, and Pekko
discovery RBAC; once the cluster reports `Ready`, the two `RecordedTopicReconciler`
loops `POST /topics` for `orders.events` and `payments.events`, and the
`EntityTypeReconciler` `POST /entities` + adds the two source mappings. Each CR's
status then tracks live `recorderRunning` / `consumerLag`.

**Contrast — embedded, single pod (guardrail in action):**

```yaml
apiVersion: joxette.dev/v1alpha1
kind: JoxetteCluster
metadata: { name: dev, namespace: joxette }
spec:
  image: ghcr.io/acme/joxette-service:0.1.0
  clustering: { mode: catalog }
  catalog:
    backend: embedded
    path: /data/joxette.ducklake
    pvc: { size: 20Gi }
    objectStoragePath: s3://joxette-dev/
  objectStore: { region: us-east-1, bucket: joxette-dev, secretRef: { name: joxette-s3 } }
  kafka: { bootstrapServers: kafka:9092 }
  tiers:
    recorder:   { enabled: true }    # replicas ignored — collapsed
    replay:     { enabled: true }    # into one all-in-one
    compaction: { enabled: true }    # StatefulSet, replicas: 1
```

The operator collapses this into a single `roles: [all]` StatefulSet pod with a
PVC and `Recreate` strategy. Setting `recorder.replicas: 2` here is **rejected**
by the webhook with `CatalogConstraintViolated` — the embedded catalog cannot be
shared.

---

## 10. Phasing

- **Phase 0** — prerequisites: container image; (recommended) Pekko-Management
  integration for Track B (§8).
- **Phase 1** — `JoxetteCluster`: dependent resources, tier→workload mapping, the
  catalog guardrail (webhook + reconcile backstop), `/health` status polling.
- **Phase 2** — `RecordedTopic` + `EntityType` API reconcilers: clusterRef
  resolution, diff/converge, finalizers + `deletionPolicy`, drift detection.
- **Phase 3** — polish: `ServiceMonitor`/metrics wiring, HPA for the replay tier,
  admission-webhook hardening, all-namespaces vs scoped operation, generated
  OpenAPI client.

---

## 11. Non-goals

- Provisioning Kafka brokers or the catalog database (Quack server / PostgreSQL)
  — delegate to their own operators; Joxette's operator only *connects*.
- Creating Kafka topics beyond the existing `createIfAbsent` passthrough on
  `RecordedTopic` (Jikkou / a Kafka operator remains the topic-provisioning tool).
- The Joxette UI.
- True multi-node Pekko clustering on the **current** code — that is the Track B
  Phase 0 prerequisite, not something the operator can conjure without the app
  changes in §8.2.

---

See also: [`clustering-deployment.md`](clustering-deployment.md) (roles,
catalog constraint, coordination), [`catalog-scaling.md`](catalog-scaling.md)
(catalog backend migration), [`object-storage.md`](object-storage.md) (S3 + IRSA).
