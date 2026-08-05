# Joxette Helm chart

Deploys `joxette-service` to Kubernetes. The **catalog backend** decides the
deployment shape; the **clustering mode** decides how pods form a Pekko cluster.

| `catalog.backend` | Workload | `clustering.mode` allowed | Scaling |
|---|---|---|---|
| `embedded` (default) | single all-in-one `StatefulSet` (1 replica, PVC, PodDisruptionBudget `maxUnavailable: 0`) | `catalog` only | none — single writer |
| `quack` / `postgresql` | per-tier `Deployment`s (recorder / replay / compaction) | `catalog` or `pekko-management` | recorder/replay horizontal; compaction pinned to 1 |

The chart **enforces the single-writer guardrail**: an embedded catalog with
`replicas > 1`, a split tier, or `pekko-management` mode fails the render with a
clear message (mirrors `docs/operator-design.md` §5). A shared backend without a
`catalog.uri` also fails.

## Quick start

### Local (kind), embedded catalog

```bash
# build the image via Cloud Native Buildpacks (no Dockerfile), then load into kind
mvn -pl joxette-service spring-boot:build-image -Djoxette.image.name=joxette-service:dev
kind load docker-image joxette-service:dev

helm install joxette deploy/helm/joxette -f deploy/helm/joxette/values-kind.yaml
kubectl port-forward svc/joxette 8080:8080
curl http://localhost:8080/health
```

### Scaled-out (shared PostgreSQL catalog + real Pekko cluster)

```bash
helm install joxette deploy/helm/joxette -f deploy/helm/joxette/values-cluster.yaml
```

This needs an existing PostgreSQL catalog (`catalog.uri`) and Secrets for Kafka /
S3. The chart never provisions the catalog DB, Kafka, or the object store — it
only connects to them.

## Clustering modes

- **`catalog`** (default) — each pod self-joins a one-member cluster.
  Cross-process coordination is via the shared catalog (`compaction_locks` +
  heartbeat registry) and the Kafka consumer group. The only safe mode for an
  embedded catalog.
- **`pekko-management`** — Cluster Bootstrap forms one shared cluster via
  `kubernetes-api` discovery (pods found by the label
  `app.kubernetes.io/name=<name>`). The chart then also creates a **headless
  Service**, the **pod RBAC** (`pods: list/watch`, `coordination.k8s.io/leases`),
  injects **`POD_IP`** via the downward API, and configures the lease-backed
  split-brain resolver. The compaction `ClusterSingleton` becomes genuinely
  cluster-wide. Requires a shared catalog backend.

## Key values

| Key | Default | Notes |
|---|---|---|
| `catalog.backend` | `embedded` | `embedded` \| `quack` \| `postgresql` |
| `catalog.uri` | `""` | required for quack/postgresql |
| `catalog.objectStoragePath` | `s3://joxette-data/` | Parquet data root (always remote) |
| `catalog.embedded.persistence.size` | `20Gi` | PVC for the `.ducklake` file |
| `clustering.mode` | `catalog` | `catalog` \| `pekko-management` |
| `clustering.requiredContactPointNr` | `null` | defaults to `tiers.recorder.replicas` |
| `tiers.<t>.replicas` | `1` | recorder/replay; compaction always pinned to 1 |
| `tiers.replay.hpa.enabled` | `false` | autoscale the replay tier (shared backend only; needs metrics-server) |
| `tiers.replay.hpa.{min,max}Replicas` | `2` / `10` | HPA bounds |
| `tiers.replay.hpa.targetCPUUtilizationPercentage` | `70` | HPA CPU target |
| `tiers.replay.hpa.targetActiveReplaysPerPod` | `10` | target avg `joxette_replay_active` per pod, used only when `activeReplaysMetric.enabled` |
| `tiers.replay.hpa.activeReplaysMetric.enabled` | `false` | add the `joxette_replay_active` Pods metric to the HPA; **requires prometheus-adapter** — see "Replay autoscaling" below |
| `kafka.bootstrapServers` | `kafka:9092` | |
| `objectStore.existingSecret` | `""` | Secret with `access-key` / `secret-key`; omit for IRSA |
| `serviceAccount.annotations` | `{}` | e.g. `eks.amazonaws.com/role-arn` |
| `serviceMonitor.enabled` | `false` | Prometheus Operator scrape of `/actuator/prometheus`; **requires the ServiceMonitor CRD** installed |
| `monitoring.prometheusRule.enabled` | `false` | Prometheus Operator alerting rules (consumer lag, write-channel depth, sink state); **requires the PrometheusRule CRD** installed — the template also self-guards via `.Capabilities` so enabling this on a cluster without the CRD renders nothing instead of erroring |

All `joxette.*` properties can be overridden via `extraEnv` (Spring relaxed
binding, e.g. `JOXETTE_RECORDING_BATCH-SIZE: "20000"`).

### Replay autoscaling

Only the **replay** tier is HPA-eligible — it is stateless reads. Recorders are
bounded by the Kafka partition count (extra pods would idle) and compaction is
pinned to one node, so neither is autoscaled. Enabling `tiers.replay.hpa` with an
embedded catalog is rejected (single all-in-one pod, no replay tier). When the HPA
is enabled the replay Deployment omits a static `replicas` so the HPA owns it.
Requires `metrics-server` in the cluster (CPU metric).

A second, optional signal — average `joxette_replay_active` per pod — is gated
behind `tiers.replay.hpa.activeReplaysMetric.enabled` (default `false`, both here
and in `values.yaml`). Replay is I/O-bound (SSE/NDJSON fan-out to slow clients),
so many concurrent streams can saturate a pod at low CPU; this metric catches
what CPU alone misses. **Do not enable it without `prometheus-adapter`** exposing
`joxette_replay_active` via the custom metrics API (`custom.metrics.k8s.io`) —
this chart does not install or configure the adapter. HPA v2 semantics: if a
configured metric fails to resolve, scale-up still proceeds on the other
metrics, but scale-down is rejected outright. So turning this on without the
adapter installed silently ratchets the replay tier up to `maxReplicas` and it
never scales back down.

### Monitoring: ServiceMonitor and PrometheusRule

Both `serviceMonitor.enabled` and `monitoring.prometheusRule.enabled` default to
`false` because they render CRDs (`ServiceMonitor` / `PrometheusRule`) that the
Prometheus Operator provides — on a plain cluster without those CRDs installed,
`helm install` fails outright ("resource mapping not found... ensure CRDs are
installed first"). `values-cluster.yaml` turns both on explicitly as a
production-cluster assumption. `templates/prometheusrule.yaml` additionally
guards on `.Capabilities.APIVersions.Has "monitoring.coreos.com/v1"`, so even an
explicit `enabled: true` degrades to a no-op instead of erroring on a cluster
where the CRD isn't present yet.

### PodDisruptionBudget and node drains

The embedded topology's PDB pins `maxUnavailable: 0` (see the table above) —
intentional, since a second writer against the same DuckDB file corrupts it. One
side effect: `kubectl drain` on the node hosting the embedded pod will block
waiting for eviction to succeed, since the PDB refuses it. If you need to drain
that node anyway, either delete the PDB first (`kubectl delete pdb <release>`) or
run `kubectl drain --disable-eviction` (bypasses PDBs — you accept the pod may be
force-deleted rather than gracefully evicted).

See also: [`docs/clustering-deployment.md`](../../../docs/clustering-deployment.md),
[`docs/operator-design.md`](../../../docs/operator-design.md),
[`docs/catalog-scaling.md`](../../../docs/catalog-scaling.md).
