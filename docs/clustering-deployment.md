# Concurrency, Roles & Clustered Deployment

This document explains how a Joxette process uses concurrency internally, how
work is split across instances using **roles**, what actually coordinates
multiple instances, and how to deploy single-node and multi-node topologies —
on Kubernetes or plain hosts.

It deliberately does **not** repeat the catalog backend migration runbook — see
[`catalog-scaling.md`](catalog-scaling.md), which is a prerequisite for any
multi-instance deployment.

---

## TL;DR

- **One process = one virtual-thread runtime + one serialized DuckDB writer.**
  Inside a process there is no shared-state contention to worry about: all DuckLake
  writes funnel through a single bounded channel drained by one virtual thread.
- **The embedded DuckDB catalog is single-writer per file.** Two processes
  pointed at the same embedded catalog file **will corrupt it**. Horizontal
  scale-out therefore requires a shared catalog backend — **Quack (Stage 2)** or
  **PostgreSQL (Stage 3)** — from [`catalog-scaling.md`](catalog-scaling.md).
- **Roles, not replicas, are the scaling lever.** `joxette.roles` decides which
  subsystems start on a node: `recorder`, `entity-router`, `compaction`,
  `replay`. Scale recorders and replay nodes horizontally; keep compaction to a
  single active node.
- **Cross-process coordination runs through the shared catalog, not Pekko.**
  Each process currently forms its **own single-node Pekko cluster** (see
  [Pekko: current single-node model](#pekko-current-single-node-model)). What
  genuinely coordinates instances today is: the **Kafka consumer group**
  (partition balancing across recorders), the **`compaction_locks`** table
  (per-target mutual exclusion), and the **`joxette_instances`** heartbeat
  registry (observability).

---

## 1. Concurrency model (within one process)

Joxette runs on **Java 25 virtual threads** under Spring Boot 4. Every HTTP
request, Kafka poll loop, scheduled job, and Jox pipeline fiber is a virtual
thread, so blocking I/O (Kafka poll, DuckDB JDBC, S3 reads) is cheap and needs
no reactive/callback style.

### The single-writer write channel

DuckDB enforces single-writer semantics. Rather than locking at the JDBC layer,
**all writes funnel through one bounded [Jox](https://jox.softwaremill.com/)
channel** (`DuckLakeWriteChannel`) drained by **one dedicated virtual thread**
(`joxette-write-drain`):

```
Kafka VT 1 ──┐
Kafka VT 2 ──┼──▶ Channel<WriteBatch> (bounded) ──▶ drain VT ──▶ DuckDB / DuckLake
Kafka VT N ──┘                                                   (general + entity writers)
```

- **Reads bypass the channel.** Each replay request opens its own `Statement`
  on the shared connection; concurrent reads are safe in DuckDB.
- **Backpressure is implicit.** When writes are slow the channel fills, Jox
  `send` blocks the producing virtual thread, the Kafka poll loop stalls, and
  consumer lag rises. Lag is the observable pressure valve — there is no
  separate flow-control protocol.

Key tunables (`joxette.threading.*`):

| Property | Default | Effect |
|---|---|---|
| `write-channel-capacity` | `128` | Bounded write-channel slots; raise if write bursts cause avoidable lag |
| `default-source-parallelism` | `1` | Virtual threads per Kafka source; raise only for high-partition CPU-bound routing |
| `topic-parallelism.<topic>` | inherits default | Per-topic override |
| `compaction-thread-type` | `virtual` | Set `platform` only if a compaction library pins virtual threads |

Recording batches flush on size or time (`joxette.recording.batch-size`,
default `10000`; `batch-timeout-ms`, default `1000`).

### Pekko's role in concurrency

Pekko actors are used for **supervision and lifecycle**, not throughput. The
actor dispatcher is intentionally tiny (`fixed-pool-size = 4`) because actors
only route messages; all heavy work is offloaded to virtual threads via
`context.pipeToSelf(CompletableFuture.supplyAsync(…, pekkoVtExecutor), …)`.

Two coordinator actors run under the system guardian:

- **`RecordingCoordinatorActor`** — manages one `TopicLifecycleActor` child per
  topic. Each child runs `TopicRecorder` on a VT with Pekko exponential-backoff
  restart on failure.
- **`ReplayCoordinatorActor`** — manages one `ReplayActor` child per active
  replay-to-topic SSE request. Each child runs `FlowReplayEngine` (Jox supervised
  scope) on a VT. When the SSE client disconnects, `SseEmitter.onCompletion` tells
  the coordinator `CancelReplay(id)`, which interrupts the VT — the Jox scope
  propagates cancellation to the DuckDB read immediately. Completed/cancelled
  entries linger in the coordinator for 30 s (visible in the live flow map) then
  are evicted.

---

## 2. The single-writer catalog constraint (read this first)

This is the constraint that shapes every clustered deployment.

DuckLake stores table data as Parquet on object storage, but it needs a
**catalog database** for metadata, inlined rows, and snapshots. The catalog
backend is chosen automatically from the URI scheme of `joxette.catalog.path`:

| `joxette.catalog.path` | Backend | Processes that can share it |
|---|---|---|
| `./data/joxette.ducklake` (file / no scheme) | `EMBEDDED_DUCKDB` | **Exactly one** |
| `quack://host:port/…` | `QUACK` (DuckDB 1.5.3+, beta) | Many |
| `postgresql://host/db` | `POSTGRESQL` | Many (HA-capable) |

> **A second process pointed at the same embedded `.ducklake` file will corrupt
> it.** Stage 1 (embedded) is single-instance only. Any topology with more than
> one Joxette process **must** first move the catalog to Quack or PostgreSQL per
> [`catalog-scaling.md`](catalog-scaling.md). Object storage (the Parquet data
> path) is always shared and safe; it is only the catalog that is single-writer
> at Stage 1.

Readiness reflects catalog health: `DuckLakeManager.initialize()` fails the
Spring context if `ATTACH` fails, and `CatalogHealthIndicator` surfaces it at
`GET /actuator/health/readiness`.

---

## 3. Instance roles

`joxette.roles` (a list) selects which subsystems start. Gating is implemented
by `@ConditionalOnRole("<role>")` (backed by `OnRoleCondition`, a Spring
`Condition` that binds `joxette.roles` from any property source). Default is
`[all]`.

| Role | What it starts | Gated component(s) |
|---|---|---|
| `recorder` | Kafka consumers writing to general cassettes | `RecordingStartupRunner` |
| `entity-router` | Entity extraction/routing within the recording pipeline (**requires `recorder`**) | recording pipeline |
| `compaction` | Cron compaction + retention jobs and `POST /compaction/trigger` | `CompactionScheduler`, `RetentionScheduler` |
| `replay` | The `GET/POST /cassettes/**` replay API | replay controllers |
| `all` | Synthetic alias → all four roles (default) | — |

The active set is logged at startup (`Starting Joxette with roles: […]`) and
reported under `GET /health` (`roles`, `instanceId`). `replay` can additionally
be turned off independently with `joxette.replay.enabled=false`.

```yaml
# All-in-one (default)
joxette: { roles: [all] }

# Dedicated write node — Kafka consumers + entity routing, no API, no compaction
joxette: { roles: [recorder, entity-router] }

# Single compaction/maintenance node
joxette: { roles: [compaction] }

# Stateless read-only replay node
joxette: { roles: [replay] }
```

---

## 4. What actually coordinates multiple instances

Three independent mechanisms, none of which require a multi-node Pekko cluster:

### 4.1 Kafka consumer group (recorder fan-out)

All `recorder` nodes join the same consumer group
(`joxette.kafka.consumer-group`). Kafka assigns topic partitions across them, so
adding recorder pods spreads partitions automatically. Joxette uses the
**KIP-848 cooperative protocol** (`group-protocol: consumer`) so a rebalance only
pauses the *revoked* partitions; non-revoked partitions keep flowing. Recording
is at-least-once; replay deduplicates on read by `(topic, partition, offset)`, so
duplicate delivery during a rebalance is harmless.

### 4.2 `compaction_locks` table (per-target mutual exclusion)

`ducklake_merge_adjacent_files` is **not safe to run in parallel on the same
table**. `CompactionLockManager` guards each target (entity type or topic) with a
row in the `compaction_locks` table (plain DuckDB, not DuckLake):

- Acquire via `INSERT … ON CONFLICT DO NOTHING` then confirm ownership.
- Heartbeat every `HEARTBEAT_INTERVAL_MINUTES` (10) from a dedicated VT so a long
  merge never trips its own TTL (`joxette.compaction.lock-ttl-minutes`, default 120).
- Targets already locked by another instance are **silently skipped** and counted
  as skipped in the run result.

Because the lock lives in the shared catalog, it works across processes **when the
catalog is shared (Stage 2/3)**. This is what makes it safe even if more than one
`compaction` node is accidentally running.

### 4.3 `joxette_instances` heartbeat registry (observability)

Every instance upserts a row (`instance_id = hostname:pid`, roles,
`catalog_backend`, `started_at`, `last_heartbeat`, `kafka_assignments`) and
refreshes `last_heartbeat` every **30 s**. Rows stale for >90 s are reported as
`stale`; rows older than 2 min are reaped at the next startup. Surfaced at:

| Endpoint | Source | Resolution |
|---|---|---|
| `GET /instances` | `joxette_instances` table | 30 s heartbeat |
| `GET /instances/topology` | live Pekko membership (this process only — see below) | ~10 s phi-accrual |
| `GET /instances/cluster-state` | unified self + registry + topology + active replays | — |
| `GET /instances/live-metrics` | SSE, cluster-state every 2 s | streaming |

### Pekko: current single-node model

> **Important.** Each Joxette process starts its ActorSystem with
> `seed-nodes = []` and then issues a **programmatic self-join**
> (`Cluster.manager().tell(Join.create(self))` in `PekkoConfig`). This means
> **every process forms its own one-member cluster** — separate pods do **not**
> currently discover each other or form a shared Pekko cluster.

Consequences for deployment:

- The **`ClusterSingleton` compaction actor is per-process**, not cluster-wide. It
  prevents overlapping compaction *within* a process; it does **not** elect a
  single compaction node across pods.
- Cross-pod compaction safety comes from the **`compaction_locks` table**
  (§4.2), not from Pekko. The practical rule is therefore: **run exactly one
  `compaction`-role node**, and rely on the lock table as a backstop.
- The Split-Brain Resolver (`keep-majority`) and phi-accrual failure detector in
  `pekko.conf` are configured but are effectively no-ops in a one-member cluster.

Forming a real multi-node Pekko cluster (shared seed nodes / discovery) is a
future enhancement; see [Current limitations](#current-limitations). Until then,
plan topologies around the catalog and Kafka consumer group, treating Pekko as
intra-process supervision.

---

## 5. Deployment topologies

### 5.1 Single instance (default, Stage 1)

One process, `roles: [all]`, embedded catalog. Simplest and fully featured;
the only option while on the embedded catalog. Suitable for dev, small volumes,
and single-VM production.

```
            ┌──────────────────────────┐
 Kafka ───▶ │ Joxette (all roles)      │ ───▶ object storage (Parquet)
            │  embedded .ducklake file │
 REST  ───▶ │  + replay API            │
            └──────────────────────────┘
```

### 5.2 Role-split scale-out (Stage 2/3 catalog required)

Once the catalog is on Quack or PostgreSQL, split by role and scale the
stateless tiers:

```
                         shared catalog (Quack / PostgreSQL)
                                    ▲     ▲     ▲
                  ┌─────────────────┘     │     └─────────────────┐
        ┌─────────┴─────────┐   ┌─────────┴────────┐   ┌──────────┴─────────┐
Kafka ─▶│ recorder ×N       │   │ compaction ×1    │   │ replay ×M          │◀─ REST
        │ entity-router     │   │ (cron + locks)   │   │ (stateless reads)  │
        │ (consumer group)  │   └──────────────────┘   └────────────────────┘
        └───────────────────┘
                                    ▼  all tiers share ▼
                              object storage (Parquet data)
```

| Tier | Roles | Scaling | Notes |
|---|---|---|---|
| Recorders | `recorder`, `entity-router` | Horizontal — up to the partition count | Kafka consumer group balances partitions; KIP-848 rebalance |
| Replay API | `replay` | Horizontal — stateless | Reads only; put behind a load balancer |
| Compaction | `compaction` | **Exactly one** | Cron-driven; `compaction_locks` is the cross-pod backstop |

A pure write-only recorder node should also set `joxette.replay.enabled=false`.

---

## 6. Kubernetes deployment

There are no shipped manifests yet (see [Current limitations](#current-limitations));
the following are the patterns to apply. A **Kubernetes operator** that encodes
and enforces these patterns (including the catalog single-writer guardrail and
declarative topic/entity management) is designed in
[`operator-design.md`](operator-design.md).

### Workload kinds

- **Stage 1 / embedded catalog → single-replica `StatefulSet`** (or a
  `Deployment` with `replicas: 1` and `strategy: Recreate`). A `PersistentVolume`
  holds the `.ducklake` catalog file. **Never set `replicas > 1`** against an
  embedded catalog — it corrupts the file. Object storage holds the Parquet data.
- **Recorder tier (Stage 2/3) → `Deployment`**, `replicas` ≤ Kafka partition
  count. Stateless w.r.t. local disk (catalog is remote).
- **Replay tier → `Deployment`**, horizontally scalable, behind a `Service` /
  Ingress. Stateless.
- **Compaction tier → `Deployment` with `replicas: 1`** (the `compaction_locks`
  table tolerates a brief overlap during rollout, but steady-state should be one).

### Health probes

Map Spring Boot Actuator endpoints. The `liveness`/`readiness` probe groups are
auto-enabled when Spring Boot detects Kubernetes; elsewhere set
`management.endpoint.health.probes.enabled=true` (the base `/actuator/health`
endpoint is always available and already includes the `catalog` component).

```yaml
readinessProbe:
  httpGet: { path: /actuator/health/readiness, port: 8080 }
  # gates traffic on catalog ATTACH success
livenessProbe:
  httpGet: { path: /actuator/health/liveness, port: 8080 }
startupProbe:
  httpGet: { path: /actuator/health, port: 8080 }
  failureThreshold: 30        # allow time for catalog ATTACH + cluster self-join
```

### Graceful shutdown

Joxette drives an ordered 4-step shutdown on `SIGTERM` via `PekkoConfig.shutdown()`:

1. **Stop recorders** — `RecordingCoordinator.stopAll()` asks every
   `TopicLifecycleActor` to stop; waits for each recorder VT to exit and offsets
   to commit before proceeding.
2. **Cancel active replays** — `ReplayCoordinatorActor.CancelAll` interrupts every
   in-flight replay VT so the DuckDB connection is released before it closes.
3. **Drain write channel** — lets any in-flight DuckDB batches reach DuckLake
   before the catalog connection closes.
4. **Pekko coordinated shutdown** — cluster leave → exiting → actor-system
   terminate → Artery transport shutdown (≤20 s timeout).

Give pods enough grace to complete all four steps:

```yaml
terminationGracePeriodSeconds: 45
```

### Configuration via env vars

Spring relaxed binding maps env vars to properties:

| Property | Env var | Example |
|---|---|---|
| `joxette.roles` | `JOXETTE_ROLES` | `recorder,entity-router` |
| `joxette.catalog.path` | `JOXETTE_CATALOG_PATH` | `postgresql://pg/joxette` |
| `joxette.catalog.object-storage-path` | `JOXETTE_CATALOG_OBJECT-STORAGE-PATH` | `s3://bucket/joxette` |
| `joxette.kafka.bootstrap-servers` | `JOXETTE_KAFKA_BOOTSTRAP-SERVERS` | `kafka:9092` |
| `joxette.replay.enabled` | `JOXETTE_REPLAY_ENABLED` | `false` |
| `joxette.pekko.remote-port` | `JOXETTE_PEKKO_REMOTE-PORT` | `2551` |

S3/object-store credentials follow the DuckDB httpfs / SECRET model in
[`object-storage.md`](object-storage.md) (IRSA on EKS is supported).

### A note on Pekko remoting in containers

Because pods don't form a shared cluster today, **no headless Service or stable
remoting addressing is required** for correctness — `remote-port: 0`
(OS-assigned) is fine. If/when multi-node clustering lands, recorders/compaction
would need a headless `Service` and a fixed `joxette.pekko.remote-port` (e.g.
`2551`) so seed-node addresses are stable; set that now only if you are
pre-provisioning for it.

---

## 7. Deploying without Kubernetes

The same rules apply on VMs / bare metal / `docker compose`:

- **Single host:** run one `java -jar` (or `docker run`) with `roles: [all]` and
  an embedded catalog on local disk. This is what `docker-compose.yml` + `mvn
  spring-boot:run` give you for local dev.
- **Multiple hosts:** stand up PostgreSQL (or a Quack server) for the catalog,
  point every node's `joxette.catalog.path` at it, then start role-specialised
  processes (e.g. a `systemd` unit per role). One process per host keeps a clean
  `hostname:pid` identity in the registry.
- Put the `replay` nodes behind any TCP/HTTP load balancer; they are stateless.
- Keep a single `compaction` process.

---

## 8. Failure & recovery

| Event | Behaviour |
|---|---|
| Recorder pod dies | Kafka rebalances its partitions to surviving recorders (KIP-848, cooperative). Uncommitted offsets reprocess; read-side dedup absorbs duplicates. |
| Replay pod dies | Stateless — the load balancer drops it; in-flight SSE/NDJSON streams to that pod break and clients reconnect. |
| Compaction pod dies mid-run | Its `compaction_locks` rows expire after the TTL (default 120 min) and become acquirable again; partial merges are safe (DuckLake is transactional). The next scheduled run resumes. |
| Catalog (Stage 2/3) unavailable | `CatalogHealthIndicator` reports `DOWN`; readiness fails and traffic is withheld until the catalog returns. |
| Whole catalog lost | Parquet data on object storage survives. Rebuild config + `known_entities` via snapshot restore / `POST /cassettes/entities/rebuild-known-entities`. See [`catalog-scaling.md`](catalog-scaling.md). |

---

## 9. Configuration reference (clustering-relevant)

```yaml
joxette:
  roles: [all]                       # recorder | entity-router | compaction | replay | all

  pekko:
    remote-port: 0                   # 0 = OS-assigned; set a fixed port only when pre-provisioning multi-node

  catalog:
    path: "./data/joxette.ducklake"  # file → EMBEDDED (1 process); quack:// or postgresql:// → shared
    object-storage-path: "s3://joxette-data/"

  kafka:
    bootstrap-servers: "localhost:9092"
    consumer-group: "joxette-recorder"       # shared across recorder nodes
    group-protocol: "consumer"               # KIP-848 cooperative rebalance

  threading:
    write-channel-capacity: 128
    default-source-parallelism: 1
    topic-parallelism: {}                    # { "high-volume-topic": 2 }
    compaction-thread-type: virtual

  recording:
    batch-size: 10000
    batch-timeout-ms: 1000

  compaction:
    schedule: "0 0 3 * * *"                  # daily 03:00 (Spring 6-field cron)
    lock-ttl-minutes: 120

  retention:
    schedule: "0 0 1 * * *"                  # daily 01:00 (before compaction)

  replay:
    enabled: true                            # false on pure write-only nodes
```

See also: [`catalog-scaling.md`](catalog-scaling.md) (catalog backends),
[`object-storage.md`](object-storage.md) (S3/GCS/Azure + IRSA),
[`error-handling.md`](error-handling.md) (RFC 7807 responses).

---

## Current limitations

These are known gaps as of this writing — documented so deployments don't assume
capabilities that aren't wired yet:

1. **No shared Pekko cluster.** Processes self-join one-member clusters
   (`seed-nodes = []`). `ClusterSingleton` is per-process; cross-pod compaction
   safety relies on the `compaction_locks` table, so **run exactly one
   `compaction` node**. Forming a real shared cluster (Pekko Management Cluster
   Bootstrap + `kubernetes-api` discovery + a `coordination.k8s.io` Lease behind
   the SBR/singleton) is the "Track B" prerequisite in
   [`operator-design.md`](operator-design.md) §8.2.
2. **No shipped Kubernetes/Helm manifests.** §6 describes the intended patterns
   and [`operator-design.md`](operator-design.md) designs an operator around them;
   neither manifests nor the operator are built yet.
3. **Embedded catalog is single-instance.** Multi-instance requires the Stage 2
   (Quack, beta) or Stage 3 (PostgreSQL) catalog backend first.
