# ADR-0001: Composable capabilities — remove static role system

**Status:** Accepted  
**Date:** 2026-06-01  
**Authors:** Srdan Srepfler

---

## Context

Joxette grew a `joxette.roles` system (`recorder`, `entity-router`, `compaction`,
`replay`, `all`) backed by a Spring `@ConditionalOnRole` annotation evaluated once
at context refresh. The intent was to allow role-split cluster topologies (dedicated
recorder nodes, dedicated replay nodes, etc.).

Several problems emerged:

1. **Static, not dynamic.** Roles are baked in at JVM startup. A node cannot gain
   or shed a capability without a restart. This is incompatible with a cluster where
   workloads should be able to shift dynamically.

2. **Wrong abstraction.** The role system treats capabilities as exclusive
   properties of a node. In practice, every Joxette capability requires the same
   prerequisite — an attached DuckLake catalog — and every node that has the catalog
   can serve every workload.

3. **HTTP forwarding complexity.** The role split forced a design where a request
   arriving at the wrong node had to be forwarded to a capable node. This introduced
   hop latency and kept the forwarding node busy for the duration of long-running
   SSE/NDJSON streams.

4. **Misaligned with Kafka's own model.** Recording distribution is already handled
   by the Kafka consumer group: add a node and it gets partitions, remove it and
   partitions rebalance. Joxette's role system was duplicating coordination that
   Kafka already provides for free.

---

## Decision

**Remove the static role system entirely.** Every Joxette node runs every
capability. The catalog is the only prerequisite — not a role.

### Catalog is infrastructure, not a role

The DuckLake catalog is required by every workload:

| Workload | Why catalog is required |
|---|---|
| Recording | Inlined rows and snapshots written to catalog |
| Replay | Table metadata and inlined rows read from catalog |
| Compaction | `ducklake_merge_adjacent_files` is a catalog operation |
| Management API | Config tables (`topic_configs`, `entity_type_configs`, etc.) live in the catalog DuckDB |

A node that cannot attach the catalog is not a functional Joxette node.
`DuckLakeManager.initialize()` fails the Spring context on `ATTACH` failure;
`CatalogHealthIndicator` surfaces this at `/actuator/health/readiness`, gating
LB traffic before the node is considered healthy.

### Distribution falls out of existing primitives

| Concern | Mechanism | Joxette coordination needed |
|---|---|---|
| Recording partition distribution | Kafka consumer group rebalance | None |
| Replay load distribution | LB round-robin / least-connections | None |
| Compaction mutual exclusion | `compaction_locks` catalog table | None |
| Pause/resume across cluster | Config table write → all nodes react | None |

### No HTTP forwarding

Because every node can serve every request, there is nothing to forward.
A replay SSE stream served by any node reads from the same catalog and produces
the same result. The LB is the only routing layer needed.

### Opt-out flags for resource isolation

Two boolean flags replace the role system for operators who want to dedicate
nodes to specific workloads:

```yaml
joxette:
  recording:
    enabled: true    # set false to skip Kafka consumers on this node
  compaction:
    enabled: true    # set false to skip compaction scheduler on this node
```

`joxette.recording.enabled=false`: Kafka consumers do not start. No partitions
are assigned. The write channel is not created. The node still attaches the
catalog, serves replay, and runs management endpoints.

`joxette.compaction.enabled=false`: The cron compaction and retention schedulers
do not start. The `/compaction/trigger` endpoint still accepts manual triggers.
The `compaction_locks` table prevents concurrent runs regardless of how many
nodes have compaction enabled.

The replay API is always enabled — there is no reason to disable it on a node
that has the catalog.

### Load balancer topology

All nodes sit behind a single LB target. Health is gated on
`/actuator/health/readiness` (catalog attachment). No session affinity, no
capability routing, no sticky sessions required.

Nodes with `recording.enabled=false` may be placed in a separate LB target
group if the operator wants to isolate analytical traffic from write traffic,
but this is a deployment preference — not a code concern.

```
                    ┌─────────────────────────────────────┐
 Clients ──→ LB ──→ │  node A  (recording + replay + cmp) │
                    │  node B  (recording + replay + cmp) │
                    │  node C  (replay only, rec=false)   │
                    └─────────────────────────────────────┘
                                      │
                              shared catalog
                         (Quack / PostgreSQL for N>1;
                          embedded for single-node)
```

---

## Config change propagation — Option A + B

Topic and entity config mutations must propagate to all recording nodes without
HTTP forwarding. Two complementary mechanisms are used:

### Option A — Durable catalog as source of truth (reconciliation loop)

Every recording-enabled node runs a `RecordingConfigWatcher` that periodically
reads `topic_configs` from the shared catalog and reconciles against what is
currently running locally:

```
RecordingConfigWatcher (every 30 s + on startup)
  reads topic_configs from catalog
  sends ReconcileTopics(desiredState) to RecordingCoordinatorActor
    → start topics in desired but not running
    → stop topics running but removed/paused in desired
    → restart topics whose config changed (brokerId, startFrom)
```

This is the **authoritative** mechanism — it guarantees convergence even if a
pub/sub notification is missed (e.g. a node was down when a config change
occurred). Reaction time is bounded by the poll interval (default 30 s).

### Option B — Pekko typed pub/sub for immediate notification

`TopicController` and `EntityController` publish a typed event to a cluster-wide
`Topic` actor after every catalog write. All recording nodes subscribe and
trigger an immediate reconciliation, bypassing the 30 s wait:

```
POST /topics  ──→  write topic_configs  ──→  publish TopicConfigChanged
                                                 │
                          ┌──────────────────────┘  (Pekko Topic / DistributedPubSub)
                          │
              RecordingConfigWatcher (all recording nodes)
                  → reads catalog snapshot
                  → sends ReconcileTopics to local coordinator
```

Entity config changes (`POST /entities/**`) publish `EntityConfigChanged`, which
all nodes receive and use to call `messageRouter.reload()` locally — ensuring
every node's in-memory routing tables are up to date within milliseconds.

**Single-node mode:** the `Topic` actor is local; pub/sub is intra-process.
**Multi-node mode:** `Topic` uses `DistributedPubSub` under the hood once real
cluster bootstrap (Phase 2) is in place.

### Consequence for `TopicController`

`TopicController` no longer calls `coordinator.startTopic()` / `stopTopic()`
directly. It writes to the catalog and publishes an event. The local
`RecordingConfigWatcher` receives the event (same node, same JVM) and
reconciles — so perceived latency is identical to the old imperative call, but
correctness now comes from the catalog, not from which HTTP node was hit.

---

## Implementation plan

### Phase 1 — Remove static role machinery + catalog-driven recording (this session)

1. Add `joxette.recording.enabled` and `joxette.compaction.enabled` booleans to
   `JoxetteProperties.Recording` and `JoxetteProperties.Compaction`
2. Delete `ConditionalOnRole.java`, `OnRoleCondition.java`, `InstanceRoles.java`
3. Remove all `@ConditionalOnRole` annotations:
   - `CassetteController` — removed entirely (replay is always available)
   - `CompactionController` — removed (endpoint available everywhere; lock table
     prevents concurrent runs)
   - `CompactionScheduler` / `RetentionScheduler` — replaced with
     `@ConditionalOnProperty(name = "joxette.compaction.enabled", matchIfMissing = true)`
4. Create `TopicConfigChanged` and `EntityConfigChanged` event records
5. Add `ReconcileTopics` command to `RecordingCoordinatorActor`; implement
   declarative reconciliation (start/stop/restart based on desired vs actual state)
6. Create `RecordingConfigWatcher`: subscribes to `TopicConfigChanged` pub/sub +
   30 s reconciliation tick; replaces `RecordingStartupRunner`
7. Create `EntityConfigWatcher`: subscribes to `EntityConfigChanged` pub/sub;
   calls `messageRouter.reload()` on all nodes
8. Update `TopicController`: write to catalog + publish event; remove imperative
   `coordinator.startTopic()` / `stopTopic()` calls
9. Update `EntityController`: write to catalog + publish event; remove direct
   `messageRouter.reload()` calls
10. Remove `instanceRoles.isEntityRouter()` guard in `MessageRouter.route()`
    (entity routing always active when entity configs exist)
11. Replace `joxette.roles` in `application.yml` with `recording.enabled` /
    `compaction.enabled` booleans
12. Update `joxette_instances` table: replace `roles VARCHAR[]` with
    `recording_enabled BOOLEAN`, `compaction_enabled BOOLEAN`
13. Update `InstanceRegistry`, `InstanceController`, `HealthController`,
    `ClusterStateView`, and flow map UI to use the new fields

### Phase 2 — Multi-node cluster bootstrap (future)

- Pekko Management cluster bootstrap (kubernetes-api or DNS discovery)
- `http_port` column in `joxette_instances` for observability tooling
- Headless Kubernetes Service alongside LB Service

---

## Consequences

**Positive:**
- Simpler configuration — two booleans replace a multi-value role list
- No HTTP forwarding code, no transient-node-busy problem
- No Pekko Receptionist / capability routing layer needed
- Every node is identical in capability; LB topology is trivial
- Recording scale-out is purely a Kafka consumer-group concern
- Compaction safety is purely a catalog-lock concern

**Negative / trade-offs:**
- A `recording.enabled=true` node that has no Kafka broker reachable will fail
  consumer startup. Previously a `replay`-only node would not attempt a Kafka
  connection at all. Mitigation: `recording.enabled=false` on nodes with no
  Kafka connectivity.
- `compaction.enabled=true` on all nodes means the cron fires on every node;
  the lock table prevents duplicate work but there is slightly more lock contention
  at the scheduled time. Mitigation: `compaction.enabled=false` on all but one
  node, or accept the lock-and-skip behaviour as harmless.

---

## Rejected alternatives

**Keep role system, add dynamic role mutation via Pekko Receptionist:**
Adds significant complexity (Receptionist subscriptions, capability routing actors,
HTTP proxy or redirect for SSE). Rejected because the same outcome (any node serves
any request) is achievable with zero routing infrastructure once the catalog is
shared.

**Role-split with HTTP forwarding:**
Rejected because it keeps a transient node busy for the duration of long SSE streams,
adds a latency hop, and complicates the LB topology.

**ClusterSingleton compaction across a real multi-node cluster:**
Compaction exclusion is already solved at the catalog layer via `compaction_locks`.
The `ClusterSingleton` is redundant for cross-node safety and will be removed as
part of Phase 1 simplification.
