# Cluster Flow Map: Component-Driven Instance Panel Layout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Cluster page's Flow Map instance panel — currently laid out via hand-computed pixel math (`ROW_H`, `JOB_W`, `COL_GAP`, `CONT_W`, `CONT_H`) — with a version whose internals are real CSS flex layout that sizes and spaces itself from actual component content, while leaving the existing implementation fully intact and unlinked.

**Architecture:** Add a new component file (`ClusterFlowMapObjects.tsx`) that reuses the existing particle system, SSE hook, rate computation, and freestanding-node components (Kafka topic, remote DuckLake, object storage, replay target) from `ClusterFlowMap.tsx` via import, but replaces the instance-panel node with one whose recorder/DuckLake/replay rows are plain React children in a CSS flex layout with named per-row `<Handle>`s, instead of separately positioned React Flow nodes. Switch the route to render the new component.

**Tech Stack:** React, TypeScript (strict), `@xyflow/react` (React Flow) v12, Vite.

## Global Constraints

- No new npm dependencies.
- `ui/src/components/ClusterFlowMap.tsx` must remain fully intact and working if directly imported — only additive `export` keyword changes, no logic changes.
- No new unit tests (this is a visual/layout rewrite of a live view with no existing test coverage) — verification is TypeScript typecheck + browser check per CLAUDE.md's UI-change requirement.
- Baseline: `npx tsc --noEmit` currently reports exactly 7 pre-existing errors, all in `src/components/SunburstChart.tsx` (unrelated, do not fix). After every step, confirm no *new* errors appear: `npx tsc --noEmit 2>&1 | grep -v SunburstChart` must produce no output.
- Dev server is already running in the main checkout at `http://localhost:5173` (started outside this session) — use it for browser verification; do not start a second one on the same port.

---

### Task 1: Export shared infrastructure from ClusterFlowMap.tsx

**Files:**
- Modify: `ui/src/components/ClusterFlowMap.tsx`

**Interfaces:**
- Produces (all newly `export`ed, no signature changes): `ParticleStore` (type), `makeParticleStore`, `ParticleContext`, `useLiveMetrics`, `useParticleSpawner`, `useAnimationLoop`, `ParticleEdgeData` (type), `ParticleEdge`, `KafkaTopicNodeData` (type), `DuckLakeNodeData` (type), `ObjectStorageNodeData` (type), `ReplayTargetNodeData` (type), `KafkaTopicNode`, `DuckLakeNode`, `ReplayTargetNode`, `ObjectStorageNode`, `Rates` (type), `useRates`, `Stat`, `fmt`, `nodeBase`, `nodeHeader`, `nodeTitle`, `nodeMeta`, `handleStyle`, `pill`, `jobTypeLabel`.

- [ ] **Step 1: Add `export` to each symbol Task 2 will need to import**

Apply each of these exact single-line replacements (each old line appears exactly once in the file):

| Old | New |
|---|---|
| `interface ParticleStore {` | `export interface ParticleStore {` |
| `function makeParticleStore(): ParticleStore {` | `export function makeParticleStore(): ParticleStore {` |
| `const ParticleContext = createContext<ParticleStore \| null>(null)` | `export const ParticleContext = createContext<ParticleStore \| null>(null)` |
| `function useLiveMetrics(): { data: ClusterStateView \| null; connected: boolean } {` | `export function useLiveMetrics(): { data: ClusterStateView \| null; connected: boolean } {` |
| `function useParticleSpawner(` | `export function useParticleSpawner(` |
| `function useAnimationLoop(store: ParticleStore) {` | `export function useAnimationLoop(store: ParticleStore) {` |
| `interface ParticleEdgeData extends Record<string, unknown> {` | `export interface ParticleEdgeData extends Record<string, unknown> {` |
| `function ParticleEdge({ id, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, data, markerEnd }: EdgeProps<Edge<ParticleEdgeData>>) {` | `export function ParticleEdge({ id, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, data, markerEnd }: EdgeProps<Edge<ParticleEdgeData>>) {` |
| `interface KafkaTopicNodeData extends Record<string, unknown> { label: string; partitions: number }` | `export interface KafkaTopicNodeData extends Record<string, unknown> { label: string; partitions: number }` |
| `interface DuckLakeNodeData extends Record<string, unknown> { label: string; totalWritten: number }` | `export interface DuckLakeNodeData extends Record<string, unknown> { label: string; totalWritten: number }` |
| `interface ObjectStorageNodeData extends Record<string, unknown> { label: string }` | `export interface ObjectStorageNodeData extends Record<string, unknown> { label: string }` |
| `interface ReplayTargetNodeData extends Record<string, unknown> { label: string }` | `export interface ReplayTargetNodeData extends Record<string, unknown> { label: string }` |
| `function KafkaTopicNode({ data }: NodeProps<Node<KafkaTopicNodeData>>) {` | `export function KafkaTopicNode({ data }: NodeProps<Node<KafkaTopicNodeData>>) {` |
| `function DuckLakeNode({ data }: NodeProps<Node<DuckLakeNodeData>>) {` | `export function DuckLakeNode({ data }: NodeProps<Node<DuckLakeNodeData>>) {` |
| `function ReplayTargetNode({ data }: NodeProps<Node<ReplayTargetNodeData>>) {` | `export function ReplayTargetNode({ data }: NodeProps<Node<ReplayTargetNodeData>>) {` |
| `function ObjectStorageNode({ data }: NodeProps<Node<ObjectStorageNodeData>>) {` | `export function ObjectStorageNode({ data }: NodeProps<Node<ObjectStorageNodeData>>) {` |
| `interface Rates { consumedPerSec: number; writtenPerSec: number }` | `export interface Rates { consumedPerSec: number; writtenPerSec: number }` |
| `function useRates(recorders: Record<string, RecorderStatus>): Record<string, Rates> {` | `export function useRates(recorders: Record<string, RecorderStatus>): Record<string, Rates> {` |
| `function Stat({ label, value, color }: { label: string; value: string; color?: string }) {` | `export function Stat({ label, value, color }: { label: string; value: string; color?: string }) {` |
| `function fmt(n: number): string {` | `export function fmt(n: number): string {` |
| `const nodeBase: React.CSSProperties = {` | `export const nodeBase: React.CSSProperties = {` |
| `const nodeHeader: React.CSSProperties = { display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6, flexWrap: 'wrap' }` | `export const nodeHeader: React.CSSProperties = { display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6, flexWrap: 'wrap' }` |
| `const nodeTitle: React.CSSProperties = { fontSize: '0.8125rem', fontWeight: 600, color: 'var(--ink-primary)' }` | `export const nodeTitle: React.CSSProperties = { fontSize: '0.8125rem', fontWeight: 600, color: 'var(--ink-primary)' }` |
| `const nodeMeta: React.CSSProperties = { display: 'flex', gap: 12, flexWrap: 'wrap' }` | `export const nodeMeta: React.CSSProperties = { display: 'flex', gap: 12, flexWrap: 'wrap' }` |
| `const handleStyle: React.CSSProperties = { background: 'var(--rule-strong)', border: 'none', width: 8, height: 8 }` | `export const handleStyle: React.CSSProperties = { background: 'var(--rule-strong)', border: 'none', width: 8, height: 8 }` |
| `const pill: React.CSSProperties = { fontSize: '0.5625rem', fontWeight: 600, padding: '1px 6px', borderRadius: '999px', letterSpacing: '0.04em' }` | `export const pill: React.CSSProperties = { fontSize: '0.5625rem', fontWeight: 600, padding: '1px 6px', borderRadius: '999px', letterSpacing: '0.04em' }` |
| `const jobTypeLabel: React.CSSProperties = { fontSize: '0.5625rem', fontWeight: 700, letterSpacing: '0.1em', color: 'var(--ink-tertiary)', textTransform: 'uppercase' }` | `export const jobTypeLabel: React.CSSProperties = { fontSize: '0.5625rem', fontWeight: 700, letterSpacing: '0.1em', color: 'var(--ink-tertiary)', textTransform: 'uppercase' }` |

Do **not** export anything else — `useParticleStore`, `dotStyle`, `InstanceContainerNode`/`InstanceContainerNodeData`, `RecordJobNode`/`RecordJobNodeData`, `ReplayJobNode`/`ReplayJobNodeData`, `DuckDbEngineNode`/`DuckDbEngineNodeData`, `buildGraph`, `FlowInner`, the layout constants (`ROW_H`, `JOB_W`, `CONT_PAD_X`, `CONT_PAD_TOP`, `CONT_PAD_BOT`, `LAKE_W`, `COL_GAP`, `X_KAFKA`, `X_INSTANCE`), and the local `nodeTypes`/`edgeTypes` registries stay exactly as they are — Task 2 does not need them, and `ClusterFlowMap.tsx`'s own exported `ClusterFlowMap` component must keep working unchanged.

- [ ] **Step 2: Typecheck**

Run: `cd ui && npx tsc --noEmit 2>&1 | grep -v SunburstChart`
Expected: no output (0 new errors — adding `export` is purely additive and cannot introduce type errors).

- [ ] **Step 3: Commit**

```bash
git add ui/src/components/ClusterFlowMap.tsx
git commit -m "$(cat <<'EOF'
refactor(ui/cluster): export flow-map infrastructure for reuse

Purely additive export keywords — no behavior change — so the new
component-driven instance panel layout (next commit) can reuse the
particle system, SSE hook, rate computation, and freestanding node
components instead of duplicating them.
EOF
)"
```

---

### Task 2: Create the component-driven instance panel (`ClusterFlowMapObjects.tsx`)

**Files:**
- Create: `ui/src/components/ClusterFlowMapObjects.tsx`

**Interfaces:**
- Consumes (from Task 1): everything listed in Task 1's "Produces" list, imported from `./ClusterFlowMap`. Also consumes `instancesApi` is **not** needed directly (encapsulated inside the imported `useLiveMetrics`). Consumes `ClusterStateView`, `RecorderStatus`, `ActiveReplay` from `../api/client` (same as `ClusterFlowMap.tsx` does).
- Produces: `export function ClusterFlowMapObjects()` — a drop-in replacement for `ClusterFlowMap()` with the same zero-prop signature, for Task 3 to import.

- [ ] **Step 1: Write the new file**

```tsx
import '@xyflow/react/dist/style.css'
import {
  ReactFlow,
  Background,
  useNodesState,
  useEdgesState,
  useReactFlow,
  ReactFlowProvider,
  Position,
  Handle,
  type NodeProps,
  type Node,
  type Edge,
  BackgroundVariant,
} from '@xyflow/react'
import { useEffect, useRef } from 'react'
import type { ClusterStateView, RecorderStatus, ActiveReplay } from '../api/client'
import {
  type ParticleStore, makeParticleStore, ParticleContext,
  useLiveMetrics, useParticleSpawner, useAnimationLoop,
  type ParticleEdgeData, ParticleEdge,
  type KafkaTopicNodeData, KafkaTopicNode,
  type DuckLakeNodeData, DuckLakeNode,
  type ObjectStorageNodeData, ObjectStorageNode,
  type ReplayTargetNodeData, ReplayTargetNode,
  type Rates, useRates,
  Stat, fmt,
  nodeBase, nodeHeader, nodeTitle, nodeMeta, handleStyle, pill, jobTypeLabel,
} from './ClusterFlowMap'

// ---------------------------------------------------------------------------
// Instance panel — internals are real CSS flex layout, not computed pixel
// offsets. Row components below are plain JSX (not registered React Flow
// node types); each carries its own named <Handle>s, nested in normal DOM
// flow, so their connection points track the row's real rendered position.
// ---------------------------------------------------------------------------

interface InstanceContainerNodeData extends Record<string, unknown> {
  instanceId: string
  recordingEnabled: boolean
  compactionEnabled: boolean
  pekkoStatus: string | null
  reachable: boolean
  hasCatalog: boolean
  recorderEntries: [string, RecorderStatus][]
  rates: Record<string, Rates>
  totalWritten: number
  replays: ActiveReplay[]
}

const colLabelStyle: React.CSSProperties = {
  fontSize: '0.5625rem', fontWeight: 700, letterSpacing: '0.1em',
  textTransform: 'uppercase', color: 'var(--ink-tertiary)',
}

function RecorderRow({ topic, rec, rates }: { topic: string; rec: RecorderStatus; rates: Rates }) {
  const statusColor = rec.running ? (rec.consumerLag > 1000 ? '#A26612' : '#3E6A44') : '#8B2121'
  return (
    <div style={{ ...nodeBase, position: 'relative', minWidth: 220, borderColor: statusColor }}>
      <Handle type="target" position={Position.Left} id={`recorder-in-${topic}`} style={handleStyle} />
      <div style={nodeHeader}>
        <span style={jobTypeLabel}>RECORD</span>
        <span style={nodeTitle}>{topic}</span>
        <span style={{ ...pill, background: rec.running ? '#dcfce7' : '#fee2e2', color: statusColor, marginLeft: 'auto' }}>
          {rec.running ? 'live' : 'paused'}
        </span>
      </div>
      <div style={nodeMeta}>
        <Stat label="IN/S"  value={fmt(rates.consumedPerSec)} color={statusColor} />
        <Stat label="OUT/S" value={fmt(rates.writtenPerSec)} />
        <Stat label="LAG"   value={rec.consumerLag < 0 ? '—' : rec.consumerLag.toLocaleString()} color={rec.consumerLag > 1000 ? '#A26612' : undefined} />
        <Stat label="PARTS" value={rec.assignedPartitions.length > 0 ? Array.from(rec.assignedPartitions).join(',') : '—'} />
      </div>
      {rec.lastError && (
        <div style={{ marginTop: 4, fontSize: '0.6rem', color: '#8B2121', fontFamily: 'var(--font-mono)', wordBreak: 'break-all' }}>{rec.lastError}</div>
      )}
      <Handle type="source" position={Position.Right} id={`recorder-out-${topic}`} style={handleStyle} />
    </div>
  )
}

function DuckLakeRow({ totalWritten }: { totalWritten: number }) {
  return (
    <div style={{ ...nodeBase, position: 'relative', background: 'var(--surface-sunken)', minWidth: 180, boxSizing: 'border-box' }}>
      <Handle type="target" position={Position.Left}   id="lake-in" style={handleStyle} />
      <Handle type="source" position={Position.Right}  id="lake-replay-out" style={handleStyle} />
      <Handle type="source" position={Position.Bottom} id="lake-tier-out" style={handleStyle} />
      <div style={nodeHeader}>
        <img src="/DuckLake_icon-darkmode.svg" alt="DuckLake" style={{ width: 18, height: 18, flexShrink: 0 }} />
        <img src="/DuckDB_icon-darkmode.svg"   alt="DuckDB"   style={{ width: 14, height: 14, flexShrink: 0, opacity: 0.6 }} />
        <span style={nodeTitle}>DuckLake</span>
      </div>
      <div style={nodeMeta}>
        <Stat label="WRITTEN" value={totalWritten.toLocaleString()} />
      </div>
    </div>
  )
}

function ReplayRow({ replay }: { replay: ActiveReplay }) {
  const running = replay.status === 'running'
  const statusColor = running ? '#1E5A8A' : 'var(--ink-tertiary)'
  const statusBg    = running ? '#dbeafe'  : 'var(--surface-sunken)'
  return (
    <div style={{ ...nodeBase, position: 'relative', minWidth: 220, borderColor: statusColor, opacity: running ? 1 : 0.45 }}>
      <Handle type="target" position={Position.Left} id={`replay-in-${replay.id}`} style={handleStyle} />
      <div style={nodeHeader}>
        <span style={jobTypeLabel}>REPLAY</span>
        <span style={nodeTitle}>{replay.sourceTopic}</span>
        <span style={{ ...pill, background: statusBg, color: statusColor, marginLeft: 'auto' }}>{replay.status}</span>
      </div>
      <div style={nodeMeta}>
        <Stat label="SENT" value={replay.sentCount.toLocaleString()} color={running ? statusColor : undefined} />
        <Stat label="→" value={replay.targetTopic} />
      </div>
      <Handle type="source" position={Position.Right} id={`replay-out-${replay.id}`} style={handleStyle} />
    </div>
  )
}

function InstanceContainerNode({ data }: NodeProps<Node<InstanceContainerNodeData>>) {
  const statusColor = data.pekkoStatus === 'up' ? '#3E6A44' : data.pekkoStatus ? '#A26612' : 'var(--rule-strong)'
  const hasRecorders = data.recorderEntries.length > 0
  const hasReplays   = data.replays.length > 0
  const HEADER_H = 44

  return (
    <div style={{
      border: `1.5px solid ${statusColor}`,
      borderRadius: 'var(--radius-md)',
      background: 'var(--surface)',
      boxShadow: '0 2px 8px rgba(30,26,20,0.06)',
    }}>
      <div style={{ height: HEADER_H, padding: '8px 12px', borderBottom: '1px solid var(--rule)', display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
        <img src="/joxette logo.png" alt="Joxette" style={{ width: 20, height: 20, flexShrink: 0, filter: 'invert(1) brightness(0.8)' }} />
        <span style={{ ...nodeTitle, fontFamily: 'var(--font-mono)', fontSize: '0.6875rem' }}>{data.instanceId}</span>
        {data.recordingEnabled && (
          <span style={{ ...pill, background: '#dcfce7', color: '#3E6A44' }}>recording</span>
        )}
        {data.compactionEnabled && (
          <span style={{ ...pill, background: 'var(--surface-sunken)', color: 'var(--ink-secondary)' }}>compaction</span>
        )}
        <span style={{ marginLeft: 'auto', fontSize: '0.5625rem', fontWeight: 700, letterSpacing: '0.08em', color: statusColor, textTransform: 'uppercase' }}>
          {data.pekkoStatus ?? 'unknown'}
          {data.reachable ? '' : ' ✗'}
        </span>
      </div>

      <div style={{ display: 'flex', alignItems: 'stretch' }}>
        {hasRecorders && (
          <div style={{ padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 12 }}>
            <span style={colLabelStyle}>Recording</span>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
              {data.recorderEntries.map(([topic, rec]) => (
                <RecorderRow key={topic} topic={topic} rec={rec} rates={data.rates[topic] ?? { consumedPerSec: 0, writtenPerSec: 0 }} />
              ))}
            </div>
          </div>
        )}

        {data.hasCatalog && (
          <div style={{ padding: '10px 16px', borderLeft: hasRecorders ? '1px solid var(--rule)' : undefined, display: 'flex', flexDirection: 'column', gap: 12, justifyContent: 'center' }}>
            <span style={colLabelStyle}>Catalog</span>
            <DuckLakeRow totalWritten={data.totalWritten} />
          </div>
        )}

        {hasReplays && (
          <div style={{ padding: '10px 16px', borderLeft: '1px solid var(--rule)', display: 'flex', flexDirection: 'column', gap: 12 }}>
            <span style={colLabelStyle}>Replay</span>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
              {data.replays.map(replay => (
                <ReplayRow key={replay.id} replay={replay} />
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Graph builder — freestanding nodes (Kafka topics, remote DuckLake, replay
// targets, object storage) keep simple constant-gap positioning: that's
// normal diagram-node placement, not the pixel-math problem being fixed.
// Bezier edges curve to meet handles wherever they land, so exact alignment
// with the instance panel's now-self-sizing rows isn't required.
// ---------------------------------------------------------------------------

const X_KAFKA    = 0
const X_INSTANCE = 220
const KAFKA_GAP  = 90

function buildGraph(data: ClusterStateView, rates: Record<string, Rates>): { nodes: Node[]; edges: Edge[] } {
  const nodes: Node[] = []
  const edges: Edge[] = []

  const recorderEntries = Object.entries(data.self.recorders)
  const activeReplays   = data.activeReplays ?? []
  const hasCatalog      = !!data.self.catalogBackend
  const totalWritten    = recorderEntries.reduce((sum, [, rec]) => sum + rec.messagesWritten, 0)

  nodes.push({
    id: 'instance',
    type: 'instanceContainerNode',
    position: { x: X_INSTANCE, y: 0 },
    data: {
      instanceId: data.self.instanceId,
      recordingEnabled: data.self.recordingEnabled,
      compactionEnabled: data.self.compactionEnabled,
      pekkoStatus: data.self.pekkoStatus,
      reachable: data.self.pekkoReachable,
      hasCatalog,
      recorderEntries, rates, totalWritten,
      replays: activeReplays,
    } satisfies InstanceContainerNodeData,
    draggable: false, selectable: false, focusable: false,
  })

  if (!hasCatalog) {
    // Remote catalog — DuckLake shown as an external node; no Catalog column in the panel
    nodes.push({
      id: 'sink-ducklake',
      type: 'duckLakeNode',
      position: { x: X_INSTANCE + 420, y: 0 },
      data: { label: 'DuckLake', totalWritten } satisfies DuckLakeNodeData,
    })
  } else {
    nodes.push({
      id: 'obj-store',
      type: 'objectStorageNode',
      position: { x: X_INSTANCE + 40, y: 260 },
      data: { label: 'Object Storage' } satisfies ObjectStorageNodeData,
    })
    edges.push({
      id: 'e-lake-objstore',
      source: 'instance', sourceHandle: 'lake-tier-out',
      target: 'obj-store',
      type: 'particleEdge',
      data: { active: totalWritten > 0, rateLabel: 'Parquet', dashed: true } satisfies ParticleEdgeData,
    })
  }

  recorderEntries.forEach(([topic, rec], i) => {
    const r = rates[topic] ?? { consumedPerSec: 0, writtenPerSec: 0 }

    nodes.push({
      id: `kafka-${topic}`,
      type: 'kafkaTopicNode',
      position: { x: X_KAFKA, y: i * KAFKA_GAP },
      data: { label: topic, partitions: rec.assignedPartitions.length } satisfies KafkaTopicNodeData,
    })

    edges.push({
      id: `e-kafka-rec-${topic}`,
      source: `kafka-${topic}`, target: 'instance', targetHandle: `recorder-in-${topic}`,
      type: 'particleEdge',
      data: { active: rec.running, rateLabel: r.consumedPerSec > 0 ? `${fmt(r.consumedPerSec)}/s` : undefined } satisfies ParticleEdgeData,
    })

    edges.push(hasCatalog ? {
      id: `e-rec-lake-${topic}`,
      source: 'instance', sourceHandle: `recorder-out-${topic}`,
      target: 'instance', targetHandle: 'lake-in',
      type: 'particleEdge',
      data: { active: rec.running, rateLabel: r.writtenPerSec > 0 ? `${fmt(r.writtenPerSec)}/s` : undefined } satisfies ParticleEdgeData,
    } : {
      id: `e-rec-lake-${topic}`,
      source: 'instance', sourceHandle: `recorder-out-${topic}`,
      target: 'sink-ducklake',
      type: 'particleEdge',
      data: { active: rec.running, rateLabel: r.writtenPerSec > 0 ? `${fmt(r.writtenPerSec)}/s` : undefined } satisfies ParticleEdgeData,
    })
  })

  if (activeReplays.length > 0) {
    const uniqueTargets = [...new Set(activeReplays.map(r => r.targetTopic))]
    uniqueTargets.forEach((topic, k) => {
      nodes.push({
        id: `replay-target-topic-${topic}`,
        type: 'replayTargetNode',
        position: { x: X_INSTANCE + 520, y: k * KAFKA_GAP },
        data: { label: topic } satisfies ReplayTargetNodeData,
      })
    })

    activeReplays.forEach(replay => {
      edges.push(hasCatalog ? {
        id: `e-lake-replay-${replay.id}`,
        source: 'instance', sourceHandle: 'lake-replay-out',
        target: 'instance', targetHandle: `replay-in-${replay.id}`,
        type: 'particleEdge',
        data: { active: replay.status === 'running' } satisfies ParticleEdgeData,
      } : {
        id: `e-lake-replay-${replay.id}`,
        source: 'sink-ducklake',
        target: 'instance', targetHandle: `replay-in-${replay.id}`,
        type: 'particleEdge',
        data: { active: replay.status === 'running' } satisfies ParticleEdgeData,
      })
      edges.push({
        id: `e-replay-target-${replay.id}`,
        source: 'instance', sourceHandle: `replay-out-${replay.id}`,
        target: `replay-target-topic-${replay.targetTopic}`,
        type: 'particleEdge',
        data: { active: replay.status === 'running', rateLabel: replay.sentCount > 0 ? replay.sentCount.toLocaleString() : undefined } satisfies ParticleEdgeData,
      })
    })
  }

  return { nodes, edges }
}

// ---------------------------------------------------------------------------
// Node / edge type registries
// ---------------------------------------------------------------------------

const nodeTypes = {
  kafkaTopicNode:        KafkaTopicNode,
  instanceContainerNode: InstanceContainerNode,
  duckLakeNode:          DuckLakeNode,
  objectStorageNode:     ObjectStorageNode,
  replayTargetNode:      ReplayTargetNode,
}

const edgeTypes = { particleEdge: ParticleEdge }

// ---------------------------------------------------------------------------
// Inner graph component / public export
// ---------------------------------------------------------------------------

function FlowInner({ store }: { store: ParticleStore }) {
  const { data, connected } = useLiveMetrics()
  const rates = useRates(data?.self.recorders ?? {})
  const ratesRef = useRef(rates)
  ratesRef.current = rates

  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const { fitView } = useReactFlow()

  const replays = data?.activeReplays ?? []
  useParticleSpawner(data?.self.recorders ?? {}, replays, store)
  useAnimationLoop(store)

  useEffect(() => {
    if (!data) return
    const { nodes: n, edges: e } = buildGraph(data, ratesRef.current)
    setNodes(n); setEdges(e)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data])

  useEffect(() => {
    if (!data) return
    const { nodes: n, edges: e } = buildGraph(data, rates)
    setNodes(n); setEdges(e)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rates])

  const fitted = useRef(false)
  useEffect(() => {
    if (data && !fitted.current) {
      fitted.current = true
      setTimeout(() => { void fitView({ padding: 0.2, duration: 300 }) }, 50)
    }
  }, [data, fitView])

  return (
    <div style={{ position: 'relative', width: '100%', height: 560, background: 'var(--surface-raised)', borderRadius: 'var(--radius-md)', border: '1px solid var(--rule)', overflow: 'hidden' }}>
      <div style={{ position: 'absolute', top: 12, left: 16, right: 16, display: 'flex', alignItems: 'center', gap: 10, zIndex: 10, pointerEvents: 'none' }}>
        <span style={{ fontSize: 'var(--type-micro-size)', letterSpacing: 'var(--type-micro-tracking)', textTransform: 'uppercase', fontWeight: 600, color: 'var(--ink-tertiary)' }}>
          Flow Map
        </span>
        <span style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 'var(--type-caption-size)', color: connected ? 'var(--signal-live)' : 'var(--signal-error)' }}>
          <span style={{ width: 7, height: 7, borderRadius: '50%', background: 'currentColor', display: 'inline-block', animation: connected ? 'jx-pulse 2s ease infinite' : 'none' }} />
          {connected ? 'LIVE' : 'CONNECTING…'}
        </span>
        <span style={{ marginLeft: 'auto', fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)', fontFamily: 'var(--font-mono)', display: 'flex', gap: 10 }}>
          <span style={{ color: '#6E1C1C' }}>● consume</span>
          <span style={{ color: '#3E6A44' }}>● write</span>
          <span style={{ color: '#1E5A8A' }}>● replay</span>
          <span style={{ color: '#6B46A0' }}>╌ tier</span>
        </span>
      </div>

      {!data && (
        <div style={{ position: 'absolute', inset: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--ink-tertiary)', fontSize: 'var(--type-body-sm-size)' }}>
          Waiting for first metrics event…
        </div>
      )}

      <ReactFlow
        nodes={nodes} edges={edges}
        onNodesChange={onNodesChange} onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes} edgeTypes={edgeTypes}
        proOptions={{ hideAttribution: true }}
        colorMode="light"
        style={{ background: 'transparent' }}
        nodesDraggable nodesConnectable={false} elementsSelectable={false}
      >
        <Background variant={BackgroundVariant.Dots} gap={20} size={1} color="var(--rule)" />
      </ReactFlow>

      <style>{`
        @keyframes jx-pulse { 0%,100%{opacity:1} 50%{opacity:0.35} }
        .react-flow__attribution { display: none; }
      `}</style>
    </div>
  )
}

export function ClusterFlowMapObjects() {
  const storeRef = useRef<ParticleStore | null>(null)
  if (!storeRef.current) storeRef.current = makeParticleStore()
  return (
    <ParticleContext.Provider value={storeRef.current}>
      <ReactFlowProvider>
        <FlowInner store={storeRef.current} />
      </ReactFlowProvider>
    </ParticleContext.Provider>
  )
}
```

- [ ] **Step 2: Typecheck**

Run: `cd ui && npx tsc --noEmit 2>&1 | grep -v SunburstChart`
Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add ui/src/components/ClusterFlowMapObjects.tsx
git commit -m "$(cat <<'EOF'
feat(ui/cluster): add component-driven instance panel layout

New ClusterFlowMapObjects.tsx: the instance panel's recorder/DuckLake/
replay rows are now plain React children in a CSS flex layout with
named per-row Handles, instead of separately positioned React Flow
nodes driven by hand-computed pixel math (ROW_H/JOB_W/COL_GAP/CONT_W/
CONT_H). Container has no fixed width/height — React Flow auto-measures
it. Not yet wired into any route.
EOF
)"
```

---

### Task 3: Switch the route and verify in browser

**Files:**
- Modify: `ui/src/routes/cluster/index.tsx`

- [ ] **Step 1: Switch the import and usage**

In `ui/src/routes/cluster/index.tsx`:

Change:
```tsx
import { ClusterFlowMap } from '../../components/ClusterFlowMap'
```
to:
```tsx
import { ClusterFlowMapObjects } from '../../components/ClusterFlowMapObjects'
```

Change:
```tsx
{tab === 'map' && <ClusterFlowMap />}
```
to:
```tsx
{tab === 'map' && <ClusterFlowMapObjects />}
```

- [ ] **Step 2: Typecheck**

Run: `cd ui && npx tsc --noEmit 2>&1 | grep -v SunburstChart`
Expected: no output.

- [ ] **Step 3: Browser verification**

The dev server is already running at `http://localhost:5173`. Using the browser automation tools:
1. Navigate to `http://localhost:5173/cluster`.
2. Confirm the "Flow Map" tab (default) renders the instance panel with no fixed-size clipping or overlap, and that recorder/replay rows appear as cards inside CSS-flex columns.
3. Check `read_console_messages` for any React/React-Flow errors or warnings (e.g. "Couldn't create edge" from an unresolved handle).
4. Watch for a few seconds and confirm particles still animate along edges (kafka→recorder, recorder→lake, lake→object-store, and lake→replay/replay→target if any replay is active).
5. Confirm the "Detail" tab and the rest of the page still work (this route wasn't otherwise touched).
6. If the app has a light/dark mode toggle, check both; otherwise confirm it matches the OS theme with no unstyled/hardcoded-color regressions.

If anything looks wrong, fix it in `ClusterFlowMapObjects.tsx` (Task 2's file) and re-verify — do not touch `ClusterFlowMap.tsx`.

- [ ] **Step 4: Commit**

```bash
git add ui/src/routes/cluster/index.tsx
git commit -m "$(cat <<'EOF'
feat(ui/cluster): switch Flow Map tab to component-driven layout

ClusterFlowMap.tsx (the pixel-math implementation) is left intact and
unlinked for reference/rollback.
EOF
)"
```

---

## Self-Review Notes

- **Spec coverage:** all three spec sections (Architecture, Components, Data flow/edges, File organization) are covered by Tasks 1–3. The spec's "Testing / verification" section maps to Task 3 Step 3.
- **Edge-id parity:** every edge id in the new `buildGraph` matches the original's naming scheme exactly, so `useParticleSpawner` (unmodified, imported) keeps working without changes.
- **Incidental correctness fix:** the original remote-catalog (`!hasCatalog`) replay edge specified `sourceHandle: 'right'` against `DuckLakeNode`, whose Handle has no explicit `id` — a latent mismatch in the *existing* code. The new `buildGraph` omits `sourceHandle` for that specific case (matching the handle's actual default id), incidentally fixing it since this exact edge is being rewritten anyway. Not touched in `ClusterFlowMap.tsx` itself, consistent with "don't modify the old implementation."
