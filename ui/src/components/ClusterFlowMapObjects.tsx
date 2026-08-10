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

// Rebuilding the graph on every live-metrics tick would otherwise snap any
// manually dragged node straight back to buildGraph's computed position —
// and worse, replacing an in-progress-drag node with a freshly reconstructed
// object desyncs React Flow's own drag tracking ("trying to drag a node
// that is not initialized", xyflow error #015). The documented-safe pattern
// (https://reactflow.dev/examples/nodes/update-node) is to spread the
// EXISTING tracked node — position, measured size, drag state, all of it —
// and only replace `data`. Brand-new nodes (e.g. a recorder that just
// started) fall through to buildGraph's freshly computed node as-is.
function mergeNodeData(prev: Node[], next: Node[]): Node[] {
  const prevById = new Map(prev.map(n => [n.id, n]))
  return next.map(n => {
    const existing = prevById.get(n.id)
    return existing ? { ...existing, data: n.data } : n
  })
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
    setNodes(prev => mergeNodeData(prev, n)); setEdges(e)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data])

  useEffect(() => {
    if (!data) return
    const { nodes: n, edges: e } = buildGraph(data, rates)
    setNodes(prev => mergeNodeData(prev, n)); setEdges(e)
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
        defaultEdgeOptions={{ zIndex: 1000 }}
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
