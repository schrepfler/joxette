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
  BaseEdge,
  EdgeLabelRenderer,
  getBezierPath,
  type NodeProps,
  type EdgeProps,
  type Node,
  type Edge,
  BackgroundVariant,
} from '@xyflow/react'
import { useEffect, useRef, useState, createContext, useContext } from 'react'
import { instancesApi, type ClusterStateView, type RecorderStatus, type ActiveReplay } from '../api/client'

// ---------------------------------------------------------------------------
// Particle system
// ---------------------------------------------------------------------------

interface Particle {
  id: number
  edgeId: string
  progress: number  // 0→1 along bezier
  speed: number     // progress/ms
  color: string
}

interface ParticleStore {
  particles: Particle[]
  subscribe: (cb: () => void) => () => void
  spawn: (edgeId: string, count: number, color: string) => void
  tick: (dt: number) => void
}

function makeParticleStore(): ParticleStore {
  let particles: Particle[] = []
  let nextId = 0
  const listeners = new Set<() => void>()
  const notify = () => listeners.forEach(cb => cb())

  return {
    get particles() { return particles },
    subscribe(cb) { listeners.add(cb); return () => listeners.delete(cb) },
    spawn(edgeId, count, color) {
      for (let i = 0; i < count; i++) {
        particles = [...particles, { id: nextId++, edgeId, progress: Math.random() * 0.15, speed: 0.0004 + Math.random() * 0.0003, color }]
      }
      notify()
    },
    tick(dt) {
      const before = particles.length
      particles = particles.map(p => ({ ...p, progress: p.progress + p.speed * dt })).filter(p => p.progress < 1)
      if (particles.length !== before || particles.length > 0) notify()
    },
  }
}

const ParticleContext = createContext<ParticleStore | null>(null)
function useParticleStore() {
  const s = useContext(ParticleContext)
  if (!s) throw new Error('ParticleContext not provided')
  return s
}

// ---------------------------------------------------------------------------
// SSE hook
// ---------------------------------------------------------------------------

function useLiveMetrics(): { data: ClusterStateView | null; connected: boolean } {
  const [data, setData] = useState<ClusterStateView | null>(null)
  const [connected, setConnected] = useState(false)

  useEffect(() => {
    const es = new EventSource(instancesApi.liveMetricsUrl())
    es.addEventListener('metrics', (e: MessageEvent) => {
      try { setData(JSON.parse(e.data as string) as ClusterStateView); setConnected(true) } catch { /* ignore */ }
    })
    es.onerror = () => setConnected(false)
    return () => es.close()
  }, [])

  return { data, connected }
}

// ---------------------------------------------------------------------------
// Particle spawner — tracks counter deltas, one dot per message (capped)
// ---------------------------------------------------------------------------

const MAX_PARTICLES_PER_TICK = 12

function useParticleSpawner(
  recorders: Record<string, RecorderStatus>,
  replays: ActiveReplay[],
  store: ParticleStore,
) {
  const prevConsumed   = useRef<Record<string, number>>({})
  const prevWritten    = useRef<Record<string, number>>({})
  const prevReplaySent = useRef<Record<string, number>>({})

  useEffect(() => {
    for (const [topic, rec] of Object.entries(recorders)) {
      const prevC = prevConsumed.current[topic] ?? rec.messagesConsumed
      const prevW = prevWritten.current[topic]  ?? rec.messagesWritten
      const dC = Math.max(0, rec.messagesConsumed - prevC)
      const dW = Math.max(0, rec.messagesWritten  - prevW)
      if (dC > 0 && rec.running) store.spawn(`e-kafka-rec-${topic}`, Math.min(dC, MAX_PARTICLES_PER_TICK), '#6E1C1C')
      if (dW > 0 && rec.running) store.spawn(`e-rec-lake-${topic}`,  Math.min(dW, MAX_PARTICLES_PER_TICK), '#3E6A44')
      prevConsumed.current[topic] = rec.messagesConsumed
      prevWritten.current[topic]  = rec.messagesWritten
    }
    for (const replay of replays) {
      if (replay.status !== 'running') continue
      const prev = prevReplaySent.current[replay.id] ?? replay.sentCount
      const delta = Math.max(0, replay.sentCount - prev)
      if (delta > 0) {
        store.spawn(`e-lake-replay-${replay.id}`,   Math.min(delta, MAX_PARTICLES_PER_TICK), '#1E5A8A')
        store.spawn(`e-replay-target-${replay.id}`, Math.min(delta, MAX_PARTICLES_PER_TICK), '#1E5A8A')
      }
      prevReplaySent.current[replay.id] = replay.sentCount
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [recorders, replays, store])
}

function useAnimationLoop(store: ParticleStore) {
  useEffect(() => {
    let raf: number
    let last = performance.now()
    const loop = (now: number) => { store.tick(now - last); last = now; raf = requestAnimationFrame(loop) }
    raf = requestAnimationFrame(loop)
    return () => cancelAnimationFrame(raf)
  }, [store])
}

// ---------------------------------------------------------------------------
// Custom particle edge
// ---------------------------------------------------------------------------

interface ParticleEdgeData extends Record<string, unknown> {
  active: boolean
  rateLabel?: string
}

function ParticleEdge({ id, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, data, markerEnd }: EdgeProps<Edge<ParticleEdgeData>>) {
  const store = useParticleStore()
  const [, forceRender] = useState(0)
  useEffect(() => store.subscribe(() => forceRender(n => n + 1)), [store])

  const [edgePath, labelX, labelY] = getBezierPath({ sourceX, sourceY, sourcePosition, targetX, targetY, targetPosition })
  const pathRef = useRef<SVGPathElement | null>(null)
  const edgeParticles = store.particles.filter(p => p.edgeId === id)

  return (
    <>
      <BaseEdge id={id} path={edgePath} markerEnd={markerEnd}
        style={{ stroke: data?.active ? 'var(--accent)' : 'var(--rule-strong)', strokeWidth: 1.5 }} />
      <path ref={pathRef} d={edgePath} fill="none" stroke="none" style={{ pointerEvents: 'none' }} />
      {edgeParticles.map(p => {
        const path = pathRef.current
        if (!path) return null
        const pt = path.getPointAtLength(p.progress * path.getTotalLength())
        return <circle key={p.id} cx={pt.x} cy={pt.y} r={3.5} fill={p.color} opacity={0.85} style={{ pointerEvents: 'none' }} />
      })}
      {data?.rateLabel && (
        <EdgeLabelRenderer>
          <div style={{
            position: 'absolute',
            transform: `translate(-50%,-50%) translate(${labelX}px,${labelY}px)`,
            fontSize: '0.6875rem', fontFamily: 'var(--font-mono)', color: 'var(--ink-secondary)',
            background: 'var(--surface-raised)', padding: '1px 5px', borderRadius: 'var(--radius-xs)',
            pointerEvents: 'none', opacity: data.active ? 1 : 0.4,
          }}>{data.rateLabel}</div>
        </EdgeLabelRenderer>
      )}
    </>
  )
}

// ---------------------------------------------------------------------------
// Node data types
// ---------------------------------------------------------------------------

interface KafkaTopicNodeData extends Record<string, unknown> { label: string; partitions: number }
interface InstanceContainerNodeData extends Record<string, unknown> {
  instanceId: string; roles: string[]; pekkoStatus: string | null; reachable: boolean; width: number; height: number
}
interface RecordJobNodeData extends Record<string, unknown> {
  topic: string; running: boolean; lag: number; consumedPerSec: number; writtenPerSec: number; partitions: number[]; error: string | null
}
interface ReplayJobNodeData extends Record<string, unknown> {
  sourceTopic: string; targetTopic: string; sentCount: number; status: ActiveReplay['status']
}
interface DuckLakeNodeData extends Record<string, unknown> { label: string; totalWritten: number }
interface ReplayTargetNodeData extends Record<string, unknown> { label: string }

// ---------------------------------------------------------------------------
// Node components
// ---------------------------------------------------------------------------

function KafkaTopicNode({ data }: NodeProps<Node<KafkaTopicNodeData>>) {
  return (
    <div style={{ ...nodeBase, minWidth: 150 }}>
      <div style={nodeHeader}>
        <span style={dotStyle('#3E6A44')} />
        <span style={nodeTitle}>{data.label}</span>
      </div>
      <div style={nodeMeta}>
        <Stat label="PARTITIONS" value={String(data.partitions)} />
      </div>
      <Handle type="source" position={Position.Right} style={handleStyle} />
    </div>
  )
}

function InstanceContainerNode({ data }: NodeProps<Node<InstanceContainerNodeData>>) {
  const statusColor = data.pekkoStatus === 'up' ? '#3E6A44' : data.pekkoStatus ? '#A26612' : 'var(--rule-strong)'
  return (
    <div style={{
      width: data.width, height: data.height,
      border: `1.5px solid ${statusColor}`,
      borderRadius: 'var(--radius-md)',
      background: 'var(--surface)',
      boxShadow: '0 2px 8px rgba(30,26,20,0.06)',
    }}>
      <div style={{ padding: '8px 12px', borderBottom: '1px solid var(--rule)', display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
        <span style={dotStyle(statusColor)} />
        <span style={{ ...nodeTitle, fontFamily: 'var(--font-mono)', fontSize: '0.6875rem' }}>{data.instanceId}</span>
        {data.roles.map(r => (
          <span key={r} style={{ ...pill, background: 'var(--surface-sunken)', color: 'var(--ink-secondary)' }}>{r}</span>
        ))}
        <span style={{ marginLeft: 'auto', fontSize: '0.5625rem', fontWeight: 700, letterSpacing: '0.08em', color: statusColor, textTransform: 'uppercase' }}>
          {data.pekkoStatus ?? 'unknown'}
          {data.reachable ? '' : ' ✗'}
        </span>
      </div>
    </div>
  )
}

function RecordJobNode({ data }: NodeProps<Node<RecordJobNodeData>>) {
  const statusColor = data.running ? (data.lag > 1000 ? '#A26612' : '#3E6A44') : '#8B2121'
  return (
    <div style={{ ...nodeBase, minWidth: 220, borderColor: statusColor }}>
      <Handle type="target" position={Position.Left} style={handleStyle} />
      <div style={nodeHeader}>
        <span style={jobTypeLabel}>RECORD</span>
        <span style={nodeTitle}>{data.topic}</span>
        <span style={{ ...pill, background: data.running ? '#dcfce7' : '#fee2e2', color: statusColor, marginLeft: 'auto' }}>
          {data.running ? 'live' : 'paused'}
        </span>
      </div>
      <div style={nodeMeta}>
        <Stat label="IN/S"  value={fmt(data.consumedPerSec)} color={statusColor} />
        <Stat label="OUT/S" value={fmt(data.writtenPerSec)} />
        <Stat label="LAG"   value={data.lag < 0 ? '—' : data.lag.toLocaleString()} color={data.lag > 1000 ? '#A26612' : undefined} />
        <Stat label="PARTS" value={data.partitions.length > 0 ? data.partitions.join(',') : '—'} />
      </div>
      {data.error && (
        <div style={{ marginTop: 4, fontSize: '0.6rem', color: '#8B2121', fontFamily: 'var(--font-mono)', wordBreak: 'break-all' }}>{data.error}</div>
      )}
      <Handle type="source" position={Position.Right} style={handleStyle} />
    </div>
  )
}

function ReplayJobNode({ data }: NodeProps<Node<ReplayJobNodeData>>) {
  const statusColor = data.status === 'running' ? '#1E5A8A' : data.status === 'completed' ? '#3E6A44' : '#8B2121'
  const statusBg    = data.status === 'running' ? '#dbeafe'  : data.status === 'completed' ? '#dcfce7'  : '#fee2e2'
  return (
    <div style={{ ...nodeBase, minWidth: 220, borderColor: statusColor }}>
      <Handle type="target" position={Position.Left} style={handleStyle} />
      <div style={nodeHeader}>
        <span style={jobTypeLabel}>REPLAY</span>
        <span style={nodeTitle}>{data.sourceTopic}</span>
        <span style={{ ...pill, background: statusBg, color: statusColor, marginLeft: 'auto' }}>{data.status}</span>
      </div>
      <div style={nodeMeta}>
        <Stat label="SENT" value={data.sentCount.toLocaleString()} color={statusColor} />
        <Stat label="→" value={data.targetTopic} />
      </div>
      <Handle type="source" position={Position.Right} style={handleStyle} />
    </div>
  )
}

function DuckLakeNode({ data }: NodeProps<Node<DuckLakeNodeData>>) {
  return (
    <div style={{ ...nodeBase, background: 'var(--surface-sunken)', minWidth: 150 }}>
      <Handle type="target" position={Position.Left} style={handleStyle} />
      <div style={nodeHeader}>
        <img src="/DuckLake_icon-darkmode.svg" alt="DuckLake" style={{ width: 18, height: 18, flexShrink: 0 }} />
        <img src="/DuckDB_icon-darkmode.svg"   alt="DuckDB"   style={{ width: 14, height: 14, flexShrink: 0, opacity: 0.6 }} />
        <span style={nodeTitle}>{data.label}</span>
      </div>
      <div style={nodeMeta}>
        <Stat label="TOTAL WRITTEN" value={data.totalWritten.toLocaleString()} />
      </div>
      <Handle type="source" position={Position.Right} style={handleStyle} />
    </div>
  )
}

function ReplayTargetNode({ data }: NodeProps<Node<ReplayTargetNodeData>>) {
  return (
    <div style={{ ...nodeBase, minWidth: 140 }}>
      <Handle type="target" position={Position.Left} style={handleStyle} />
      <div style={nodeHeader}>
        <span style={dotStyle('#1E5A8A')} />
        <span style={nodeTitle}>{data.label}</span>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Graph builder
// ---------------------------------------------------------------------------

const ROW_H          = 110
const JOB_W          = 240
const CONT_PAD_X     = 20
const CONT_PAD_TOP   = 50   // header height
const CONT_PAD_BOT   = 16
const CONT_W         = JOB_W + CONT_PAD_X * 2
const X_KAFKA        = 0
const X_INSTANCE     = 220
const X_DUCKLAKE     = X_INSTANCE + CONT_W + 70
const X_REPLAY_TGT   = X_DUCKLAKE + 180 + 60

interface Rates { consumedPerSec: number; writtenPerSec: number }

function buildGraph(data: ClusterStateView, rates: Record<string, Rates>): { nodes: Node[]; edges: Edge[] } {
  const nodes: Node[] = []
  const edges: Edge[] = []

  const recorderEntries = Object.entries(data.self.recorders)
  const activeReplays   = data.activeReplays ?? []
  const totalJobs       = Math.max(recorderEntries.length + activeReplays.length, 1)
  const CONT_H          = CONT_PAD_TOP + totalJobs * ROW_H + CONT_PAD_BOT

  const startY = -(CONT_H / 2)

  // Instance container
  nodes.push({
    id: 'instance',
    type: 'instanceContainerNode',
    position: { x: X_INSTANCE, y: startY },
    data: {
      instanceId: data.self.instanceId,
      roles: data.self.roles,
      pekkoStatus: data.self.pekkoStatus,
      reachable: data.self.pekkoReachable,
      width: CONT_W,
      height: CONT_H,
    } satisfies InstanceContainerNodeData,
    style: { width: CONT_W, height: CONT_H },
    zIndex: 0,
  })

  // Recorder job nodes (children of instance)
  recorderEntries.forEach(([topic, rec], i) => {
    const r = rates[topic] ?? { consumedPerSec: 0, writtenPerSec: 0 }
    const childY  = CONT_PAD_TOP + i * ROW_H
    const absJobY = startY + childY

    nodes.push({
      id: `recorder-${topic}`,
      type: 'recordJobNode',
      parentId: 'instance',
      extent: 'parent' as const,
      position: { x: CONT_PAD_X, y: childY },
      zIndex: 1,
      data: {
        topic, running: rec.running, lag: rec.consumerLag,
        consumedPerSec: r.consumedPerSec, writtenPerSec: r.writtenPerSec,
        partitions: Array.from(rec.assignedPartitions), error: rec.lastError,
      } satisfies RecordJobNodeData,
    })

    // Source Kafka topic (external, y-aligned with this recorder)
    nodes.push({
      id: `kafka-${topic}`,
      type: 'kafkaTopicNode',
      position: { x: X_KAFKA, y: absJobY },
      data: { label: topic, partitions: rec.assignedPartitions.length } satisfies KafkaTopicNodeData,
    })

    edges.push({
      id: `e-kafka-rec-${topic}`,
      source: `kafka-${topic}`, target: `recorder-${topic}`,
      type: 'particleEdge',
      data: { active: rec.running, rateLabel: r.consumedPerSec > 0 ? `${fmt(r.consumedPerSec)}/s` : undefined } satisfies ParticleEdgeData,
    })
    edges.push({
      id: `e-rec-lake-${topic}`,
      source: `recorder-${topic}`, target: 'sink-ducklake',
      type: 'particleEdge',
      data: { active: rec.running, rateLabel: r.writtenPerSec > 0 ? `${fmt(r.writtenPerSec)}/s` : undefined } satisfies ParticleEdgeData,
    })
  })

  // Replay job nodes (children of instance)
  activeReplays.forEach((replay, j) => {
    const childY  = CONT_PAD_TOP + (recorderEntries.length + j) * ROW_H
    const absJobY = startY + childY

    nodes.push({
      id: `replay-job-${replay.id}`,
      type: 'replayJobNode',
      parentId: 'instance',
      extent: 'parent' as const,
      position: { x: CONT_PAD_X, y: childY },
      zIndex: 1,
      data: { sourceTopic: replay.sourceTopic, targetTopic: replay.targetTopic, sentCount: replay.sentCount, status: replay.status } satisfies ReplayJobNodeData,
    })

    // Replay target Kafka topic (external)
    nodes.push({
      id: `replay-target-${replay.id}`,
      type: 'replayTargetNode',
      position: { x: X_REPLAY_TGT, y: absJobY },
      data: { label: replay.targetTopic } satisfies ReplayTargetNodeData,
    })

    edges.push({
      id: `e-lake-replay-${replay.id}`,
      source: 'sink-ducklake', target: `replay-job-${replay.id}`,
      type: 'particleEdge',
      data: { active: replay.status === 'running' } satisfies ParticleEdgeData,
    })
    edges.push({
      id: `e-replay-target-${replay.id}`,
      source: `replay-job-${replay.id}`, target: `replay-target-${replay.id}`,
      type: 'particleEdge',
      data: { active: replay.status === 'running', rateLabel: replay.sentCount > 0 ? replay.sentCount.toLocaleString() : undefined } satisfies ParticleEdgeData,
    })
  })

  // DuckLake node — centred vertically on the recorder rows
  const totalWritten = recorderEntries.reduce((sum, [, rec]) => sum + rec.messagesWritten, 0)
  const lakeY = recorderEntries.length > 0
    ? startY + CONT_PAD_TOP + ((recorderEntries.length - 1) / 2) * ROW_H
    : startY + CONT_H / 2 - 50
  nodes.push({
    id: 'sink-ducklake',
    type: 'duckLakeNode',
    position: { x: X_DUCKLAKE, y: lakeY },
    data: { label: 'DuckLake', totalWritten } satisfies DuckLakeNodeData,
  })

  return { nodes, edges }
}

// ---------------------------------------------------------------------------
// Rate computation
// ---------------------------------------------------------------------------

interface RateState { consumed: number; written: number; ts: number }

function useRates(recorders: Record<string, RecorderStatus>): Record<string, Rates> {
  const prevRef = useRef<Record<string, RateState>>({})
  const [rates, setRates] = useState<Record<string, Rates>>({})

  useEffect(() => {
    const now = Date.now()
    const next: Record<string, Rates> = {}
    const nextPrev: Record<string, RateState> = {}
    for (const [topic, rec] of Object.entries(recorders)) {
      const prev = prevRef.current[topic]
      if (prev && now - prev.ts > 100) {
        const dt = (now - prev.ts) / 1000
        next[topic] = {
          consumedPerSec: Math.max(0, (rec.messagesConsumed - prev.consumed) / dt),
          writtenPerSec:  Math.max(0, (rec.messagesWritten  - prev.written)  / dt),
        }
      } else {
        next[topic] = { consumedPerSec: 0, writtenPerSec: 0 }
      }
      nextPrev[topic] = { consumed: rec.messagesConsumed, written: rec.messagesWritten, ts: now }
    }
    prevRef.current = nextPrev
    setRates(next)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [JSON.stringify(Object.fromEntries(Object.entries(recorders).map(([k, v]) => [k, v.messagesConsumed])))])

  return rates
}

// ---------------------------------------------------------------------------
// Node / edge type registries
// ---------------------------------------------------------------------------

const nodeTypes = {
  kafkaTopicNode:        KafkaTopicNode,
  instanceContainerNode: InstanceContainerNode,
  recordJobNode:         RecordJobNode,
  replayJobNode:         ReplayJobNode,
  duckLakeNode:          DuckLakeNode,
  replayTargetNode:      ReplayTargetNode,
}

const edgeTypes = { particleEdge: ParticleEdge }

// ---------------------------------------------------------------------------
// Inner graph component
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
      {/* Overlay header */}
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

// ---------------------------------------------------------------------------
// Public export
// ---------------------------------------------------------------------------

export function ClusterFlowMap() {
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

// ---------------------------------------------------------------------------
// Shared styles
// ---------------------------------------------------------------------------

function Stat({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column' }}>
      <span style={{ fontSize: '0.5625rem', fontWeight: 600, letterSpacing: '0.1em', textTransform: 'uppercase', color: 'var(--ink-tertiary)' }}>{label}</span>
      <span style={{ fontFamily: 'var(--font-mono)', fontSize: '0.75rem', fontVariantNumeric: 'tabular-nums', color: color ?? 'var(--ink-primary)', fontWeight: 600 }}>{value}</span>
    </div>
  )
}

function fmt(n: number): string {
  if (n >= 1000) return `${(n / 1000).toFixed(1)}k`
  return Math.round(n).toString()
}

const nodeBase: React.CSSProperties = {
  background: 'var(--surface-raised)',
  border: '1px solid var(--rule)',
  borderRadius: 'var(--radius-sm)',
  padding: '8px 12px',
  minWidth: 140,
  boxShadow: '0 1px 3px rgba(30,26,20,0.07)',
  fontFamily: 'var(--font-body)',
}
const nodeHeader: React.CSSProperties = { display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6, flexWrap: 'wrap' }
const nodeTitle: React.CSSProperties = { fontSize: '0.8125rem', fontWeight: 600, color: 'var(--ink-primary)' }
const nodeMeta: React.CSSProperties = { display: 'flex', gap: 12, flexWrap: 'wrap' }
const handleStyle: React.CSSProperties = { background: 'var(--rule-strong)', border: 'none', width: 8, height: 8 }
const pill: React.CSSProperties = { fontSize: '0.5625rem', fontWeight: 600, padding: '1px 6px', borderRadius: '999px', letterSpacing: '0.04em' }
const jobTypeLabel: React.CSSProperties = { fontSize: '0.5625rem', fontWeight: 700, letterSpacing: '0.1em', color: 'var(--ink-tertiary)', textTransform: 'uppercase' }

function dotStyle(color: string): React.CSSProperties {
  return { width: 7, height: 7, borderRadius: '50%', background: color, display: 'inline-block', flexShrink: 0 }
}
