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
import { useEffect, useRef, useState, useCallback, createContext, useContext } from 'react'
import { instancesApi, type ClusterStateView, type RecorderStatus } from '../api/client'

// ---------------------------------------------------------------------------
// Particle system — lives outside React Flow's node/edge state so spawning a
// dot never triggers a layout recalculation.
// ---------------------------------------------------------------------------

interface Particle {
  id: number
  edgeId: string
  // progress 0→1 along the bezier path
  progress: number
  // pixels/ms travel speed
  speed: number
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
    subscribe(cb) {
      listeners.add(cb)
      return () => listeners.delete(cb)
    },
    spawn(edgeId, count, color) {
      for (let i = 0; i < count; i++) {
        particles = [...particles, {
          id: nextId++,
          edgeId,
          // stagger start positions so a burst of 5 doesn't look like one blob
          progress: Math.random() * 0.15,
          speed: 0.0004 + Math.random() * 0.0003,
          color,
        }]
      }
      notify()
    },
    tick(dt) {
      const before = particles.length
      particles = particles
        .map(p => ({ ...p, progress: p.progress + p.speed * dt }))
        .filter(p => p.progress < 1)
      if (particles.length !== before) notify()
      else if (particles.length > 0) notify()
    },
  }
}

const ParticleContext = createContext<ParticleStore | null>(null)

function useParticleStore() {
  const store = useContext(ParticleContext)
  if (!store) throw new Error('ParticleContext not provided')
  return store
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
      try {
        setData(JSON.parse(e.data as string) as ClusterStateView)
        setConnected(true)
      } catch { /* ignore */ }
    })
    es.onerror = () => { setConnected(false) }
    return () => es.close()
  }, [])

  return { data, connected }
}

// ---------------------------------------------------------------------------
// Delta → particle spawning
// Track cumulative counters between SSE snapshots; each new message gets one dot.
// Capped at 12 per edge per snapshot so a burst doesn't flood the canvas.
// ---------------------------------------------------------------------------

const MAX_PARTICLES_PER_TICK = 12

function useParticleSpawner(
  recorders: Record<string, RecorderStatus>,
  store: ParticleStore,
) {
  const prevConsumed = useRef<Record<string, number>>({})
  const prevWritten  = useRef<Record<string, number>>({})

  useEffect(() => {
    for (const [topic, rec] of Object.entries(recorders)) {
      const prevC = prevConsumed.current[topic] ?? rec.messagesConsumed
      const prevW = prevWritten.current[topic]  ?? rec.messagesWritten

      const deltaC = Math.max(0, rec.messagesConsumed - prevC)
      const deltaW = Math.max(0, rec.messagesWritten  - prevW)

      if (deltaC > 0 && rec.running) {
        store.spawn(
          `e-topic-rec-${topic}`,
          Math.min(deltaC, MAX_PARTICLES_PER_TICK),
          '#6E1C1C',   // --accent (oxblood)
        )
      }
      if (deltaW > 0 && rec.running) {
        store.spawn(
          `e-rec-sink-${topic}`,
          Math.min(deltaW, MAX_PARTICLES_PER_TICK),
          '#3E6A44',   // --signal-live (green)
        )
      }

      prevConsumed.current[topic] = rec.messagesConsumed
      prevWritten.current[topic]  = rec.messagesWritten
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [recorders, store])
}

// ---------------------------------------------------------------------------
// Animation loop — rAF drives the particle store tick
// ---------------------------------------------------------------------------

function useAnimationLoop(store: ParticleStore) {
  useEffect(() => {
    let raf: number
    let last = performance.now()
    const loop = (now: number) => {
      store.tick(now - last)
      last = now
      raf = requestAnimationFrame(loop)
    }
    raf = requestAnimationFrame(loop)
    return () => cancelAnimationFrame(raf)
  }, [store])
}

// ---------------------------------------------------------------------------
// Custom edge: draws the bezier path + live particles via SVG <circle>
// ---------------------------------------------------------------------------

interface ParticleEdgeData extends Record<string, unknown> {
  active: boolean
  rateLabel?: string
}

function ParticleEdge({
  id,
  sourceX, sourceY, targetX, targetY,
  sourcePosition, targetPosition,
  data,
  markerEnd,
}: EdgeProps<Edge<ParticleEdgeData>>) {
  const store = useParticleStore()
  const [, forceRender] = useState(0)

  useEffect(() => {
    return store.subscribe(() => forceRender(n => n + 1))
  }, [store])

  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX, sourceY, sourcePosition,
    targetX, targetY, targetPosition,
  })

  // Use a hidden SVG path element to resolve positions along the bezier
  const pathRef = useRef<SVGPathElement | null>(null)

  const edgeParticles = store.particles.filter(p => p.edgeId === id)

  return (
    <>
      <BaseEdge
        id={id}
        path={edgePath}
        markerEnd={markerEnd}
        style={{
          stroke: data?.active ? 'var(--accent)' : 'var(--rule-strong)',
          strokeWidth: 1.5,
        }}
      />

      {/* Hidden path used as a geometry ruler for particle positioning */}
      <path
        ref={pathRef}
        d={edgePath}
        fill="none"
        stroke="none"
        style={{ pointerEvents: 'none' }}
      />

      {/* Particle dots */}
      {edgeParticles.map(p => {
        const path = pathRef.current
        if (!path) return null
        const len = path.getTotalLength()
        const pt  = path.getPointAtLength(p.progress * len)
        return (
          <circle
            key={p.id}
            cx={pt.x}
            cy={pt.y}
            r={3.5}
            fill={p.color}
            opacity={0.85}
            style={{ pointerEvents: 'none' }}
          />
        )
      })}

      {/* Rate label */}
      {data?.rateLabel && (
        <EdgeLabelRenderer>
          <div
            style={{
              position: 'absolute',
              transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
              fontSize: '0.6875rem',
              fontFamily: 'var(--font-mono)',
              color: 'var(--ink-secondary)',
              background: 'var(--surface-raised)',
              padding: '1px 5px',
              borderRadius: 'var(--radius-xs)',
              pointerEvents: 'none',
              opacity: data.active ? 1 : 0.4,
            }}
          >
            {data.rateLabel}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  )
}

// ---------------------------------------------------------------------------
// Node types (unchanged from before)
// ---------------------------------------------------------------------------

interface TopicNodeData extends Record<string, unknown> {
  label: string
  partitions: number
}

interface RecorderNodeData extends Record<string, unknown> {
  topic: string
  running: boolean
  lag: number
  consumedPerSec: number
  writtenPerSec: number
  partitions: number[]
  error: string | null
}

interface SinkNodeData extends Record<string, unknown> {
  label: string
  totalWritten: number
}

interface InstanceNodeData extends Record<string, unknown> {
  instanceId: string
  roles: string[]
  pekkoStatus: string | null
  reachable: boolean
}

function TopicNode({ data }: NodeProps<Node<TopicNodeData>>) {
  return (
    <div style={nodeBase}>
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

function RecorderNode({ data }: NodeProps<Node<RecorderNodeData>>) {
  const statusColor = data.running
    ? (data.lag > 1000 ? '#A26612' : '#3E6A44')
    : '#8B2121'

  return (
    <div style={{ ...nodeBase, borderColor: statusColor, minWidth: 180 }}>
      <Handle type="target" position={Position.Left} style={handleStyle} />
      <div style={nodeHeader}>
        <span style={dotStyle(statusColor)} />
        <span style={nodeTitle}>{data.topic}</span>
        <span style={{ ...pill, background: data.running ? '#dcfce7' : '#fee2e2', color: statusColor }}>
          {data.running ? 'recording' : 'paused'}
        </span>
      </div>
      <div style={nodeMeta}>
        <Stat label="IN/S"  value={fmt(data.consumedPerSec)} color={statusColor} />
        <Stat label="OUT/S" value={fmt(data.writtenPerSec)} />
        <Stat label="LAG"   value={data.lag < 0 ? '—' : data.lag.toLocaleString()} color={data.lag > 1000 ? '#A26612' : undefined} />
        <Stat label="PARTS" value={data.partitions.length > 0 ? data.partitions.join(',') : '—'} />
      </div>
      {data.error && (
        <div style={{ marginTop: 6, fontSize: '0.625rem', color: '#8B2121', fontFamily: 'var(--font-mono)', wordBreak: 'break-all' }}>
          {data.error}
        </div>
      )}
      <Handle type="source" position={Position.Right} style={handleStyle} />
    </div>
  )
}

function SinkNode({ data }: NodeProps<Node<SinkNodeData>>) {
  return (
    <div style={{ ...nodeBase, background: 'var(--surface-sunken)', minWidth: 140 }}>
      <Handle type="target" position={Position.Left} style={handleStyle} />
      <div style={nodeHeader}>
        <span style={dotStyle('#6E1C1C')} />
        <span style={nodeTitle}>{data.label}</span>
      </div>
      <div style={nodeMeta}>
        <Stat label="TOTAL WRITTEN" value={data.totalWritten.toLocaleString()} />
      </div>
    </div>
  )
}

function InstanceNode({ data }: NodeProps<Node<InstanceNodeData>>) {
  const statusColor = data.pekkoStatus === 'up'
    ? '#3E6A44'
    : data.pekkoStatus === 'leaving' || data.pekkoStatus === 'exiting'
      ? '#A26612'
      : '#8B2121'

  return (
    <div style={{ ...nodeBase, minWidth: 180 }}>
      <Handle type="target" position={Position.Top} style={handleStyle} />
      <div style={nodeHeader}>
        <span style={dotStyle(statusColor)} />
        <span style={{ ...nodeTitle, fontFamily: 'var(--font-mono)', fontSize: '0.6875rem' }}>
          {data.instanceId}
        </span>
      </div>
      <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap', marginTop: 6 }}>
        {data.roles.map(r => (
          <span key={r} style={{ ...pill, background: 'var(--surface-sunken)', color: 'var(--ink-secondary)' }}>{r}</span>
        ))}
      </div>
      <div style={{ ...nodeMeta, marginTop: 6 }}>
        <Stat label="PEKKO" value={data.pekkoStatus ?? '—'} color={statusColor} />
        <Stat label="REACH" value={data.reachable ? '✓' : '✗'} color={data.reachable ? '#3E6A44' : '#8B2121'} />
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Graph builder
// ---------------------------------------------------------------------------

interface Rates { consumedPerSec: number; writtenPerSec: number }

function buildGraph(
  data: ClusterStateView,
  rates: Record<string, Rates>,
): { nodes: Node[]; edges: Edge[] } {
  const nodes: Node[] = []
  const edges: Edge[] = []

  const recorderEntries = Object.entries(data.self.recorders)
  const topicCount = recorderEntries.length
  const ROW_H = 130
  const startY = -(topicCount * ROW_H) / 2

  recorderEntries.forEach(([topic], i) => {
    nodes.push({
      id: `topic-${topic}`,
      type: 'topicNode',
      position: { x: 0, y: startY + i * ROW_H },
      data: {
        label: topic,
        partitions: data.self.recorders[topic]?.assignedPartitions.length ?? 0,
      } satisfies TopicNodeData,
    })
  })

  recorderEntries.forEach(([topic, rec], i) => {
    const r = rates[topic] ?? { consumedPerSec: 0, writtenPerSec: 0 }
    nodes.push({
      id: `recorder-${topic}`,
      type: 'recorderNode',
      position: { x: 300, y: startY + i * ROW_H },
      data: {
        topic,
        running: rec.running,
        lag: rec.consumerLag,
        consumedPerSec: r.consumedPerSec,
        writtenPerSec: r.writtenPerSec,
        partitions: rec.assignedPartitions,
        error: rec.lastError,
      } satisfies RecorderNodeData,
    })

    edges.push({
      id: `e-topic-rec-${topic}`,
      source: `topic-${topic}`,
      target: `recorder-${topic}`,
      type: 'particleEdge',
      data: {
        active: rec.running,
        rateLabel: r.consumedPerSec > 0 ? `${fmt(r.consumedPerSec)}/s` : undefined,
      } satisfies ParticleEdgeData,
    })
  })

  const totalWritten = recorderEntries.reduce((sum, [, rec]) => sum + rec.messagesWritten, 0)
  nodes.push({
    id: 'sink-ducklake',
    type: 'sinkNode',
    position: { x: 600, y: -50 },
    data: { label: 'DuckLake', totalWritten } satisfies SinkNodeData,
  })

  recorderEntries.forEach(([topic, rec]) => {
    const r = rates[topic] ?? { consumedPerSec: 0, writtenPerSec: 0 }
    edges.push({
      id: `e-rec-sink-${topic}`,
      source: `recorder-${topic}`,
      target: 'sink-ducklake',
      type: 'particleEdge',
      data: {
        active: rec.running,
        rateLabel: r.writtenPerSec > 0 ? `${fmt(r.writtenPerSec)}/s` : undefined,
      } satisfies ParticleEdgeData,
    })
  })

  const topoStartX = 0
  data.topology.forEach((member, i) => {
    const shortId = member.address.replace('pekko://joxette@', '')
    nodes.push({
      id: `topo-${member.address}`,
      type: 'instanceNode',
      position: { x: topoStartX + i * 260, y: startY + topicCount * ROW_H + 60 },
      data: {
        instanceId: shortId,
        roles: Array.from(member.roles),
        pekkoStatus: member.status,
        reachable: member.reachable,
      } satisfies InstanceNodeData,
    })
  })

  return { nodes, edges }
}

// ---------------------------------------------------------------------------
// Rate computation (used only for labels now; spawning uses raw deltas)
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
// Inner graph component (needs ReactFlowProvider context for useReactFlow)
// ---------------------------------------------------------------------------

const nodeTypes = {
  topicNode:    TopicNode,
  recorderNode: RecorderNode,
  sinkNode:     SinkNode,
  instanceNode: InstanceNode,
}

const edgeTypes = {
  particleEdge: ParticleEdge,
}

function FlowInner({ store }: { store: ParticleStore }) {
  const { data, connected } = useLiveMetrics()
  const rates = useRates(data?.self.recorders ?? {})
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const { fitView } = useReactFlow()

  useParticleSpawner(data?.self.recorders ?? {}, store)
  useAnimationLoop(store)

  const updateGraph = useCallback(() => {
    if (!data) return
    const { nodes: n, edges: e } = buildGraph(data, rates)
    setNodes(n)
    setEdges(e)
  }, [data, rates, setNodes, setEdges])

  useEffect(() => { updateGraph() }, [updateGraph])

  // fitView only on first data arrival
  const fitted = useRef(false)
  useEffect(() => {
    if (data && !fitted.current) {
      fitted.current = true
      setTimeout(() => { void fitView({ padding: 0.2, duration: 300 }) }, 50)
    }
  }, [data, fitView])

  return (
    <div style={{ position: 'relative', width: '100%', height: 520, background: 'var(--surface-raised)', borderRadius: 'var(--radius-md)', border: '1px solid var(--rule)', overflow: 'hidden' }}>
      <div style={{ position: 'absolute', top: 12, left: 16, right: 16, display: 'flex', alignItems: 'center', gap: 10, zIndex: 10, pointerEvents: 'none' }}>
        <span style={{ fontSize: 'var(--type-micro-size)', letterSpacing: 'var(--type-micro-tracking)', textTransform: 'uppercase', fontWeight: 600, color: 'var(--ink-tertiary)' }}>
          Flow Map
        </span>
        <span style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 'var(--type-caption-size)', color: connected ? 'var(--signal-live)' : 'var(--signal-error)' }}>
          <span style={{ width: 7, height: 7, borderRadius: '50%', background: 'currentColor', display: 'inline-block', animation: connected ? 'jx-pulse 2s ease infinite' : 'none' }} />
          {connected ? 'LIVE' : 'CONNECTING…'}
        </span>
        <span style={{ marginLeft: 'auto', fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)', fontFamily: 'var(--font-mono)' }}>
          ● consumed &nbsp;&nbsp; ● written
        </span>
      </div>

      {!data && (
        <div style={{ position: 'absolute', inset: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--ink-tertiary)', fontSize: 'var(--type-body-sm-size)' }}>
          Waiting for first metrics event…
        </div>
      )}

      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        proOptions={{ hideAttribution: true }}
        colorMode="light"
        style={{ background: 'transparent' }}
        nodesDraggable
        nodesConnectable={false}
        elementsSelectable={false}
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
// Public export — wraps FlowInner in the particle context + ReactFlowProvider
// ---------------------------------------------------------------------------

export function ClusterFlowMap() {
  // Stable store instance across renders
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
      <span style={{ fontSize: '0.5625rem', fontWeight: 600, letterSpacing: '0.1em', textTransform: 'uppercase', color: 'var(--ink-tertiary)' }}>
        {label}
      </span>
      <span style={{ fontFamily: 'var(--font-mono)', fontSize: '0.75rem', fontVariantNumeric: 'tabular-nums', color: color ?? 'var(--ink-primary)', fontWeight: 600 }}>
        {value}
      </span>
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
  padding: '10px 14px',
  minWidth: 140,
  boxShadow: '0 1px 3px rgba(30,26,20,0.07)',
  fontFamily: 'var(--font-body)',
}

const nodeHeader: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: 6,
  marginBottom: 6,
  flexWrap: 'wrap',
}

const nodeTitle: React.CSSProperties = {
  fontSize: '0.8125rem',
  fontWeight: 600,
  color: 'var(--ink-primary)',
}

const nodeMeta: React.CSSProperties = { display: 'flex', gap: 12, flexWrap: 'wrap' }

const handleStyle: React.CSSProperties = {
  background: 'var(--rule-strong)',
  border: 'none',
  width: 8,
  height: 8,
}

const pill: React.CSSProperties = {
  fontSize: '0.5625rem',
  fontWeight: 600,
  padding: '1px 6px',
  borderRadius: '999px',
  letterSpacing: '0.04em',
}

function dotStyle(color: string): React.CSSProperties {
  return { width: 7, height: 7, borderRadius: '50%', background: color, display: 'inline-block', flexShrink: 0 }
}
