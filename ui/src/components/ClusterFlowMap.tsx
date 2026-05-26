import '@xyflow/react/dist/style.css'
import {
  ReactFlow,
  Background,
  useNodesState,
  useEdgesState,
  Position,
  Handle,
  type NodeProps,
  type Node,
  type Edge,
  BackgroundVariant,
} from '@xyflow/react'
import { useEffect, useRef, useState, useCallback } from 'react'
import { instancesApi, type ClusterStateView, type RecorderStatus } from '../api/client'

// ---------------------------------------------------------------------------
// SSE hook
// ---------------------------------------------------------------------------

function useLiveMetrics(): { data: ClusterStateView | null; connected: boolean } {
  const [data, setData] = useState<ClusterStateView | null>(null)
  const [connected, setConnected] = useState(false)
  const esRef = useRef<EventSource | null>(null)

  useEffect(() => {
    const es = new EventSource(instancesApi.liveMetricsUrl())
    esRef.current = es

    es.addEventListener('metrics', (e: MessageEvent) => {
      try {
        setData(JSON.parse(e.data as string) as ClusterStateView)
        setConnected(true)
      } catch {
        // ignore parse errors
      }
    })

    es.onerror = () => { setConnected(false) }

    return () => {
      es.close()
      esRef.current = null
    }
  }, [])

  return { data, connected }
}

// ---------------------------------------------------------------------------
// Rate computation: derive msgs/s from cumulative counters across snapshots
// ---------------------------------------------------------------------------

interface RateState {
  consumed: number
  written: number
  ts: number
}

function useRates(recorders: Record<string, RecorderStatus>): Record<string, { consumedPerSec: number; writtenPerSec: number }> {
  const prevRef = useRef<Record<string, RateState>>({})
  const [rates, setRates] = useState<Record<string, { consumedPerSec: number; writtenPerSec: number }>>({})

  useEffect(() => {
    const now = Date.now()
    const next: Record<string, { consumedPerSec: number; writtenPerSec: number }> = {}
    const nextPrev: Record<string, RateState> = {}

    for (const [topic, rec] of Object.entries(recorders)) {
      const prev = prevRef.current[topic]
      if (prev && now - prev.ts > 100) {
        const dtSec = (now - prev.ts) / 1000
        next[topic] = {
          consumedPerSec: Math.max(0, (rec.messagesConsumed - prev.consumed) / dtSec),
          writtenPerSec:  Math.max(0, (rec.messagesWritten  - prev.written)  / dtSec),
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
// Custom node types
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
        <span style={dot('#3E6A44')} />
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
        <span style={dot(statusColor)} />
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
        <span style={dot('#6E1C1C')} />
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
        <span style={dot(statusColor)} />
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

function buildGraph(
  data: ClusterStateView,
  rates: Record<string, { consumedPerSec: number; writtenPerSec: number }>,
): { nodes: Node[]; edges: Edge[] } {
  const nodes: Node[] = []
  const edges: Edge[] = []

  const recorderEntries = Object.entries(data.self.recorders)
  const topicCount = recorderEntries.length
  const ROW_H = 120
  const startY = -(topicCount * ROW_H) / 2

  // Topic source nodes (left column)
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

  // Recorder nodes (middle column)
  recorderEntries.forEach(([topic, rec], i) => {
    const r = rates[topic] ?? { consumedPerSec: 0, writtenPerSec: 0 }
    nodes.push({
      id: `recorder-${topic}`,
      type: 'recorderNode',
      position: { x: 280, y: startY + i * ROW_H },
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

    // Edge: topic → recorder
    edges.push({
      id: `e-topic-rec-${topic}`,
      source: `topic-${topic}`,
      target: `recorder-${topic}`,
      animated: rec.running && (rates[topic]?.consumedPerSec ?? 0) > 0,
      style: edgeStyle(rec.running),
      label: rates[topic]?.consumedPerSec ? `${fmt(rates[topic].consumedPerSec)}/s` : undefined,
      labelStyle: labelStyle,
      labelBgStyle: { fill: 'var(--surface-raised)', fillOpacity: 0.85 },
    })
  })

  // DuckLake sink node (right column)
  const totalWritten = recorderEntries.reduce((sum, [, rec]) => sum + rec.messagesWritten, 0)
  nodes.push({
    id: 'sink-ducklake',
    type: 'sinkNode',
    position: { x: 560, y: -50 },
    data: {
      label: 'DuckLake',
      totalWritten,
    } satisfies SinkNodeData,
  })

  // Edges: recorders → sink
  recorderEntries.forEach(([topic, rec]) => {
    edges.push({
      id: `e-rec-sink-${topic}`,
      source: `recorder-${topic}`,
      target: 'sink-ducklake',
      animated: rec.running && (rates[topic]?.writtenPerSec ?? 0) > 0,
      style: edgeStyle(rec.running),
      label: rates[topic]?.writtenPerSec ? `${fmt(rates[topic].writtenPerSec)}/s` : undefined,
      labelStyle: labelStyle,
      labelBgStyle: { fill: 'var(--surface-raised)', fillOpacity: 0.85 },
    })
  })

  // Instance / topology nodes (bottom row)
  const topoEntries = data.topology
  const topoStartX = 0
  topoEntries.forEach((member, i) => {
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
// Main component
// ---------------------------------------------------------------------------

const nodeTypes = {
  topicNode:    TopicNode,
  recorderNode: RecorderNode,
  sinkNode:     SinkNode,
  instanceNode: InstanceNode,
}

export function ClusterFlowMap() {
  const { data, connected } = useLiveMetrics()
  const rates = useRates(data?.self.recorders ?? {})
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])

  const updateGraph = useCallback(() => {
    if (!data) return
    const { nodes: n, edges: e } = buildGraph(data, rates)
    setNodes(n)
    setEdges(e)
  }, [data, rates, setNodes, setEdges])

  useEffect(() => { updateGraph() }, [updateGraph])

  return (
    <div style={{ position: 'relative', width: '100%', height: 520, background: 'var(--surface-raised)', borderRadius: 'var(--radius-md)', border: '1px solid var(--rule)', overflow: 'hidden' }}>
      {/* Status bar */}
      <div style={{ position: 'absolute', top: 12, left: 16, right: 16, display: 'flex', alignItems: 'center', gap: 10, zIndex: 10, pointerEvents: 'none' }}>
        <span style={{ fontSize: 'var(--type-micro-size)', letterSpacing: 'var(--type-micro-tracking)', textTransform: 'uppercase', fontWeight: 600, color: 'var(--ink-tertiary)' }}>
          Flow Map
        </span>
        <span style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 'var(--type-caption-size)', color: connected ? 'var(--signal-live)' : 'var(--signal-error)' }}>
          <span style={{ width: 7, height: 7, borderRadius: '50%', background: 'currentColor', display: 'inline-block', animation: connected ? 'pulse 2s ease infinite' : 'none' }} />
          {connected ? 'LIVE' : 'CONNECTING…'}
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
        fitView
        fitViewOptions={{ padding: 0.2 }}
        proOptions={{ hideAttribution: true }}
        colorMode="light"
        style={{ background: 'transparent' }}
      >
        <Background
          variant={BackgroundVariant.Dots}
          gap={20}
          size={1}
          color="var(--rule)"
        />
      </ReactFlow>

      <style>{`
        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50%       { opacity: 0.35; }
        }
        .react-flow__edge-path { stroke-width: 1.5; }
        .react-flow__attribution { display: none; }
      `}</style>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Sub-components and styles
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

const nodeMeta: React.CSSProperties = {
  display: 'flex',
  gap: 12,
  flexWrap: 'wrap',
}

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

const labelStyle: React.CSSProperties = {
  fontSize: '0.6875rem',
  fontFamily: 'var(--font-mono)',
  fill: 'var(--ink-secondary)',
}

function dot(color: string): React.CSSProperties {
  return { width: 7, height: 7, borderRadius: '50%', background: color, display: 'inline-block', flexShrink: 0 }
}

function edgeStyle(active: boolean): React.CSSProperties {
  return { stroke: active ? 'var(--accent)' : 'var(--rule-strong)', strokeWidth: 1.5 }
}
