/**
 * CassetteTimeline
 *
 * A two-panel cassette replay viewer:
 *   Upper panel  — syntax-highlighted JSON viewer for the selected message
 *   Lower panel  — horizontal canvas timeline
 *
 * Timeline features:
 *   • Messages plotted proportional to their kafka_timestamp spacing
 *   • Click a marker to select it
 *   • ← / → arrow keys to step message-by-message
 *   • Drag the timeline to pan
 *   • Scroll-wheel / pinch to zoom (recentres on the selected message)
 *   • "Fit to window" button
 *   • Colour coding by `colorKey` (partition for topics, topic for entities)
 *   • Progressive page loading as the user scrubs toward the edges
 */

import {
  useRef,
  useEffect,
  useCallback,
  useState,
  useMemo,
  type ReactNode,
} from 'react'
import { JsonView } from './JsonView'

// ─── Types ───────────────────────────────────────────────────────────────────

export interface TimelineRecord {
  /** Kafka message timestamp (ISO 8601) */
  timestamp: string
  /** Used for colour coding (e.g. partition number or topic name) */
  colorKey: string
  /** The full JSON value string (may be base64-encoded JSON) */
  value: string | null
  /** Any extra metadata to show in the detail panel (key-value pairs) */
  meta: Record<string, string>
  /** Kafka headers — used by the Header group-by dimension */
  headers?: Array<{ key: string; value: string }>
  /** The originating Kafka topic — used by the Source topic group-by dimension */
  sourceTopic?: string
  /** The business entity ID — used by the Entity ID group-by dimension */
  entityId?: string
}

export interface CassetteTimelineProps {
  records: TimelineRecord[]
  /** Called when the user scrolls near the start; return false to suppress */
  onLoadBefore?: () => void
  /** Called when the user scrolls near the end; return false to suppress */
  onLoadAfter?: () => void
  /** Whether more pages exist before the current window */
  hasMore?: boolean
  /** Whether a page load is in progress */
  loading?: boolean
  /** Optional label shown in the header */
  title?: string
  /** Extra controls rendered next to the "Fit" button */
  extraControls?: ReactNode
  /** Show the "Message type" group-by option (only meaningful for entity cassettes) */
  supportsMessageType?: boolean
  /** Called whenever the primary group-by mode changes */
  onGroupByModeChange?: (mode: GroupByMode) => void
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function tryDecodeBase64(s: string): string | null {
  try {
    if (!/^[A-Za-z0-9+/\-_]+=*$/.test(s)) return null
    return atob(s.replace(/-/g, '+').replace(/_/g, '/'))
  } catch {
    return null
  }
}

function tryParseJson(s: string | null): { parsed: unknown; raw: string } | null {
  if (!s) return null
  try { return { parsed: JSON.parse(s) as unknown, raw: s } } catch { /* */ }
  const decoded = tryDecodeBase64(s)
  if (decoded) {
    try { return { parsed: JSON.parse(decoded) as unknown, raw: decoded } } catch { /* */ }
  }
  return null
}

function isoToMs(ts: string): number {
  return new Date(ts).getTime()
}

function msToLabel(ms: number): string {
  const d = new Date(ms)
  const pad = (n: number, len = 2) => String(n).padStart(len, '0')
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`
}

function msToFullLabel(ms: number): string {
  const d = new Date(ms)
  return d.toISOString().slice(0, 23).replace('T', ' ')
}

export const PALETTE = [
  '#3182ce', '#38a169', '#d69e2e', '#e53e3e',
  '#805ad5', '#dd6b20', '#00b5d8', '#e91e8c',
  '#00897b', '#f44336',
]

const MAX_GROUPS = 12

export function colorForKey(key: string, allKeys: string[]): string {
  const idx = allKeys.indexOf(key)
  return PALETTE[idx % PALETTE.length] ?? '#718096'
}

// ─── Group-by dimensions ─────────────────────────────────────────────────────

export type GroupByMode =
  | { kind: 'colorKey' }
  | { kind: 'entityId' }
  | { kind: 'messageType' }
  | { kind: 'topic' }
  | { kind: 'header'; headerKey: string }
  | { kind: 'jsonpath'; expression: string }

/** Evaluate a simple dot-path JSONPath expression (e.g. $.field, $.a.b.c) */
function evalJsonPath(rawValue: string | null, expr: string): string | null {
  if (!rawValue || !expr.startsWith('$.')) return null
  const parsed = tryParseJson(rawValue)
  if (!parsed) return null
  const parts = expr.slice(2).split('.')
  let node: unknown = parsed.parsed
  for (const part of parts) {
    if (part === '') continue
    if (node == null || typeof node !== 'object') return null
    node = (node as Record<string, unknown>)[part]
  }
  if (node == null) return null
  if (typeof node === 'object') return JSON.stringify(node)
  return String(node)
}

function getEffectiveColorKey(record: TimelineRecord, mode: GroupByMode): string {
  switch (mode.kind) {
    case 'colorKey':
      return record.colorKey
    case 'entityId':
      return record.entityId ?? '(none)'
    case 'messageType':
      return record.meta?.type ?? '(unknown)'
    case 'topic':
      return record.sourceTopic ?? '(none)'
    case 'header': {
      const match = (record.headers ?? []).find(h => h.key === mode.headerKey)
      return match?.value ?? '(none)'
    }
    case 'jsonpath':
      return evalJsonPath(record.value, mode.expression) ?? '(none)'
  }
}

// ─── GroupBySelector ─────────────────────────────────────────────────────────

interface GroupBySelectorProps {
  mode: GroupByMode
  availableHeaderKeys: string[]
  supportsMessageType?: boolean
  onChange: (mode: GroupByMode) => void
}

function GroupBySelector({ mode, availableHeaderKeys, supportsMessageType, onChange }: GroupBySelectorProps) {
  const [draftExpr, setDraftExpr] = useState(
    mode.kind === 'jsonpath' ? mode.expression : '',
  )

  function commitExpr(expr: string) {
    onChange({ kind: 'jsonpath', expression: expr.trim() || '$.' })
  }

  function handleDimensionChange(e: React.ChangeEvent<HTMLSelectElement>) {
    const val = e.target.value
    if (val === 'colorKey') { onChange({ kind: 'colorKey' }); return }
    if (val === 'entityId') { onChange({ kind: 'entityId' }); return }
    if (val === 'messageType') { onChange({ kind: 'messageType' }); return }
    if (val === 'topic') { onChange({ kind: 'topic' }); return }
    if (val === 'header') {
      const firstKey = availableHeaderKeys[0] ?? ''
      onChange({ kind: 'header', headerKey: firstKey })
      return
    }
    if (val === 'jsonpath') {
      const expr = mode.kind === 'jsonpath' ? mode.expression : '$.'
      setDraftExpr(expr)
      onChange({ kind: 'jsonpath', expression: expr })
      return
    }
  }

  const dimensionValue = mode.kind

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
      <span style={{ fontSize: 11, color: '#718096', fontWeight: 600 }}>Group by</span>
      <select
        value={dimensionValue}
        onChange={handleDimensionChange}
        style={selectStyle}
        title="Choose how to colour-code messages"
      >
        <option value="colorKey">Default</option>
        <option value="entityId">Entity ID</option>
        {supportsMessageType && <option value="messageType">Message type</option>}
        <option value="topic">Source topic</option>
        <option value="header">Header value</option>
        <option value="jsonpath">JSONPath</option>
      </select>

      {mode.kind === 'header' && (
        <select
          value={mode.headerKey}
          onChange={e => onChange({ kind: 'header', headerKey: e.target.value })}
          style={selectStyle}
          title="Header key to group by"
        >
          {availableHeaderKeys.length === 0
            ? <option value="">(no headers in loaded messages)</option>
            : availableHeaderKeys.map(k => <option key={k} value={k}>{k}</option>)
          }
        </select>
      )}

      {mode.kind === 'jsonpath' && (
        <input
          type="text"
          value={draftExpr}
          placeholder="$.field"
          onChange={e => setDraftExpr(e.target.value)}
          onBlur={e => commitExpr(e.target.value)}
          onKeyDown={e => { if (e.key === 'Enter') commitExpr(draftExpr) }}
          style={{
            ...selectStyle,
            width: 120,
            fontFamily: 'monospace',
          }}
          title="JSONPath expression (e.g. $.status, $.type)"
        />
      )}
    </div>
  )
}

// ─── AndBySelector ───────────────────────────────────────────────────────────

interface AndBySelectorProps {
  mode: GroupByMode | null
  availableHeaderKeys: string[]
  excludeKind: GroupByMode['kind']
  supportsMessageType?: boolean
  onChange: (mode: GroupByMode | null) => void
}

function AndBySelector({ mode, availableHeaderKeys, excludeKind, supportsMessageType, onChange }: AndBySelectorProps) {
  const [draftExpr, setDraftExpr] = useState(
    mode?.kind === 'jsonpath' ? mode.expression : '',
  )

  function commitExpr(expr: string) {
    onChange({ kind: 'jsonpath', expression: expr.trim() || '$.' })
  }

  function handleDimensionChange(e: React.ChangeEvent<HTMLSelectElement>) {
    const val = e.target.value
    if (val === '__none__') { onChange(null); return }
    if (val === 'colorKey') { onChange({ kind: 'colorKey' }); return }
    if (val === 'entityId') { onChange({ kind: 'entityId' }); return }
    if (val === 'messageType') { onChange({ kind: 'messageType' }); return }
    if (val === 'topic') { onChange({ kind: 'topic' }); return }
    if (val === 'header') {
      onChange({ kind: 'header', headerKey: availableHeaderKeys[0] ?? '' })
      return
    }
    if (val === 'jsonpath') {
      const expr = mode?.kind === 'jsonpath' ? mode.expression : '$.'
      setDraftExpr(expr)
      onChange({ kind: 'jsonpath', expression: expr })
      return
    }
  }

  const dimensionValue = mode?.kind ?? '__none__'

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
      <span style={{ fontSize: 11, color: '#718096', fontWeight: 600 }}>And by</span>
      <select
        value={dimensionValue}
        onChange={handleDimensionChange}
        style={selectStyle}
        title="Choose a secondary colour-coding dimension"
      >
        <option value="__none__">— none —</option>
        {excludeKind !== 'colorKey' && <option value="colorKey">Default</option>}
        {excludeKind !== 'entityId' && <option value="entityId">Entity ID</option>}
        {supportsMessageType && excludeKind !== 'messageType' && <option value="messageType">Message type</option>}
        {excludeKind !== 'topic' && <option value="topic">Source topic</option>}
        {excludeKind !== 'header' && <option value="header">Header value</option>}
        {excludeKind !== 'jsonpath' && <option value="jsonpath">JSONPath</option>}
      </select>

      {mode?.kind === 'header' && (
        <select
          value={mode.headerKey}
          onChange={e => onChange({ kind: 'header', headerKey: e.target.value })}
          style={selectStyle}
          title="Header key to group by"
        >
          {availableHeaderKeys.length === 0
            ? <option value="">(no headers in loaded messages)</option>
            : availableHeaderKeys.map(k => <option key={k} value={k}>{k}</option>)
          }
        </select>
      )}

      {mode?.kind === 'jsonpath' && (
        <input
          type="text"
          value={draftExpr}
          placeholder="$.field"
          onChange={e => setDraftExpr(e.target.value)}
          onBlur={e => commitExpr(e.target.value)}
          onKeyDown={e => { if (e.key === 'Enter') commitExpr(draftExpr) }}
          style={{ ...selectStyle, width: 120, fontFamily: 'monospace' }}
          title="JSONPath expression (e.g. $.status, $.type)"
        />
      )}
    </div>
  )
}

// ─── Canvas timeline ─────────────────────────────────────────────────────────

const TIMELINE_HEIGHT = 160        // px for the whole canvas
const MARKER_Y = 90                // y-centre of markers
const MARKER_RADIUS = 6
const SELECTED_RADIUS = 9
const TICK_Y = TIMELINE_HEIGHT - 28

interface ViewState {
  /** ms-per-pixel (world units per canvas pixel) */
  msPerPx: number
  /** The canvas x-pixel that corresponds to the global epoch centre */
  originPx: number
}

// ─── TimelineCanvas ───────────────────────────────────────────────────────────

interface TimelineCanvasProps {
  records: TimelineRecord[]
  selectedIdx: number
  colorKeys: string[]
  onSelect: (idx: number) => void
  onViewChange?: (vs: ViewState) => void
  fitKey?: number  // increment to trigger fit-to-window
}

function TimelineCanvas({ records, selectedIdx, colorKeys, onSelect, fitKey }: TimelineCanvasProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const vsRef = useRef<ViewState>({ msPerPx: 1, originPx: 0 })
  const dragRef = useRef<{ startX: number; startOriginPx: number } | null>(null)
  const lastPinchDistRef = useRef<number | null>(null)

  const timestamps = useMemo(() => records.map(r => isoToMs(r.timestamp)), [records])
  const minMs = useMemo(() => (timestamps.length ? Math.min(...timestamps) : 0), [timestamps])
  const maxMs = useMemo(() => (timestamps.length ? Math.max(...timestamps) : 1), [timestamps])

  // Fit-to-window logic
  const fit = useCallback(() => {
    const canvas = canvasRef.current
    if (!canvas || timestamps.length === 0) return
    const w = canvas.width
    const span = maxMs - minMs || 1000
    const padding = 60
    const msPerPx = span / (w - padding * 2)
    vsRef.current = {
      msPerPx: Math.max(msPerPx, 0.001),
      originPx: padding - minMs / (span / (w - padding * 2)),
    }
    draw()
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [timestamps, minMs, maxMs])

  // Fit on mount and when fitKey changes
  useEffect(() => { fit() }, [fit, fitKey])

  // Recentre on selected message when it changes (without zooming)
  useEffect(() => {
    if (selectedIdx < 0 || !timestamps[selectedIdx]) return
    const canvas = canvasRef.current
    if (!canvas) return
    const vs = vsRef.current
    const selectedX = vs.originPx + (timestamps[selectedIdx] - 0) / vs.msPerPx
    // Shift so selected marker is centred
    const targetX = canvas.width / 2
    vsRef.current = { ...vs, originPx: vs.originPx + (targetX - selectedX) }
    draw()
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedIdx])

  const draw = useCallback(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    if (!ctx) return
    const vs = vsRef.current
    const w = canvas.width
    const h = canvas.height
    const dpr = window.devicePixelRatio || 1

    ctx.clearRect(0, 0, w * dpr, h * dpr)
    ctx.save()
    ctx.scale(dpr, dpr)

    // Background
    ctx.fillStyle = '#f7fafc'
    ctx.fillRect(0, 0, w, h)

    // Timeline axis
    ctx.strokeStyle = '#cbd5e0'
    ctx.lineWidth = 1
    ctx.beginPath()
    ctx.moveTo(0, MARKER_Y + SELECTED_RADIUS + 8)
    ctx.lineTo(w, MARKER_Y + SELECTED_RADIUS + 8)
    ctx.stroke()

    // Tick marks
    if (timestamps.length > 0) {
      const spanMs = maxMs - minMs || 1000
      const visibleSpanMs = w * vs.msPerPx
      // Choose tick interval
      const rawTickInterval = visibleSpanMs / 8
      const magnitudes = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000,
        10000, 30000, 60000, 120000, 300000, 600000, 1800000, 3600000]
      const tickInterval = magnitudes.find(m => m >= rawTickInterval) ?? magnitudes[magnitudes.length - 1]

      const firstTick = Math.ceil(minMs / tickInterval) * tickInterval
      const lastTickMs = minMs + visibleSpanMs + spanMs
      ctx.strokeStyle = '#a0aec0'
      ctx.fillStyle = '#718096'
      ctx.font = '10px system-ui'
      ctx.textAlign = 'center'
      for (let t = firstTick; t <= lastTickMs; t += tickInterval) {
        const x = vs.originPx + t / vs.msPerPx
        if (x < -20 || x > w + 20) continue
        ctx.beginPath()
        ctx.moveTo(x, TICK_Y)
        ctx.lineTo(x, TICK_Y + 8)
        ctx.stroke()
        ctx.fillText(msToLabel(t), x, TICK_Y + 20)
      }
    }

    // Markers
    records.forEach((r, i) => {
      const ms = timestamps[i]
      if (ms == null) return
      const x = vs.originPx + ms / vs.msPerPx
      if (x < -20 || x > w + 20) return

      const color = colorForKey(r.colorKey, colorKeys)
      const isSelected = i === selectedIdx
      const radius = isSelected ? SELECTED_RADIUS : MARKER_RADIUS

      // Shadow for selected
      if (isSelected) {
        ctx.shadowColor = color
        ctx.shadowBlur = 10
      }

      ctx.beginPath()
      ctx.arc(x, MARKER_Y, radius, 0, Math.PI * 2)
      ctx.fillStyle = isSelected ? color : color + '99'
      ctx.fill()
      ctx.strokeStyle = isSelected ? color : '#fff'
      ctx.lineWidth = isSelected ? 2.5 : 1.5
      ctx.stroke()

      ctx.shadowBlur = 0

      // Label on selected
      if (isSelected) {
        ctx.fillStyle = '#1a202c'
        ctx.font = 'bold 11px system-ui'
        ctx.textAlign = 'center'
        const label = r.colorKey
        ctx.fillText(label, x, MARKER_Y - SELECTED_RADIUS - 6)
      }
    })

    ctx.restore()
  }, [records, timestamps, selectedIdx, colorKeys, minMs, maxMs])

  // Redraw when data/selection changes
  useEffect(() => { draw() }, [draw])

  // Resize observer
  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const ro = new ResizeObserver(() => {
      const dpr = window.devicePixelRatio || 1
      canvas.width = canvas.offsetWidth * dpr
      canvas.height = TIMELINE_HEIGHT * dpr
      draw()
    })
    ro.observe(canvas)
    return () => ro.disconnect()
  }, [draw])

  // Hit-test: find nearest marker to a canvas x coordinate
  const hitTest = useCallback((canvasX: number): number => {
    const vs = vsRef.current
    let best = -1
    let bestDist = 24 // px threshold
    timestamps.forEach((ms, i) => {
      const x = vs.originPx + ms / vs.msPerPx
      const dist = Math.abs(canvasX - x)
      if (dist < bestDist) { bestDist = dist; best = i }
    })
    return best
  }, [timestamps])

  // Pointer events
  const onPointerDown = useCallback((e: React.PointerEvent<HTMLCanvasElement>) => {
    const canvas = canvasRef.current
    if (!canvas) return
    const rect = canvas.getBoundingClientRect()
    const x = e.clientX - rect.left
    dragRef.current = { startX: x, startOriginPx: vsRef.current.originPx }
    canvas.setPointerCapture(e.pointerId)
  }, [])

  const onPointerMove = useCallback((e: React.PointerEvent<HTMLCanvasElement>) => {
    if (!dragRef.current) return
    const canvas = canvasRef.current
    if (!canvas) return
    const rect = canvas.getBoundingClientRect()
    const x = e.clientX - rect.left
    const dx = x - dragRef.current.startX
    vsRef.current = { ...vsRef.current, originPx: dragRef.current.startOriginPx + dx }
    draw()
  }, [draw])

  const onPointerUp = useCallback((e: React.PointerEvent<HTMLCanvasElement>) => {
    if (!dragRef.current) return
    const canvas = canvasRef.current
    if (!canvas) return
    const rect = canvas.getBoundingClientRect()
    const x = e.clientX - rect.left
    const dx = Math.abs(x - dragRef.current.startX)
    dragRef.current = null
    // If movement was tiny, treat as click
    if (dx < 5) {
      const hit = hitTest(x)
      if (hit >= 0) onSelect(hit)
    }
  }, [hitTest, onSelect])

  // Wheel zoom
  const onWheel = useCallback((e: React.WheelEvent<HTMLCanvasElement>) => {
    e.preventDefault()
    const canvas = canvasRef.current
    if (!canvas || timestamps.length === 0) return
    const rect = canvas.getBoundingClientRect()
    const cursorX = e.clientX - rect.left

    const zoomFactor = e.deltaY > 0 ? 1.15 : 1 / 1.15
    const vs = vsRef.current

    // World position under cursor stays fixed
    const worldUnderCursor = (cursorX - vs.originPx) * vs.msPerPx
    const newMsPerPx = Math.max(0.001, vs.msPerPx * zoomFactor)
    const newOriginPx = cursorX - worldUnderCursor / newMsPerPx
    vsRef.current = { msPerPx: newMsPerPx, originPx: newOriginPx }
    draw()
  }, [timestamps, draw])

  // Touch pinch zoom
  const onTouchStart = useCallback((e: React.TouchEvent<HTMLCanvasElement>) => {
    if (e.touches.length === 2) {
      const dx = e.touches[0].clientX - e.touches[1].clientX
      const dy = e.touches[0].clientY - e.touches[1].clientY
      lastPinchDistRef.current = Math.hypot(dx, dy)
    }
  }, [])

  const onTouchMove = useCallback((e: React.TouchEvent<HTMLCanvasElement>) => {
    if (e.touches.length !== 2 || lastPinchDistRef.current == null) return
    const dx = e.touches[0].clientX - e.touches[1].clientX
    const dy = e.touches[0].clientY - e.touches[1].clientY
    const dist = Math.hypot(dx, dy)
    const scale = dist / lastPinchDistRef.current
    lastPinchDistRef.current = dist

    const canvas = canvasRef.current
    if (!canvas) return
    const midX = (e.touches[0].clientX + e.touches[1].clientX) / 2 - canvas.getBoundingClientRect().left
    const vs = vsRef.current
    const worldMid = (midX - vs.originPx) * vs.msPerPx
    const newMsPerPx = Math.max(0.001, vs.msPerPx / scale)
    vsRef.current = { msPerPx: newMsPerPx, originPx: midX - worldMid / newMsPerPx }
    draw()
  }, [draw])

  return (
    <canvas
      ref={canvasRef}
      style={{ width: '100%', height: TIMELINE_HEIGHT, cursor: dragRef.current ? 'grabbing' : 'grab', display: 'block' }}
      onPointerDown={onPointerDown}
      onPointerMove={onPointerMove}
      onPointerUp={onPointerUp}
      onWheel={onWheel}
      onTouchStart={onTouchStart}
      onTouchMove={onTouchMove}
    />
  )
}


// ─── JSON Detail Panel ────────────────────────────────────────────────────────

function DetailPanel({ record }: { record: TimelineRecord | null }) {
  if (!record) {
    return (
      <div style={{ padding: '2rem', textAlign: 'center', color: '#a0aec0', fontSize: 14 }}>
        Select a message on the timeline below
      </div>
    )
  }

  const parsed = tryParseJson(record.value)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12, padding: '1rem' }}>
      {/* Meta chips */}
      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center' }}>
        <span style={{ fontSize: 12, fontWeight: 700, color: '#718096', marginRight: 4 }}>
          {msToFullLabel(isoToMs(record.timestamp))}
        </span>
        {Object.entries(record.meta).map(([k, v]) => (
          <MetaChip key={k} label={k} value={v} />
        ))}
      </div>

      {/* Value */}
      {parsed ? (
        <JsonView src={parsed.parsed as object} collapsed={false} />
      ) : record.value ? (
        <pre style={{
          margin: 0, padding: '0.75rem',
          background: '#1a202c', color: '#e2e8f0',
          borderRadius: 6, fontSize: 12,
          overflowX: 'auto', whiteSpace: 'pre-wrap', wordBreak: 'break-all',
        }}>
          {record.value}
        </pre>
      ) : (
        <span style={{ color: '#a0aec0', fontSize: 13 }}>No value</span>
      )}
    </div>
  )
}

function MetaChip({ label, value }: { label: string; value: string }) {
  return (
    <div style={{
      background: '#edf2f7', border: '1px solid #e2e8f0', borderRadius: 4,
      padding: '2px 8px', display: 'inline-flex', gap: 5, alignItems: 'baseline',
    }}>
      <span style={{ fontSize: 10, color: '#718096', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.04em' }}>{label}</span>
      <span style={{ fontSize: 12, color: '#2d3748', fontFamily: 'monospace' }}>{value}</span>
    </div>
  )
}

// ─── Legend ───────────────────────────────────────────────────────────────────

function Legend({ colorKeys }: { colorKeys: string[] }) {
  if (colorKeys.length === 0) return null
  return (
    <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center' }}>
      {colorKeys.map((k, i) => (
        <span key={k} style={{
          display: 'inline-flex', alignItems: 'center', gap: 5,
          fontSize: 12, color: '#4a5568',
        }}>
          <span style={{
            display: 'inline-block', width: 10, height: 10, borderRadius: '50%',
            background: PALETTE[i % PALETTE.length],
          }} />
          {k}
        </span>
      ))}
    </div>
  )
}

// ─── Main component ───────────────────────────────────────────────────────────

export function CassetteTimeline({
  records,
  onLoadBefore,
  onLoadAfter,
  hasMore,
  loading,
  title,
  extraControls,
  supportsMessageType,
  onGroupByModeChange,
}: CassetteTimelineProps) {
  const [selectedIdx, setSelectedIdx] = useState(0)
  const [fitKey, setFitKey] = useState(0)
  const [groupByMode, setGroupByMode] = useState<GroupByMode>(
    () => supportsMessageType ? { kind: 'messageType' } : { kind: 'colorKey' },
  )
  const [groupByMode2, setGroupByMode2] = useState<GroupByMode | null>(null)

  const availableHeaderKeys = useMemo(
    () => [...new Set(records.flatMap(r => (r.headers ?? []).map(h => h.key)))].sort(),
    [records],
  )

  useEffect(() => {
    onGroupByModeChange?.(groupByMode)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [groupByMode])

  const effectiveRecords = useMemo(
    () => records.map(r => ({
      ...r,
      colorKey: groupByMode2
        ? `${getEffectiveColorKey(r, groupByMode)} / ${getEffectiveColorKey(r, groupByMode2)}`
        : getEffectiveColorKey(r, groupByMode),
    })),
    [records, groupByMode, groupByMode2],
  )

  const allColorKeys = useMemo(
    () => [...new Set(effectiveRecords.map(r => r.colorKey))].sort(),
    [effectiveRecords],
  )

  const tooManyGroups = allColorKeys.length > MAX_GROUPS

  const colorKeys = useMemo(() => {
    if (!tooManyGroups) return allColorKeys
    const countMap = new Map<string, number>()
    effectiveRecords.forEach(r => countMap.set(r.colorKey, (countMap.get(r.colorKey) ?? 0) + 1))
    return [...countMap.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, MAX_GROUPS)
      .map(([k]) => k)
      .sort()
  }, [allColorKeys, tooManyGroups, effectiveRecords])

  const selectedRecord = effectiveRecords[selectedIdx] ?? null

  // Keyboard navigation
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      // Only handle when not inside an input/textarea
      const tag = (e.target as HTMLElement).tagName
      if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return

      if (e.key === 'ArrowLeft' || e.key === 'ArrowDown') {
        e.preventDefault()
        setSelectedIdx(i => Math.max(0, i - 1))
      } else if (e.key === 'ArrowRight' || e.key === 'ArrowUp') {
        e.preventDefault()
        setSelectedIdx(i => Math.min(records.length - 1, i + 1))
      }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [records.length])

  // Reset selection when records change (new load)
  useEffect(() => {
    setSelectedIdx(0)
  }, [records.length])

  // Load-more triggers when approaching edges
  const handleSelect = useCallback((idx: number) => {
    setSelectedIdx(idx)
    if (idx <= 2 && onLoadBefore) onLoadBefore()
    if (idx >= records.length - 3 && onLoadAfter) onLoadAfter()
  }, [records.length, onLoadBefore, onLoadAfter])

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }}>
      {/* Toolbar */}
      <div style={{
        display: 'flex', alignItems: 'center', gap: 10, padding: '0.5rem 0.75rem',
        borderBottom: '1px solid #e2e8f0', flexWrap: 'wrap', background: '#fff',
      }}>
        {title && <span style={{ fontSize: 14, fontWeight: 600, color: '#2d3748' }}>{title}</span>}
        <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
          <button
            style={btnStyle}
            disabled={selectedIdx === 0}
            onClick={() => handleSelect(Math.max(0, selectedIdx - 1))}
            title="Previous message (←)"
          >‹ Prev</button>
          <span style={{ fontSize: 12, color: '#718096', minWidth: 80, textAlign: 'center' }}>
            {records.length > 0 ? `${selectedIdx + 1} / ${records.length}` : '—'}
          </span>
          <button
            style={btnStyle}
            disabled={selectedIdx >= records.length - 1}
            onClick={() => handleSelect(Math.min(records.length - 1, selectedIdx + 1))}
            title="Next message (→)"
          >Next ›</button>
        </div>
        <button style={btnStyle} onClick={() => setFitKey(k => k + 1)} title="Zoom to fit all messages">
          ⊡ Fit
        </button>
        <GroupBySelector
          mode={groupByMode}
          availableHeaderKeys={availableHeaderKeys}
          supportsMessageType={supportsMessageType}
          onChange={mode => {
            setGroupByMode(mode)
            // Clear secondary when primary changes to the same kind
            setGroupByMode2(g2 => g2?.kind === mode.kind ? null : g2)
          }}
        />
        <AndBySelector
          mode={groupByMode2}
          availableHeaderKeys={availableHeaderKeys}
          excludeKind={groupByMode.kind}
          supportsMessageType={supportsMessageType}
          onChange={setGroupByMode2}
        />
        {tooManyGroups && (
          <span style={{
            fontSize: 11, color: '#c05621', background: '#fffaf0',
            border: '1px solid #fed7aa', borderRadius: 4, padding: '2px 8px',
          }}>
            Too many groups — narrow your filter
          </span>
        )}
        {loading && (
          <span style={{ fontSize: 12, color: '#718096' }}>Loading…</span>
        )}
        {hasMore && !loading && (
          <span style={{ fontSize: 12, color: '#718096', fontStyle: 'italic' }}>
            More pages available — scrub to edges to load
          </span>
        )}
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 8, alignItems: 'center' }}>
          <span style={{ fontSize: 11, color: '#a0aec0' }}>← → arrow keys • scroll to zoom • drag to pan</span>
          {extraControls}
        </div>
      </div>

      {/* Upper panel: detail */}
      <div style={{
        flex: '1 1 0', minHeight: 0, overflow: 'auto',
        borderBottom: '1px solid #e2e8f0',
        background: '#fff',
      }}>
        <DetailPanel record={selectedRecord} />
      </div>

      {/* Lower panel: timeline */}
      <div style={{ flexShrink: 0, background: '#f7fafc', borderTop: '1px solid #e2e8f0' }}>
        {/* Legend */}
        <div style={{ padding: '0.5rem 0.75rem 0', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Legend colorKeys={colorKeys} />
          {records.length === 0 && (
            <span style={{ fontSize: 13, color: '#a0aec0', padding: '0.5rem 0' }}>No messages to display</span>
          )}
        </div>
        <TimelineCanvas
          records={effectiveRecords}
          selectedIdx={selectedIdx}
          colorKeys={colorKeys}
          onSelect={handleSelect}
          fitKey={fitKey}
        />
      </div>
    </div>
  )
}

// ─── Styles ───────────────────────────────────────────────────────────────────

const btnStyle: React.CSSProperties = {
  padding: '0.3rem 0.7rem',
  background: '#fff',
  color: '#4a5568',
  border: '1px solid #cbd5e0',
  borderRadius: 4,
  cursor: 'pointer',
  fontSize: 12,
  fontWeight: 500,
}

const selectStyle: React.CSSProperties = {
  padding: '0.25rem 0.5rem',
  background: '#fff',
  color: '#4a5568',
  border: '1px solid #cbd5e0',
  borderRadius: 4,
  fontSize: 12,
  cursor: 'pointer',
}

