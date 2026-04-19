import { useState, useMemo, useRef, useEffect } from 'react'
import type { CassetteRecord } from '#/api/client'
import type {
  FragmentDefinition,
  GapTransformStep,
  GapSelector,
  GapOperation,
  MessagePattern,
  Quantifier,
  PredicateLeaf,
} from '#/transforms/types'

// ─── Public types ─────────────────────────────────────────────────────────────

export interface GapInfo {
  afterMsg: CassetteRecord
  beforeMsg: CassetteRecord
  durationMs: number
  withinFragments: string[]
}

export interface GapTimelineProps {
  messages: CassetteRecord[]
  fragments: FragmentDefinition[]
  resolvedFragments: Record<string, { startAt: string; endAt: string; eventCount: number }>
  /** Called when user clicks "Add to pipeline" inside the gap panel */
  onAddStep: (step: GapTransformStep) => void
  existingGapSteps: GapTransformStep[]
}

// ─── Layout constants ─────────────────────────────────────────────────────────

const AXIS_Y = 50
const MARKER_R = 5
const LEFT_PAD = 16
const RIGHT_PAD = 16
const GAP_RECT_TOP = AXIS_Y - 11
const GAP_RECT_H = 22
const MSG_TRACK_H = 90
const BAND_H = 22
const BAND_GAP = 4
const BAND_Y0 = MSG_TRACK_H + 2

// ─── Helpers ──────────────────────────────────────────────────────────────────

export function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`
  if (ms < 3_600_000) return `${(ms / 60_000).toFixed(1)}min`
  return `${(ms / 3_600_000).toFixed(1)}h`
}

function evalPath(msg: CassetteRecord, field: string): string | null {
  if (field === '$.key') return msg.key
  if (field.startsWith('$.value.')) {
    try {
      const parsed = JSON.parse(msg.value ?? 'null') as unknown
      if (!parsed || typeof parsed !== 'object') return null
      const parts = field.slice('$.value.'.length).split('.')
      let node: unknown = parsed
      for (const p of parts) {
        if (node == null || typeof node !== 'object') return null
        node = (node as Record<string, unknown>)[p]
      }
      return node == null ? null : String(node)
    } catch { return null }
  }
  return null
}

function matchesPredicate(msg: CassetteRecord, pred: PredicateLeaf): boolean {
  const val = evalPath(msg, pred.field)
  const sv = String(pred.value ?? '')
  switch (pred.operator) {
    case 'IS_NULL': return val == null
    case 'IS_NOT_NULL': return val != null
    case 'EQ': return val === sv
    case 'NEQ': return val !== sv
    case 'CONTAINS': return val?.includes(sv) ?? false
    case 'MATCHES': try { return new RegExp(sv).test(val ?? '') } catch { return false }
    default: return false
  }
}

function findPatternMatch(messages: CassetteRecord[], pattern: MessagePattern): number {
  if ('match' in pattern.predicate) return -1
  const leaf = pattern.predicate as PredicateLeaf
  const hits: number[] = []
  messages.forEach((m, i) => { if (matchesPredicate(m, leaf)) hits.push(i) })
  if (hits.length === 0) return -1
  const { quantifier } = pattern
  if (quantifier === 'first' || quantifier === 'any') return hits[0]
  if (quantifier === 'last') return hits[hits.length - 1]
  if (typeof quantifier === 'object' && 'nth' in quantifier) return hits[quantifier.nth - 1] ?? -1
  return hits[0]
}

/** Client-side fragment resolution — approximates GapEvaluator for dry-run preview. */
export function resolveFragmentsClientSide(
  messages: CassetteRecord[],
  fragments: FragmentDefinition[],
): Record<string, { startAt: string; endAt: string; eventCount: number }> {
  const result: Record<string, { startAt: string; endAt: string; eventCount: number }> = {}
  for (const frag of fragments) {
    const fromIdx = findPatternMatch(messages, frag.from)
    if (fromIdx < 0) continue
    const toRelIdx = findPatternMatch(messages.slice(fromIdx + 1), frag.to)
    if (toRelIdx < 0) continue
    const toIdx = fromIdx + 1 + toRelIdx
    const startAt = messages[fromIdx].timestamp
    const endAt = messages[toIdx].timestamp
    const durationMs = new Date(endAt).getTime() - new Date(startAt).getTime()
    if (frag.if?.min_duration_ms != null && durationMs < frag.if.min_duration_ms) continue
    if (frag.if?.max_duration_ms != null && durationMs > frag.if.max_duration_ms) continue
    result[frag.name] = { startAt, endAt, eventCount: toIdx - fromIdx + 1 }
  }
  return result
}

function buildPatternForMessage(
  msg: CassetteRecord,
  allMessages: CassetteRecord[],
  idx: number,
): MessagePattern {
  let field = '$.key'
  let value: string = msg.key ?? ''
  try {
    const parsed = JSON.parse(msg.value ?? 'null') as unknown
    if (parsed && typeof parsed === 'object') {
      const v = parsed as Record<string, unknown>
      for (const k of ['type', 'eventType', 'event_type', 'messageType', 'kind', 'action']) {
        if (typeof v[k] === 'string') { field = `$.value.${k}`; value = v[k] as string; break }
      }
    }
  } catch { /* ignore */ }
  const predicate: PredicateLeaf = { field, operator: 'EQ', value }
  let count = 0
  for (let i = 0; i < idx; i++) {
    if (matchesPredicate(allMessages[i], predicate)) count++
  }
  const quantifier: Quantifier = count === 0 ? 'first' : { nth: count + 1 }
  return { predicate, quantifier }
}

function msgSummary(m: CassetteRecord): string {
  const key = m.key ? `key=${m.key}` : ''
  let type = ''
  try {
    const v = JSON.parse(m.value ?? 'null') as unknown
    if (v && typeof v === 'object') {
      const vv = v as Record<string, unknown>
      for (const k of ['type', 'eventType', 'event_type']) {
        if (typeof vv[k] === 'string') { type = `type=${vv[k] as string}`; break }
      }
    }
  } catch { /* */ }
  return [key, type].filter(Boolean).join(' · ') || m.timestamp
}

// ─── GapPanel ─────────────────────────────────────────────────────────────────

interface GapPanelProps {
  info: GapInfo
  gapIdx: number
  messages: CassetteRecord[]
  onAdd: (step: GapTransformStep) => void
  onClose: () => void
}

function GapPanel({ info, gapIdx, messages, onAdd, onClose }: GapPanelProps) {
  const [opType, setOpType] = useState<'cut' | 'hold' | 'trim' | 'pad' | 'scale'>('cut')
  const [holdMs, setHoldMs] = useState(500)
  const [trimByMs, setTrimByMs] = useState(200)
  const [useByFactor, setUseByFactor] = useState(false)
  const [trimFactor, setTrimFactor] = useState(0.5)
  const [padMs, setPadMs] = useState(500)
  const [scaleFactor, setScaleFactor] = useState(0.1)

  function buildOperation(): GapOperation {
    switch (opType) {
      case 'cut': return { op: 'cut' }
      case 'hold': return { op: 'hold', target_ms: holdMs }
      case 'trim': return useByFactor
        ? { op: 'trim', by_factor: trimFactor }
        : { op: 'trim', by_ms: trimByMs }
      case 'pad': return { op: 'pad', by_ms: padMs }
      case 'scale': return { op: 'scale', factor: scaleFactor }
    }
  }

  function buildSelector(): GapSelector {
    if (info.withinFragments.length > 0) {
      return { within_fragment: info.withinFragments[0] }
    }
    const afterIdx = messages.indexOf(info.afterMsg)
    const beforeIdx = messages.indexOf(info.beforeMsg)
    const after = afterIdx >= 0 ? buildPatternForMessage(info.afterMsg, messages, afterIdx) : undefined
    const before = beforeIdx >= 0 ? buildPatternForMessage(info.beforeMsg, messages, beforeIdx) : undefined
    return { after, before }
  }

  function handleAdd() {
    onAdd({ type: 'gap_transform', select: buildSelector(), operation: buildOperation() })
  }

  return (
    <div style={panelStyle}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.45rem' }}>
        <span style={panelTitleStyle}>Gap #{gapIdx + 1}</span>
        <button style={closeBtnStyle} onClick={onClose}>✕</button>
      </div>

      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginBottom: '0.4rem' }}>
        <span style={durationBadgeStyle}>{formatDuration(info.durationMs)}</span>
        {info.withinFragments.map(name => (
          <span key={name} style={fragBadgeStyle}>within: {name}</span>
        ))}
      </div>

      <div style={{ fontSize: 11, color: '#718096', marginBottom: '0.5rem', lineHeight: 1.5 }}>
        <div>After: <code style={codeStyle}>{msgSummary(info.afterMsg)}</code></div>
        <div>Before: <code style={codeStyle}>{msgSummary(info.beforeMsg)}</code></div>
      </div>

      <div style={{ marginBottom: '0.45rem' }}>
        <div style={sectionLabelStyle}>Operation</div>
        <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
          {(['cut', 'hold', 'trim', 'pad', 'scale'] as const).map(op => (
            <button
              key={op}
              style={opType === op ? opBtnActive : opBtn}
              onClick={() => setOpType(op)}
            >
              {op.charAt(0).toUpperCase() + op.slice(1)}
            </button>
          ))}
        </div>
      </div>

      {opType === 'hold' && (
        <div style={paramRow}>
          <label style={paramLabel}>Target (ms)</label>
          <input type="number" min={0} value={holdMs} onChange={e => setHoldMs(Number(e.target.value))} style={numInput} />
        </div>
      )}
      {opType === 'trim' && (
        <div style={{ marginBottom: '0.35rem' }}>
          <div style={{ display: 'flex', gap: 10, alignItems: 'center', marginBottom: '0.25rem' }}>
            <label style={paramLabel}>By</label>
            <label style={radioLabel}>
              <input type="radio" checked={!useByFactor} onChange={() => setUseByFactor(false)} /> ms
            </label>
            <label style={radioLabel}>
              <input type="radio" checked={useByFactor} onChange={() => setUseByFactor(true)} /> factor (0–1)
            </label>
          </div>
          {!useByFactor
            ? <input type="number" min={0} value={trimByMs} onChange={e => setTrimByMs(Number(e.target.value))} style={numInput} />
            : <input type="number" min={0} max={1} step={0.05} value={trimFactor} onChange={e => setTrimFactor(Number(e.target.value))} style={numInput} />
          }
        </div>
      )}
      {opType === 'pad' && (
        <div style={paramRow}>
          <label style={paramLabel}>Add (ms)</label>
          <input type="number" min={0} value={padMs} onChange={e => setPadMs(Number(e.target.value))} style={numInput} />
        </div>
      )}
      {opType === 'scale' && (
        <div style={paramRow}>
          <label style={paramLabel}>Factor</label>
          <input type="number" min={0} step={0.05} value={scaleFactor} onChange={e => setScaleFactor(Number(e.target.value))} style={numInput} />
        </div>
      )}

      <button style={addBtnStyle} onClick={handleAdd}>
        Add to pipeline
      </button>
    </div>
  )
}

// ─── GapTimeline ──────────────────────────────────────────────────────────────

export function GapTimeline({
  messages,
  fragments,
  resolvedFragments,
  onAddStep,
  existingGapSteps,
}: GapTimelineProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [svgWidth, setSvgWidth] = useState(760)
  const [hoveredGap, setHoveredGap] = useState<number | null>(null)
  const [selectedGap, setSelectedGap] = useState<number | null>(null)

  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    const ro = new ResizeObserver(entries => { setSvgWidth(entries[0].contentRect.width) })
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  const timestamps = useMemo(
    () => messages.map(m => new Date(m.timestamp).getTime()),
    [messages],
  )

  // Log1p scale: log10(elapsed+1) — 0ms stays at left edge, avoids -Infinity
  const logPos = useMemo(() => {
    if (timestamps.length === 0) return []
    const t0 = timestamps[0]
    const maxElapsed = timestamps[timestamps.length - 1] - t0
    const maxLog = Math.log10(maxElapsed + 1)
    const avail = svgWidth - LEFT_PAD - RIGHT_PAD
    return timestamps.map(ts => {
      const elapsed = ts - t0
      if (maxLog === 0) return LEFT_PAD + avail / 2
      return LEFT_PAD + (Math.log10(Math.max(0, elapsed) + 1) / maxLog) * avail
    })
  }, [timestamps, svgWidth])

  const gaps = useMemo<GapInfo[]>(() => {
    return messages.slice(0, -1).map((afterMsg, i) => {
      const beforeMsg = messages[i + 1]
      const durationMs = timestamps[i + 1] - timestamps[i]
      const withinFragments = fragments
        .filter(f => {
          const rf = resolvedFragments[f.name]
          if (!rf) return false
          const startMs = new Date(rf.startAt).getTime()
          const endMs = new Date(rf.endAt).getTime()
          return timestamps[i] >= startMs && timestamps[i + 1] <= endMs
        })
        .map(f => f.name)
      return { afterMsg, beforeMsg, durationMs, withinFragments }
    })
  }, [messages, timestamps, fragments, resolvedFragments])

  function isCovered(gapIdx: number): boolean {
    const gap = gaps[gapIdx]
    if (!gap) return false
    return existingGapSteps.some(s => {
      if (s.select.within_fragment) return gap.withinFragments.includes(s.select.within_fragment)
      if (s.select.min_duration_ms != null) return gap.durationMs >= s.select.min_duration_ms
      return false
    })
  }

  const bandData = useMemo(() => {
    const t0 = timestamps[0] ?? 0
    const maxElapsed = (timestamps[timestamps.length - 1] ?? t0) - t0
    const maxLog = Math.log10(maxElapsed + 1)
    const avail = svgWidth - LEFT_PAD - RIGHT_PAD
    const scale = (ts: number) => {
      if (maxLog === 0) return LEFT_PAD + avail / 2
      return LEFT_PAD + (Math.log10(Math.max(0, ts - t0) + 1) / maxLog) * avail
    }
    return fragments.map((frag, i) => {
      const rf = resolvedFragments[frag.name]
      const y = BAND_Y0 + i * (BAND_H + BAND_GAP)
      if (!rf) return { frag, y, x1: LEFT_PAD, x2: LEFT_PAD + 60, resolved: false, duration: 0, eventCount: 0 }
      const x1 = scale(new Date(rf.startAt).getTime())
      const x2 = scale(new Date(rf.endAt).getTime())
      const duration = new Date(rf.endAt).getTime() - new Date(rf.startAt).getTime()
      return { frag, y, x1, x2: Math.max(x1 + 4, x2), resolved: true, duration, eventCount: rf.eventCount }
    })
  }, [fragments, resolvedFragments, timestamps, svgWidth])

  const totalHeight = MSG_TRACK_H + (fragments.length > 0 ? fragments.length * (BAND_H + BAND_GAP) + 12 : 0)
  const showLabels = gaps.length <= 10

  function handleAddStep(step: GapTransformStep) {
    onAddStep(step)
    setSelectedGap(null)
  }

  if (messages.length === 0) {
    return (
      <div style={{ textAlign: 'center', padding: '1rem', fontSize: 13, color: '#a0aec0' }}>
        No messages to display
      </div>
    )
  }

  return (
    <div ref={containerRef} style={{ width: '100%' }}>
      <svg
        width="100%"
        height={totalHeight}
        style={{ display: 'block', overflow: 'visible', fontFamily: 'system-ui, sans-serif' }}
        aria-label="Log-scaled message timeline"
      >
        {/* Axis line */}
        <line
          x1={LEFT_PAD} y1={AXIS_Y + MARKER_R + 3}
          x2={svgWidth - RIGHT_PAD} y2={AXIS_Y + MARKER_R + 3}
          stroke="#cbd5e0" strokeWidth={1}
        />

        {/* Clickable gap segments */}
        {gaps.map((gap, i) => {
          const x1 = logPos[i] + MARKER_R + 1
          const x2 = logPos[i + 1] - MARKER_R - 1
          const w = Math.max(0, x2 - x1)
          if (w < 1) return null
          const covered = isCovered(i)
          const hovered = hoveredGap === i
          const selected = selectedGap === i
          const fragColor = gap.withinFragments.length > 0
            ? (fragments.find(f => f.name === gap.withinFragments[0])?.color ?? null)
            : null
          let fill = fragColor ? fragColor + '33' : '#e2e8f0'
          if (covered) fill = '#c6f6d5'
          if (hovered) fill = fragColor ? fragColor + '55' : '#ebf8ff'
          if (selected) fill = '#bee3f8'
          return (
            <g key={i}>
              <rect
                x={x1} y={GAP_RECT_TOP} width={w} height={GAP_RECT_H}
                fill={fill}
                stroke={selected ? '#3182ce' : covered ? '#38a169' : 'none'}
                strokeWidth={selected || covered ? 1.5 : 0}
                strokeDasharray={covered ? '3 2' : undefined}
                rx={2}
                style={{ cursor: 'pointer' }}
                onMouseEnter={() => setHoveredGap(i)}
                onMouseLeave={() => setHoveredGap(null)}
                onClick={() => setSelectedGap(prev => prev === i ? null : i)}
              />
              {showLabels && (
                <text
                  x={x1 + w / 2} y={AXIS_Y + 26}
                  textAnchor="middle"
                  fill={selected ? '#2b6cb0' : '#a0aec0'}
                  fontSize={10}
                  style={{ pointerEvents: 'none', userSelect: 'none' }}
                >
                  {formatDuration(gap.durationMs)}
                </text>
              )}
            </g>
          )
        })}

        {/* Hover tooltip */}
        {hoveredGap !== null && selectedGap !== hoveredGap && (() => {
          const i = hoveredGap
          const midX = Math.min(
            Math.max(LEFT_PAD + 36, (logPos[i] + logPos[i + 1]) / 2),
            svgWidth - RIGHT_PAD - 36,
          )
          return (
            <g style={{ pointerEvents: 'none' }}>
              <rect x={midX - 34} y={14} width={68} height={18} rx={3} fill="#1a202c" opacity={0.82} />
              <text x={midX} y={27} textAnchor="middle" fill="#fff" fontSize={11}>
                {formatDuration(gaps[i].durationMs)}
              </text>
            </g>
          )
        })()}

        {/* Message markers */}
        {messages.map((msg, i) => (
          <g key={i}>
            <circle
              cx={logPos[i]} cy={AXIS_Y}
              r={MARKER_R}
              fill="#3182ce" stroke="#fff" strokeWidth={1.5}
            >
              <title>{`#${i + 1} · ${msg.timestamp}\nKey: ${msg.key ?? '(null)'}`}</title>
            </circle>
            {messages.length <= 16 && (
              <text
                x={logPos[i]} y={AXIS_Y - MARKER_R - 4}
                textAnchor="middle" fill="#718096" fontSize={9}
                style={{ pointerEvents: 'none', userSelect: 'none' }}
              >
                {i + 1}
              </text>
            )}
          </g>
        ))}

        {/* Fragment band track */}
        {bandData.map(({ frag, y, x1, x2, resolved, duration, eventCount }) => {
          const bandWidth = resolved ? Math.max(4, x2 - x1) : svgWidth - LEFT_PAD - RIGHT_PAD
          const bandX = resolved ? x1 : LEFT_PAD
          return (
            <g key={frag.name}>
              <rect
                x={bandX} y={y}
                width={bandWidth} height={BAND_H}
                fill={frag.color}
                opacity={resolved ? 0.18 : 0.06}
                stroke={frag.color}
                strokeWidth={1}
                strokeDasharray={resolved ? undefined : '5 3'}
                rx={3}
              />
              <rect x={bandX} y={y + 2} width={3} height={BAND_H - 4}
                fill={frag.color} opacity={resolved ? 0.9 : 0.35} rx={1} />
              <text
                x={bandX + 9} y={y + BAND_H / 2 + 4}
                fill={resolved ? '#2d3748' : '#a0aec0'}
                fontSize={10} fontWeight={600}
                style={{ pointerEvents: 'none', userSelect: 'none' }}
              >
                {frag.label || frag.name}
                {resolved && ` (${formatDuration(duration)} · ${eventCount} events)`}
                {!resolved && ' — unresolved'}
              </text>
            </g>
          )
        })}
      </svg>

      {/* Gap click panel */}
      {selectedGap !== null && gaps[selectedGap] && (
        <GapPanel
          info={gaps[selectedGap]}
          gapIdx={selectedGap}
          messages={messages}
          onAdd={handleAddStep}
          onClose={() => setSelectedGap(null)}
        />
      )}

      {/* Legend */}
      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginTop: 6, fontSize: 11, color: '#a0aec0' }}>
        <span>x-axis: log₁₀ scale</span>
        <span style={{ color: '#38a169' }}>■ covered by existing step</span>
        {fragments.map(f => (
          <span key={f.name} style={{ color: f.color }}>■ {f.label || f.name}</span>
        ))}
        <span>Click a gap segment to configure</span>
      </div>
    </div>
  )
}

// ─── Styles ───────────────────────────────────────────────────────────────────

const panelStyle: React.CSSProperties = {
  border: '1px solid #bee3f8',
  borderRadius: 6,
  padding: '0.65rem',
  marginTop: '0.5rem',
  background: '#ebf8ff',
}

const panelTitleStyle: React.CSSProperties = {
  fontSize: 12,
  fontWeight: 700,
  color: '#2b6cb0',
  textTransform: 'uppercase',
  letterSpacing: '0.04em',
}

const closeBtnStyle: React.CSSProperties = {
  background: 'none',
  border: 'none',
  fontSize: 14,
  cursor: 'pointer',
  color: '#718096',
  padding: '0 2px',
  lineHeight: 1,
}

const durationBadgeStyle: React.CSSProperties = {
  padding: '1px 8px',
  borderRadius: 10,
  fontSize: 12,
  background: '#2b6cb0',
  color: '#fff',
  fontWeight: 700,
  fontFamily: 'monospace',
}

const fragBadgeStyle: React.CSSProperties = {
  padding: '1px 8px',
  borderRadius: 10,
  fontSize: 11,
  background: '#9ae6b4',
  color: '#22543d',
  fontWeight: 600,
}

const codeStyle: React.CSSProperties = {
  fontFamily: 'monospace',
  fontSize: 11,
  background: 'rgba(0,0,0,0.05)',
  borderRadius: 3,
  padding: '0 3px',
}

const sectionLabelStyle: React.CSSProperties = {
  fontSize: 11,
  fontWeight: 700,
  color: '#718096',
  textTransform: 'uppercase',
  letterSpacing: '0.04em',
  marginBottom: '0.3rem',
}

const opBtn: React.CSSProperties = {
  padding: '0.2rem 0.55rem',
  fontSize: 12,
  background: '#fff',
  color: '#4a5568',
  border: '1px solid #cbd5e0',
  borderRadius: 4,
  cursor: 'pointer',
}

const opBtnActive: React.CSSProperties = {
  ...opBtn,
  background: '#3182ce',
  color: '#fff',
  border: '1px solid #3182ce',
}

const paramRow: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: 8,
  marginBottom: '0.35rem',
}

const paramLabel: React.CSSProperties = {
  fontSize: 11,
  color: '#4a5568',
  minWidth: 70,
}

const radioLabel: React.CSSProperties = {
  fontSize: 11,
  color: '#4a5568',
  display: 'flex',
  alignItems: 'center',
  gap: 3,
  cursor: 'pointer',
}

const numInput: React.CSSProperties = {
  width: 80,
  padding: '0.2rem 0.35rem',
  border: '1px solid #cbd5e0',
  borderRadius: 4,
  fontSize: 12,
}

const addBtnStyle: React.CSSProperties = {
  marginTop: '0.4rem',
  padding: '0.35rem 0.85rem',
  background: '#3182ce',
  color: '#fff',
  border: 'none',
  borderRadius: 4,
  cursor: 'pointer',
  fontSize: 12,
  fontWeight: 600,
}
