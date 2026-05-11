import { useMemo, useRef, useState } from 'react'
import type { EntityRecord } from '../api/client'

/**
 * SequenceBarcodeView
 *
 * Horizontal scrollable barcode chart for one or more entity sequences.
 * Each row = one entity. Each rectangle = one event, width ∝ time gap
 * to the next event (or a fixed width in "play number" mode).
 *
 * Inspired by the Observable NFL Barcode Chart analysis (docs/observable-sequences-reference.md).
 *
 * Colour modes:
 *   'type'    — colour by messageType (stable hash → hue)
 *   'tag'     — colour each segment by its SOL tag name; grey if untagged
 *   'numeric' — diverging red→white→green scale based on a numeric field
 *               extracted from the event value payload
 */

import type { SolTagSpan } from '../api/client'

export type BarcodeXMode = 'time' | 'index'
export type BarcodeColorMode = 'type' | 'tag' | 'numeric'

export interface BarcodeRow {
  entityId: string
  records: EntityRecord[]
  /**
   * Per-index tag name from a SOL match result (for 'tag' colour mode).
   * Built from SolMatchResponse.tags via buildTagMap().
   */
  tagMap?: Map<number, string>
  /**
   * Per-index extracted numeric value (for 'numeric' colour mode).
   * Built from extractNumeric() in the parent panel.
   */
  numericValues?: Map<number, number>
}

// ── SOL tag colour palette (mirrors SolSequenceInspector) ─────────────────────

const IMPLICIT_PRIORITY = ['SEQ', 'PREFIX', 'MATCHED', 'SUFFIX']

export function tagColor(name: string): string {
  if (name === 'MATCHED') return 'var(--accent)'
  if (name === 'PREFIX')  return 'hsl(200, 40%, 65%)'
  if (name === 'SUFFIX')  return 'hsl(200, 30%, 75%)'
  if (name === 'SEQ')     return 'hsl(0, 0%, 78%)'
  const hue = (djb2(name) * 137.508) % 360
  return `hsl(${hue.toFixed(0)}, 60%, 62%)`
}

/**
 * Builds a Map<eventIndex, tagName> from a SolMatchResponse.tags record.
 * Named tags overwrite implicit ones; among implicit, later in priority wins.
 */
export function buildTagMap(tags: Record<string, SolTagSpan>): Map<number, string> {
  const map = new Map<number, string>()
  // Sort: implicit first (lowest priority) → named last (highest priority)
  const sorted = Object.entries(tags).sort(([a], [b]) => {
    const ai = IMPLICIT_PRIORITY.indexOf(a)
    const bi = IMPLICIT_PRIORITY.indexOf(b)
    if (ai >= 0 && bi >= 0) return ai - bi
    if (ai >= 0) return -1
    if (bi >= 0) return 1
    return 0
  })
  for (const [name, span] of sorted) {
    for (let i = span.from; i < span.to; i++) map.set(i, name)
  }
  return map
}

// ── Numeric colour helpers ─────────────────────────────────────────────────────

/** Decode a base64url event value and extract a numeric field by dot-path (e.g. "amount" or "order.total"). */
export function extractNumeric(valueB64: string | null, path: string): number | null {
  if (!valueB64) return null
  try {
    const bytes = atob(valueB64.replace(/-/g, '+').replace(/_/g, '/'))
    const obj = JSON.parse(bytes) as Record<string, unknown>
    const parts = path.split('.')
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let cur: any = obj
    for (const p of parts) {
      if (cur == null || typeof cur !== 'object') return null
      cur = (cur as Record<string, unknown>)[p]
    }
    return typeof cur === 'number' ? cur : null
  } catch {
    return null
  }
}

/**
 * Diverging red→white→green scale.
 * t=0 → deep red, t=0.5 → white, t=1 → deep green.
 */
export function divergingColor(t: number): string {
  const clamped = Math.max(0, Math.min(1, t))
  if (clamped < 0.5) {
    const u = clamped / 0.5           // 0 → 1 as we go red → white
    const l = Math.round(40 + u * 45) // 40% → 85%
    const s = Math.round(80 - u * 65) // 80% → 15%
    return `hsl(0, ${s}%, ${l}%)`
  } else {
    const u = (clamped - 0.5) / 0.5   // 0 → 1 as we go white → green
    const l = Math.round(85 - u * 45) // 85% → 40%
    const s = Math.round(15 + u * 65) // 15% → 80%
    return `hsl(120, ${s}%, ${l}%)`
  }
}

/** Normalises all values in a row to [0,1] across the global domain [min, max]. */
export function buildNumericDomain(rows: BarcodeRow[]): [number, number] {
  let min = Infinity, max = -Infinity
  for (const row of rows) {
    if (!row.numericValues) continue
    for (const v of row.numericValues.values()) {
      if (v < min) min = v
      if (v > max) max = v
    }
  }
  return [min === Infinity ? 0 : min, max === -Infinity ? 1 : max]
}

interface Props {
  rows: BarcodeRow[]
  xMode?: BarcodeXMode
  colorMode?: BarcodeColorMode
  /** Cell height per row in px */
  cellHeight?: number
  /** Fixed width per event cell in 'index' mode */
  cellWidth?: number
  onEventClick?: (rowId: string, record: EntityRecord, index: number) => void
}

// ── Colour helpers ─────────────────────────────────────────────────────────────

function djb2(s: string): number {
  let h = 5381
  for (let i = 0; i < s.length; i++) h = (h * 33) ^ s.charCodeAt(i)
  return Math.abs(h)
}

function typeColor(name: string | null): string {
  if (!name) return 'hsl(0,0%,85%)'
  const hue = (djb2(name) * 137.508) % 360
  return `hsl(${hue.toFixed(0)}, 55%, 72%)`
}

// ── X-axis scale ───────────────────────────────────────────────────────────────

function buildTimeScale(rows: BarcodeRow[], totalWidth: number) {
  let minTs = Infinity, maxTs = -Infinity
  for (const row of rows) {
    for (const r of row.records) {
      const t = new Date(r.timestamp).getTime()
      if (t < minTs) minTs = t
      if (t > maxTs) maxTs = t
    }
  }
  const span = maxTs - minTs || 1
  return (ts: number) => ((ts - minTs) / span) * totalWidth
}

// ── Component ──────────────────────────────────────────────────────────────────

const LABEL_WIDTH = 120
const PADDING = 1
const MIN_RECT_WIDTH = 4
const SCROLL_WIDTH = 1200

export function SequenceBarcodeView({
  rows,
  xMode = 'time',
  colorMode = 'type',
  cellHeight = 20,
  cellWidth = 14,
  onEventClick,
}: Props) {
  const [tooltip, setTooltip] = useState<{ x: number; y: number; text: string } | null>(null)
  const scrollRef = useRef<HTMLDivElement>(null)

  const timeScale = useMemo(
    () => xMode === 'time' ? buildTimeScale(rows, SCROLL_WIDTH) : null,
    [rows, xMode],
  )

  const [numMin, numMax] = useMemo(
    () => colorMode === 'numeric' ? buildNumericDomain(rows) : [0, 1],
    [rows, colorMode],
  )

  const totalHeight = rows.length * (cellHeight + PADDING * 2) + 24 // 24 for x-axis

  function getRectX(row: BarcodeRow, idx: number): number {
    if (xMode === 'index') return idx * cellWidth
    const ts = new Date(row.records[idx].timestamp).getTime()
    return timeScale!(ts)
  }

  function getRectWidth(row: BarcodeRow, idx: number): number {
    if (xMode === 'index') return cellWidth - 1
    const ts = new Date(row.records[idx].timestamp).getTime()
    const nextTs = idx < row.records.length - 1
      ? new Date(row.records[idx + 1].timestamp).getTime()
      : ts + 60_000 // default 1 min for last event
    const w = timeScale!(nextTs) - timeScale!(ts) - 1
    return Math.max(w, MIN_RECT_WIDTH)
  }

  function getFill(row: BarcodeRow, idx: number, record: EntityRecord): string {
    if (colorMode === 'tag') {
      if (!row.tagMap || row.tagMap.size === 0) return 'hsl(0,0%,88%)'
      const name = row.tagMap.get(idx)
      return name ? tagColor(name) : 'hsl(0,0%,88%)'
    }
    if (colorMode === 'numeric') {
      const v = row.numericValues?.get(idx)
      if (v == null) return 'hsl(0,0%,88%)'
      const range = numMax - numMin || 1
      return divergingColor((v - numMin) / range)
    }
    return typeColor(record.messageType)
  }

  function getNumericLabel(row: BarcodeRow, idx: number): string {
    if (colorMode !== 'numeric') return ''
    const v = row.numericValues?.get(idx)
    return v != null ? `\nvalue: ${v}` : '\nvalue: —'
  }

  return (
    <div style={{ display: 'flex', overflow: 'hidden', position: 'relative', fontFamily: 'var(--font-body)' }}>
      {/* Fixed y-axis labels */}
      <div
        style={{
          flexShrink: 0, width: LABEL_WIDTH,
          position: 'sticky', left: 0, zIndex: 2,
          background: 'var(--surface-paper)',
          borderRight: '1px solid var(--rule)',
        }}
      >
        {/* spacer for x-axis row */}
        <div style={{ height: 24 }} />
        {rows.map(row => (
          <div
            key={row.entityId}
            style={{
              height: cellHeight + PADDING * 2,
              display: 'flex', alignItems: 'center',
              padding: '0 8px',
              fontSize: 'var(--type-caption-size)',
              color: 'var(--ink-secondary)',
              fontFamily: 'var(--font-mono)',
              overflow: 'hidden', whiteSpace: 'nowrap', textOverflow: 'ellipsis',
              borderBottom: '1px solid var(--rule)',
              cursor: 'default',
            }}
            title={row.entityId}
          >
            {row.entityId.length > 14 ? row.entityId.slice(0, 14) + '…' : row.entityId}
          </div>
        ))}
      </div>

      {/* Scrollable SVG area */}
      <div ref={scrollRef} style={{ overflowX: 'auto', flex: 1, WebkitOverflowScrolling: 'touch' }}>
        <svg
          width={SCROLL_WIDTH}
          height={totalHeight}
          style={{ display: 'block' }}
          onMouseLeave={() => setTooltip(null)}
        >
          {/* x-axis tick marks (time mode: time labels; index mode: event numbers) */}
          <g transform="translate(0,0)">
            {xMode === 'index' ? (
              // tick every 10 events
              Array.from({ length: Math.ceil(SCROLL_WIDTH / (cellWidth * 10)) }, (_, i) => {
                const x = i * cellWidth * 10
                return (
                  <g key={i} transform={`translate(${x},0)`}>
                    <line x1={0} y1={18} x2={0} y2={24} stroke="var(--rule-strong)" />
                    <text x={2} y={13} fontSize={9} fill="var(--ink-tertiary)">{i * 10}</text>
                  </g>
                )
              })
            ) : null}
          </g>

          {/* Rows */}
          {rows.map((row, ri) => {
            const rowY = 24 + ri * (cellHeight + PADDING * 2) + PADDING
            return (
              <g key={row.entityId} transform={`translate(0,${rowY})`}>
                {row.records.map((rec, idx) => {
                  const x = getRectX(row, idx)
                  const w = getRectWidth(row, idx)
                  const fill = getFill(row, idx, rec)
                  return (
                    <rect
                      key={`${rec.partition}-${rec.offset}`}
                      x={x} y={0} width={w} height={cellHeight}
                      fill={fill}
                      stroke="none"
                      strokeWidth={0}
                      rx={1}
                      style={{ cursor: 'pointer' }}
                      onMouseEnter={e => {
                        const svgRect = (e.target as SVGElement).closest('svg')!.getBoundingClientRect()
                        setTooltip({
                          x: e.clientX - svgRect.left + 8,
                          y: e.clientY - svgRect.top - 24,
                          text: [
                            rec.messageType ?? '(no type)',
                            rec.timestamp.slice(0, 19).replace('T', ' '),
                            `offset ${rec.offset}`,
                          ].join('\n') + getNumericLabel(row, idx),
                        })
                      }}
                      onMouseLeave={() => setTooltip(null)}
                      onClick={() => onEventClick?.(row.entityId, rec, idx)}
                    />
                  )
                })}
                {/* row border */}
                <line x1={0} y1={cellHeight + PADDING} x2={SCROLL_WIDTH} y2={cellHeight + PADDING} stroke="var(--rule)" strokeWidth={0.5} />
              </g>
            )
          })}
        </svg>

        {/* Tooltip */}
        {tooltip && (
          <div
            style={{
              position: 'absolute',
              left: tooltip.x + LABEL_WIDTH,
              top: tooltip.y,
              background: 'var(--surface-paper)',
              border: '1px solid var(--rule)',
              borderRadius: 'var(--radius-xs)',
              boxShadow: 'var(--shadow-md)',
              padding: '6px 10px',
              fontSize: 'var(--type-caption-size)',
              fontFamily: 'var(--font-mono)',
              color: 'var(--ink-primary)',
              pointerEvents: 'none',
              whiteSpace: 'pre',
              zIndex: 10,
            }}
          >
            {tooltip.text}
          </div>
        )}
      </div>
    </div>
  )
}

// ── Legends ────────────────────────────────────────────────────────────────────

/** Gradient legend for numeric colour mode with min/max labels. */
export function NumericLegend({ min, max, field }: { min: number; max: number; field: string }) {
  const stops = Array.from({ length: 9 }, (_, i) => {
    const t = i / 8
    return `${divergingColor(t)} ${(t * 100).toFixed(0)}%`
  }).join(', ')
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '6px 0' }}>
      <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)', fontFamily: 'var(--font-mono)' }}>{field}</span>
      <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)', fontVariantNumeric: 'tabular-nums' }}>{min}</span>
      <div style={{ width: 120, height: 12, borderRadius: 'var(--radius-xs)', background: `linear-gradient(to right, ${stops})`, flexShrink: 0 }} />
      <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)', fontVariantNumeric: 'tabular-nums' }}>{max}</span>
    </div>
  )
}

export function BarcodeLegend({ messageTypes }: { messageTypes: string[] }) {
  if (messageTypes.length === 0) return null
  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px 12px', padding: '8px 0' }}>
      {messageTypes.map(t => (
        <div key={t} style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
          <div style={{ width: 12, height: 12, borderRadius: 2, background: typeColor(t), flexShrink: 0 }} />
          <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-secondary)', fontFamily: 'var(--font-mono)' }}>{t}</span>
        </div>
      ))}
    </div>
  )
}
