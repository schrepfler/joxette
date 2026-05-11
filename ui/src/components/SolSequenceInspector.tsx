/**
 * SolSequenceInspector
 *
 * Shows a visual breakdown of how a SOL match carved up the sequence:
 *
 *   SEQ      ████████████████████████████████  32 events  100%
 *   PREFIX   ████████░░░░░░░░░░░░░░░░░░░░░░░░   8 events   25%
 *   Login    ░░░░░░░░█░░░░░░░░░░░░░░░░░░░░░░░   1 event     3%
 *   MATCHED  ░░░░░░░░████████░░░░░░░░░░░░░░░░   5 events   16%
 *   Buy      ░░░░░░░░░░░░░░░█░░░░░░░░░░░░░░░░   1 event     3%
 *   SUFFIX   ░░░░░░░░░░░░░░░░████████████████  19 events   59%
 *
 * Implicit tags (SEQ, MATCHED, PREFIX, SUFFIX) are shown first in muted style.
 * Named tags (produced by the match pattern) are shown in accent colour.
 */

import type { SolTagSpan } from '../api/client'

interface Props {
  tags: Record<string, SolTagSpan>
  sequenceLength: number
  /** Currently selected tag names. When non-empty the table is filtered to those spans. */
  selectedTags?: Set<string>
  /** Called when the user clicks a tag row to toggle its selection. */
  onTagToggle?: (name: string) => void
}

// ── Colour helpers ─────────────────────────────────────────────────────────────

function djb2(s: string): number {
  let h = 5381
  for (let i = 0; i < s.length; i++) h = (h * 33) ^ s.charCodeAt(i)
  return Math.abs(h)
}

const IMPLICIT = new Set(['SEQ', 'MATCHED', 'PREFIX', 'SUFFIX'])

function tagColour(name: string): string {
  if (name === 'MATCHED') return 'var(--accent)'
  if (name === 'PREFIX')  return 'hsl(200, 40%, 65%)'
  if (name === 'SUFFIX')  return 'hsl(200, 30%, 75%)'
  if (name === 'SEQ')     return 'hsl(0, 0%, 60%)'
  const hue = (djb2(name) * 137.508) % 360
  return `hsl(${hue.toFixed(0)}, 60%, 62%)`
}

function tagBg(name: string): string {
  if (name === 'MATCHED') return 'color-mix(in oklab, var(--accent) 20%, transparent)'
  return 'transparent'
}

// ── Component ──────────────────────────────────────────────────────────────────

export function SolSequenceInspector({ tags, sequenceLength, selectedTags, onTagToggle }: Props) {
  if (sequenceLength === 0 || Object.keys(tags).length === 0) return null
  const isFilterable = !!onTagToggle
  const hasSelection = selectedTags && selectedTags.size > 0

  // Sort: implicit tags first (SEQ → PREFIX → MATCHED → SUFFIX), then named tags
  const IMPLICIT_ORDER = ['SEQ', 'PREFIX', 'MATCHED', 'SUFFIX']
  const sorted = Object.entries(tags).sort(([a], [b]) => {
    const ai = IMPLICIT_ORDER.indexOf(a)
    const bi = IMPLICIT_ORDER.indexOf(b)
    if (ai >= 0 && bi >= 0) return ai - bi
    if (ai >= 0) return -1
    if (bi >= 0) return  1
    return a.localeCompare(b)
  })

  return (
    <div
      style={{
        border: '1px solid var(--rule)',
        borderRadius: 'var(--radius-sm)',
        overflow: 'hidden',
        fontSize: 'var(--type-body-sm-size)',
      }}
    >
      {/* Header */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '100px 1fr 60px 50px',
          gap: '0 8px',
          alignItems: 'center',
          padding: '6px 12px',
          background: 'var(--surface-raised)',
          borderBottom: '1px solid var(--rule)',
          fontSize: 'var(--type-micro-size)',
          fontWeight: 700,
          letterSpacing: 'var(--type-micro-tracking)',
          textTransform: 'uppercase',
          color: 'var(--ink-tertiary)',
        }}
      >
        <span>Tag</span>
        <span>Coverage</span>
        <span style={{ textAlign: 'right' }}>Events</span>
        <span style={{ textAlign: 'right' }}>%</span>
      </div>

      {/* Filter hint */}
      {isFilterable && (
        <div style={{
          padding: '4px 12px',
          background: hasSelection ? 'color-mix(in oklab, var(--accent) 8%, transparent)' : 'transparent',
          borderBottom: '1px solid var(--rule)',
          fontSize: 'var(--type-caption-size)',
          color: 'var(--ink-tertiary)',
          display: 'flex', alignItems: 'center', gap: 8,
        }}>
          {hasSelection
            ? <>
                <span>Filtering to: {[...selectedTags!].join(', ')}</span>
                <button
                  onClick={() => [...selectedTags!].forEach(t => onTagToggle!(t))}
                  style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--ink-tertiary)', fontSize: 'var(--type-caption-size)', padding: '0 4px' }}
                >
                  ✕ clear
                </button>
              </>
            : <span>Click a tag row to filter the event table</span>
          }
        </div>
      )}

      {/* Rows */}
      {sorted.map(([name, span]) => {
        const len   = span.to - span.from
        const pct   = sequenceLength > 0 ? (len / sequenceLength) * 100 : 0
        const isImplicit = IMPLICIT.has(name)
        const colour = tagColour(name)
        const isSelected = selectedTags?.has(name) ?? false

        return (
          <div
            key={name}
            onClick={() => onTagToggle?.(name)}
            style={{
              display: 'grid',
              gridTemplateColumns: '100px 1fr 60px 50px',
              gap: '0 8px',
              alignItems: 'center',
              padding: '7px 12px',
              borderBottom: '1px solid var(--rule)',
              background: isSelected
                ? 'color-mix(in oklab, var(--accent) 12%, transparent)'
                : tagBg(name),
              cursor: isFilterable ? 'pointer' : 'default',
              outline: isSelected ? `2px solid color-mix(in oklab, var(--accent) 50%, transparent)` : 'none',
              outlineOffset: '-2px',
            }}
          >
            {/* Tag name badge */}
            <span
              style={{
                fontFamily: 'var(--font-mono)',
                fontSize: 'var(--type-caption-size)',
                fontWeight: isImplicit ? 400 : 600,
                color: colour,
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}
              title={name}
            >
              {name}
            </span>

            {/* Coverage bar */}
            <div
              style={{
                height: 10,
                background: 'var(--rule)',
                borderRadius: 'var(--radius-xs)',
                overflow: 'hidden',
                position: 'relative',
              }}
            >
              {/* Left padding (before tag.from) */}
              {span.from > 0 && (
                <div
                  style={{
                    position: 'absolute', left: 0, top: 0, bottom: 0,
                    width: `${(span.from / sequenceLength) * 100}%`,
                    background: 'transparent',
                  }}
                />
              )}
              {/* Filled portion */}
              <div
                style={{
                  position: 'absolute',
                  left: `${(span.from / sequenceLength) * 100}%`,
                  top: 0, bottom: 0,
                  width: `${(len / sequenceLength) * 100}%`,
                  background: colour,
                  borderRadius: 'var(--radius-xs)',
                  minWidth: len > 0 ? 2 : 0,
                  opacity: isImplicit ? 0.55 : 0.85,
                }}
              />
            </div>

            {/* Event count */}
            <span
              style={{
                textAlign: 'right',
                fontFamily: 'var(--font-mono)',
                fontSize: 'var(--type-caption-size)',
                color: 'var(--ink-secondary)',
                fontVariantNumeric: 'tabular-nums',
              }}
            >
              {len}
            </span>

            {/* Percentage */}
            <span
              style={{
                textAlign: 'right',
                fontFamily: 'var(--font-mono)',
                fontSize: 'var(--type-caption-size)',
                color: 'var(--ink-tertiary)',
                fontVariantNumeric: 'tabular-nums',
              }}
            >
              {pct.toFixed(0)}%
            </span>
          </div>
        )
      })}

      {/* Footer: total */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '100px 1fr 60px 50px',
          gap: '0 8px',
          alignItems: 'center',
          padding: '6px 12px',
          background: 'var(--surface-raised)',
          fontSize: 'var(--type-caption-size)',
          color: 'var(--ink-tertiary)',
        }}
      >
        <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--ink-secondary)' }}>total</span>
        <span />
        <span style={{ textAlign: 'right', fontFamily: 'var(--font-mono)', fontVariantNumeric: 'tabular-nums', color: 'var(--ink-secondary)' }}>
          {sequenceLength}
        </span>
        <span style={{ textAlign: 'right', fontFamily: 'var(--font-mono)' }}>100%</span>
      </div>
    </div>
  )
}
