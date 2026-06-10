/**
 * SolModelPanel — the "Sequence model" summary for a SOL batch result.
 *
 * Vertical flow in pattern order: Prefix → tag → (unnamed gap) → … → Suffix,
 * each row showing the share of scanned sequences that hit that region,
 * coloured to match the editor tokens and the examples-pane spans.
 */

import type { SolModelRow } from '../api/client'
import { SOL_NEUTRAL, type SolTagColor } from './sol-colors'

interface Props {
  model: SolModelRow[]
  totalSequences: number
  tagColors: Record<string, SolTagColor>
}

function RowChip({ row, color }: { row: SolModelRow; color: SolTagColor }) {
  if (row.gap) {
    return (
      <span
        style={{
          fontStyle: 'italic',
          color: 'var(--ink-tertiary)',
          fontSize: 'var(--type-caption-size)',
          border: `1px dashed ${SOL_NEUTRAL.strong}`,
          borderRadius: 'var(--radius-xs)',
          padding: '2px 8px',
          background: 'transparent',
        }}
      >
        Unnamed Tag
      </span>
    )
  }
  const neutral = row.label === 'Prefix' || row.label === 'Suffix'
  return (
    <span
      style={{
        fontFamily: neutral ? 'var(--font-body)' : 'var(--font-mono)',
        fontSize: 'var(--type-caption-size)',
        fontWeight: 600,
        color: '#fff',
        background: color.strong,
        borderRadius: 'var(--radius-xs)',
        padding: '2px 9px',
        whiteSpace: 'nowrap',
        maxWidth: 160,
        overflow: 'hidden',
        textOverflow: 'ellipsis',
      }}
      title={row.label ?? undefined}
    >
      {row.label}
    </span>
  )
}

export function SolModelPanel({ model, totalSequences, tagColors }: Props) {
  if (model.length === 0 || totalSequences === 0) return null

  return (
    <div style={{ borderTop: '1px solid var(--rule)', marginTop: 12, paddingTop: 10 }}>
      <div
        style={{
          fontSize: 'var(--type-micro-size)',
          fontWeight: 700,
          textTransform: 'uppercase',
          letterSpacing: 'var(--type-micro-tracking)',
          color: 'var(--ink-tertiary)',
          marginBottom: 8,
        }}
      >
        Sequence model
      </div>

      <div style={{ display: 'flex', flexDirection: 'column' }}>
        {model.map((row, i) => {
          const rate = row.count / totalSequences
          const pct = Math.round(rate * 100)
          const colorKey = row.label === 'Prefix' ? 'PREFIX' : row.label === 'Suffix' ? 'SUFFIX' : row.label
          const color = row.gap || !colorKey ? SOL_NEUTRAL : (tagColors[colorKey] ?? SOL_NEUTRAL)
          return (
            <div key={i}>
              {i > 0 && (
                <div style={{ color: 'var(--ink-tertiary)', fontSize: 10, lineHeight: '10px', paddingLeft: 26 }}>⌄</div>
              )}
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '3px 0' }}>
                <div style={{ width: 150, flexShrink: 0, display: 'flex' }}>
                  <RowChip row={row} color={color} />
                </div>
                {/* Match-rate bar */}
                <div
                  style={{
                    width: 72,
                    height: 6,
                    borderRadius: 3,
                    background: 'var(--surface-raised)',
                    overflow: 'hidden',
                    flexShrink: 0,
                  }}
                >
                  <div
                    style={{
                      width: `${pct}%`,
                      height: '100%',
                      background: color.strong,
                      opacity: 0.4 + rate * 0.6,
                      borderRadius: 3,
                      transition: 'width 300ms ease',
                    }}
                  />
                </div>
                <span
                  style={{
                    fontSize: 'var(--type-caption-size)',
                    color: 'var(--ink-secondary)',
                    fontVariantNumeric: 'tabular-nums',
                    width: 42,
                    textAlign: 'right',
                  }}
                >
                  ~{pct}%
                </span>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
