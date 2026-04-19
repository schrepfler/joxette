import { useState } from 'react'
import type { MessagePattern, Quantifier, FragmentIfClause, Predicate } from '#/transforms/types'
import { PredicateBuilder } from './PredicateBuilder'

interface Props {
  value: MessagePattern
  onChange: (p: MessagePattern) => void
  label?: string
  /** Show an optional if-clause section (min/max duration) */
  showIfClause?: boolean
  ifClause?: FragmentIfClause
  onIfClauseChange?: (c: FragmentIfClause | undefined) => void
}

function quantifierKey(q: Quantifier): string {
  if (q === 'first' || q === 'last' || q === 'any') return q
  if (typeof q === 'object' && 'nth' in q) return 'nth'
  return 'first_after'
}

function defaultQuantifier(key: string, current: Quantifier, afterPattern: MessagePattern): Quantifier {
  if (key === 'first' || key === 'last' || key === 'any') return key
  if (key === 'nth') return { nth: typeof current === 'object' && 'nth' in current ? current.nth : 1 }
  return { first_after: afterPattern }
}

const defaultPattern = (): MessagePattern => ({
  predicate: { field: '', operator: 'EQ' },
  quantifier: 'first',
})

export function PatternBuilder({ value, onChange, label, showIfClause, ifClause, onIfClauseChange }: Props) {
  const [ifOpen, setIfOpen] = useState(false)

  const qKey = quantifierKey(value.quantifier)

  function setQuantifierKey(key: string) {
    const afterDefault = typeof value.quantifier === 'object' && 'first_after' in value.quantifier
      ? value.quantifier.first_after
      : defaultPattern()
    onChange({ ...value, quantifier: defaultQuantifier(key, value.quantifier, afterDefault) })
  }

  const isNth = qKey === 'nth'
  const isFirstAfter = qKey === 'first_after'

  return (
    <div style={containerStyle}>
      {label && <div style={labelStyle}>{label}</div>}

      {/* Predicate */}
      <div style={{ marginBottom: '0.5rem' }}>
        <div style={sectionLabelStyle}>Condition</div>
        <PredicateBuilder
          value={value.predicate}
          onChange={(p: Predicate | null) => onChange({ ...value, predicate: p ?? { field: '', operator: 'EQ' } })}
        />
      </div>

      {/* Quantifier */}
      <div style={{ marginBottom: isFirstAfter ? '0.5rem' : 0 }}>
        <div style={sectionLabelStyle}>Occurrence</div>
        <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap', alignItems: 'center' }}>
          {QUANTIFIER_OPTIONS.map(opt => (
            <button
              key={opt.value}
              onClick={() => setQuantifierKey(opt.value)}
              style={{
                ...qBtnStyle,
                background: qKey === opt.value ? '#3182ce' : '#fff',
                color: qKey === opt.value ? '#fff' : '#4a5568',
              }}
            >
              {opt.label}
            </button>
          ))}
          {isNth && (
            <input
              type="number"
              min={1}
              value={typeof value.quantifier === 'object' && 'nth' in value.quantifier ? value.quantifier.nth : 1}
              onChange={e => onChange({ ...value, quantifier: { nth: Math.max(1, parseInt(e.target.value) || 1) } })}
              style={nthInputStyle}
              title="N-th occurrence (1-based)"
            />
          )}
        </div>
      </div>

      {/* first_after: nested PatternBuilder */}
      {isFirstAfter && (
        <div style={nestedAfterStyle}>
          <div style={sectionLabelStyle}>After pattern</div>
          <PatternBuilder
            value={
              typeof value.quantifier === 'object' && 'first_after' in value.quantifier
                ? value.quantifier.first_after
                : defaultPattern()
            }
            onChange={after => onChange({ ...value, quantifier: { first_after: after } })}
          />
        </div>
      )}

      {/* Optional if-clause */}
      {showIfClause && onIfClauseChange && (
        <div style={{ marginTop: '0.5rem' }}>
          <button
            style={ifToggleBtnStyle}
            onClick={() => setIfOpen(o => !o)}
          >
            {ifOpen ? '▲' : '▶'} Timing constraints {ifClause?.min_duration_ms || ifClause?.max_duration_ms ? '●' : ''}
          </button>
          {ifOpen && (
            <div style={ifClauseBoxStyle}>
              <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center' }}>
                <label style={ifLabelStyle}>
                  Min duration
                  <input
                    type="number"
                    min={0}
                    value={ifClause?.min_duration_ms ?? ''}
                    placeholder="—"
                    onChange={e => {
                      const v = e.target.value === '' ? undefined : parseInt(e.target.value)
                      onIfClauseChange({ ...ifClause, min_duration_ms: v })
                    }}
                    style={durationInputStyle}
                  />
                  <span style={unitLabelStyle}>ms</span>
                </label>
                <label style={ifLabelStyle}>
                  Max duration
                  <input
                    type="number"
                    min={0}
                    value={ifClause?.max_duration_ms ?? ''}
                    placeholder="—"
                    onChange={e => {
                      const v = e.target.value === '' ? undefined : parseInt(e.target.value)
                      onIfClauseChange({ ...ifClause, max_duration_ms: v })
                    }}
                    style={durationInputStyle}
                  />
                  <span style={unitLabelStyle}>ms</span>
                </label>
                {(ifClause?.min_duration_ms || ifClause?.max_duration_ms) && (
                  <button style={clearIfBtnStyle} onClick={() => onIfClauseChange(undefined)}>
                    Clear
                  </button>
                )}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

const QUANTIFIER_OPTIONS = [
  { value: 'first', label: 'First' },
  { value: 'last', label: 'Last' },
  { value: 'any', label: 'Any' },
  { value: 'nth', label: 'Nth' },
  { value: 'first_after', label: 'First after…' },
]

const containerStyle: React.CSSProperties = {
  border: '1px solid #e2e8f0',
  borderRadius: 6,
  padding: '0.5rem 0.65rem',
  background: '#fff',
}
const labelStyle: React.CSSProperties = {
  fontSize: 12,
  fontWeight: 700,
  color: '#2d3748',
  marginBottom: '0.35rem',
}
const sectionLabelStyle: React.CSSProperties = {
  fontSize: 11,
  fontWeight: 700,
  color: '#718096',
  textTransform: 'uppercase',
  letterSpacing: '0.04em',
  marginBottom: '0.3rem',
}
const qBtnStyle: React.CSSProperties = {
  padding: '2px 8px',
  fontSize: 11,
  fontWeight: 600,
  border: '1px solid #cbd5e0',
  borderRadius: 3,
  cursor: 'pointer',
}
const nthInputStyle: React.CSSProperties = {
  width: 52,
  padding: '2px 5px',
  fontSize: 12,
  border: '1px solid #cbd5e0',
  borderRadius: 4,
  boxSizing: 'border-box',
}
const nestedAfterStyle: React.CSSProperties = {
  marginTop: '0.5rem',
  borderLeft: '3px solid #bee3f8',
  paddingLeft: '0.65rem',
}
const ifToggleBtnStyle: React.CSSProperties = {
  background: 'none',
  border: 'none',
  cursor: 'pointer',
  fontSize: 11,
  color: '#718096',
  padding: 0,
  fontWeight: 600,
}
const ifClauseBoxStyle: React.CSSProperties = {
  marginTop: '0.35rem',
  padding: '0.4rem 0.5rem',
  background: '#f7fafc',
  borderRadius: 4,
  border: '1px solid #e2e8f0',
}
const ifLabelStyle: React.CSSProperties = {
  fontSize: 12,
  color: '#4a5568',
  display: 'flex',
  alignItems: 'center',
  gap: 4,
}
const durationInputStyle: React.CSSProperties = {
  width: 80,
  padding: '2px 5px',
  fontSize: 12,
  border: '1px solid #cbd5e0',
  borderRadius: 4,
  boxSizing: 'border-box',
}
const unitLabelStyle: React.CSSProperties = {
  fontSize: 11,
  color: '#a0aec0',
}
const clearIfBtnStyle: React.CSSProperties = {
  padding: '2px 7px',
  fontSize: 11,
  background: '#fff',
  color: '#e53e3e',
  border: '1px solid #fed7d7',
  borderRadius: 3,
  cursor: 'pointer',
}
