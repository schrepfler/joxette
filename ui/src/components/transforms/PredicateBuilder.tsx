import { useEffect, useRef, useState } from 'react'
import type { Predicate, PredicateLeaf, PredicateAnd, PredicateOr, PredicateNot, FilterOperator } from '#/transforms/types'
import { isLeafPredicate } from '#/transforms/types'
import { fetchFieldSuggestions, type FieldContext } from '#/api/client'

const STATIC_FIELD_SUGGESTIONS = [
  '$.headers',
  '$.key',
  '$.offset',
  '$.partition',
  '$.timestamp',
  '$.topic',
  '$.value.id',
  '$.value.status',
  '$.value.type',
  '$.value.version',
]

const ENVELOPE_DESCRIPTIONS: Record<string, string> = {
  '$.headers': 'Kafka headers list',
  '$.key': 'Message key',
  '$.offset': 'Kafka partition offset',
  '$.partition': 'Kafka partition number',
  '$.timestamp': 'Kafka producer timestamp',
  '$.topic': 'Source topic name',
}

interface PredicateBuilderProps {
  value: Predicate | null | undefined
  onChange: (p: Predicate | null) => void
  /** When true, renders a '+ Add condition' button when value is null/undefined. */
  nullable?: boolean
  /** Nesting depth — controls border colour cycling. */
  depth?: number
  /** Renders an ✕ button that calls this when clicked. */
  onRemove?: () => void
  /** When present, fetch live field suggestions from the backend. */
  fieldContext?: FieldContext
}

const OPERATORS: { value: FilterOperator; label: string }[] = [
  { value: 'EQ', label: '= equals' },
  { value: 'NEQ', label: '≠ not equals' },
  { value: 'GT', label: '> greater than' },
  { value: 'GTE', label: '≥ greater or equal' },
  { value: 'LT', label: '< less than' },
  { value: 'LTE', label: '≤ less or equal' },
  { value: 'CONTAINS', label: 'contains' },
  { value: 'MATCHES', label: 'matches regex' },
  { value: 'IS_NULL', label: 'is null' },
  { value: 'IS_NOT_NULL', label: 'is not null' },
]

const BORDER_COLORS = ['#e2e8f0', '#bee3f8', '#c6f6d5', '#fefcbf', '#fed7d7']

export function PredicateBuilder({ value, onChange, nullable, depth = 0, onRemove, fieldContext }: PredicateBuilderProps) {
  // Empty / nullable state
  if (!value) {
    if (nullable) {
      return (
        <button
          style={addCondBtnStyle}
          onClick={() => onChange({ field: '', operator: 'EQ' })}
        >
          + Add condition
        </button>
      )
    }
    // Auto-init to a leaf
    onChange({ field: '', operator: 'EQ' })
    return null
  }

  if (isLeafPredicate(value)) {
    return (
      <LeafEditor
        value={value}
        onChange={onChange}
        onGroupify={() =>
          onChange({ match: 'and', predicates: [value, { field: '', operator: 'EQ' }] })
        }
        onRemove={onRemove}
        fieldContext={fieldContext}
      />
    )
  }

  return (
    <CompoundEditor
      value={value as PredicateAnd | PredicateOr | PredicateNot}
      onChange={onChange}
      onSimplify={() => {
        if (value.match === 'not') {
          const inner = (value as PredicateNot).predicate
          onChange(isLeafPredicate(inner) ? inner : { field: '', operator: 'EQ' })
        } else {
          const first = (value as PredicateAnd | PredicateOr).predicates[0]
          onChange(first && isLeafPredicate(first) ? first : { field: '', operator: 'EQ' })
        }
      }}
      onRemove={onRemove}
      depth={depth}
      fieldContext={fieldContext}
    />
  )
}

// ---------------------------------------------------------------------------
// Leaf editor
// ---------------------------------------------------------------------------

function LeafEditor({
  value,
  onChange,
  onGroupify,
  onRemove,
  fieldContext,
}: {
  value: PredicateLeaf
  onChange: (p: Predicate | null) => void
  onGroupify: () => void
  onRemove?: () => void
  fieldContext?: FieldContext
}) {
  const needsValue = value.operator !== 'IS_NULL' && value.operator !== 'IS_NOT_NULL'

  return (
    <div style={{ display: 'flex', gap: 4, alignItems: 'center', flexWrap: 'wrap' }}>
      <FieldCombobox
        fieldValue={value.field}
        onChange={f => onChange({ ...value, field: f })}
        fieldContext={fieldContext}
      />
      <select
        value={value.operator}
        onChange={e => onChange({ ...value, operator: e.target.value as FilterOperator })}
        style={selectStyle}
      >
        {OPERATORS.map(o => (
          <option key={o.value} value={o.value}>{o.label}</option>
        ))}
      </select>
      {needsValue && (
        <input
          type="text"
          placeholder="value"
          value={String(value.value ?? '')}
          onChange={e => onChange({ ...value, value: e.target.value })}
          style={valueInputStyle}
          title="Comparison value"
        />
      )}
      <button
        onClick={onGroupify}
        style={groupBtnStyle}
        title="Wrap in AND group to add more conditions"
      >
        Group ▾
      </button>
      {onRemove && (
        <button onClick={onRemove} style={removeBtnStyle} title="Remove condition">
          ✕
        </button>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Field combobox with lazy-loaded backend suggestions
// ---------------------------------------------------------------------------

export function FieldCombobox({
  fieldValue,
  onChange,
  fieldContext,
}: {
  fieldValue: string
  onChange: (v: string) => void
  fieldContext?: FieldContext
}) {
  const [open, setOpen] = useState(false)
  const [activeIdx, setActiveIdx] = useState(0)
  const [suggestions, setSuggestions] = useState<string[]>(STATIC_FIELD_SUGGESTIONS)
  const fetchedRef = useRef(false)
  const containerRef = useRef<HTMLDivElement>(null)

  // Fetch once on first focus
  function ensureSuggestions() {
    if (fetchedRef.current) return
    fetchedRef.current = true
    if (!fieldContext) return
    fetchFieldSuggestions(fieldContext)
      .then(fields => setSuggestions(fields))
      .catch(() => { /* keep static fallback */ })
  }
  const filtered = suggestions.filter(s =>
    s.toLowerCase().includes(fieldValue.toLowerCase())
  ).slice(0, 8)

  useEffect(() => {
    setActiveIdx(0)
  }, [fieldValue])

  useEffect(() => {
    if (!open) return
    function handleMouseDown(e: MouseEvent) {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false)
      }
    }
    document.addEventListener('mousedown', handleMouseDown)
    return () => document.removeEventListener('mousedown', handleMouseDown)
  }, [open])

  function handleKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (!open || filtered.length === 0) return
    if (e.key === 'ArrowDown') { e.preventDefault(); setActiveIdx(i => Math.min(i + 1, filtered.length - 1)) }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setActiveIdx(i => Math.max(i - 1, 0)) }
    else if (e.key === 'Enter') { e.preventDefault(); onChange(filtered[activeIdx]); setOpen(false) }
    else if (e.key === 'Escape') { setOpen(false) }
  }

  return (
    <div ref={containerRef} style={{ position: 'relative' }}>
      <input
        type="text"
        placeholder="$.value.field"
        value={fieldValue}
        onFocus={() => { ensureSuggestions(); setOpen(true) }}
        onChange={e => { onChange(e.target.value); setOpen(true) }}
        onKeyDown={handleKeyDown}
        style={fieldInputStyle}
        title="JSONPath field expression"
        autoComplete="off"
      />
      {open && filtered.length > 0 && (
        <div style={dropdownStyle}>
          {filtered.map((s, i) => (
            <div
              key={s}
              style={{
                ...dropdownRowStyle,
                background: i === activeIdx ? '#ebf8ff' : '#fff',
              }}
              onMouseDown={e => { e.preventDefault(); onChange(s); setOpen(false) }}
              onMouseEnter={() => setActiveIdx(i)}
            >
              <span style={dropdownPathStyle}>{s}</span>
              {ENVELOPE_DESCRIPTIONS[s] && (
                <span style={dropdownDescStyle}>{ENVELOPE_DESCRIPTIONS[s]}</span>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Compound editor (AND / OR / NOT)
// ---------------------------------------------------------------------------

function CompoundEditor({
  value,
  onChange,
  onSimplify,
  onRemove,
  depth,
  fieldContext,
}: {
  value: PredicateAnd | PredicateOr | PredicateNot
  onChange: (p: Predicate | null) => void
  onSimplify: () => void
  onRemove?: () => void
  depth: number
  fieldContext?: FieldContext
}) {
  const { match } = value
  const borderColor = BORDER_COLORS[depth % BORDER_COLORS.length]

  function setMatch(m: 'and' | 'or' | 'not') {
    if (m === match) return
    if (m === 'not') {
      const inner =
        'predicates' in value
          ? (value.predicates[0] ?? { field: '', operator: 'EQ' as const })
          : (value as PredicateNot).predicate
      onChange({ match: 'not', predicate: inner })
    } else {
      const preds =
        'predicates' in value
          ? value.predicates
          : [(value as PredicateNot).predicate, { field: '', operator: 'EQ' as const }]
      onChange({ match: m, predicates: preds } as PredicateAnd | PredicateOr)
    }
  }

  return (
    <div style={{ border: `1px solid ${borderColor}`, borderRadius: 6, padding: '0.5rem', background: depth % 2 === 0 ? '#fff' : '#f7fafc' }}>
      {/* Header row */}
      <div style={{ display: 'flex', gap: 5, alignItems: 'center', marginBottom: '0.4rem', flexWrap: 'wrap' }}>
        <span style={matchLabelStyle}>Match:</span>
        {(['and', 'or', 'not'] as const).map(m => (
          <button
            key={m}
            onClick={() => setMatch(m)}
            style={{
              ...matchBtnStyle,
              background: match === m ? '#3182ce' : '#fff',
              color: match === m ? '#fff' : '#4a5568',
            }}
          >
            {m.toUpperCase()}
          </button>
        ))}
        <div style={{ flex: 1 }} />
        <button onClick={onSimplify} style={simplifyBtnStyle} title="Simplify back to a single leaf condition">
          Simplify
        </button>
        {onRemove && (
          <button onClick={onRemove} style={removeBtnStyle} title="Remove group">✕</button>
        )}
      </div>

      {/* NOT: single nested predicate */}
      {match === 'not' && (
        <div style={{ paddingLeft: 8 }}>
          <PredicateBuilder
            value={(value as PredicateNot).predicate}
            onChange={inner =>
              onChange({ match: 'not', predicate: inner ?? { field: '', operator: 'EQ' } })
            }
            depth={depth + 1}
            fieldContext={fieldContext}
          />
        </div>
      )}

      {/* AND / OR: list of nested predicates */}
      {(match === 'and' || match === 'or') && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 5, paddingLeft: 8 }}>
          {(value as PredicateAnd | PredicateOr).predicates.map((pred, i) => {
            const preds = (value as PredicateAnd | PredicateOr).predicates
            return (
              <PredicateBuilder
                key={i}
                value={pred}
                onChange={updated => {
                  const newPreds = preds.map((p, idx) =>
                    idx === i ? (updated ?? { field: '', operator: 'EQ' as FilterOperator }) : p
                  )
                  onChange({ ...value, predicates: newPreds } as PredicateAnd | PredicateOr)
                }}
                onRemove={
                  preds.length > 1
                    ? () => {
                        const remaining = preds.filter((_, idx) => idx !== i)
                        if (remaining.length === 1 && isLeafPredicate(remaining[0])) {
                          onChange(remaining[0])
                        } else {
                          onChange({ ...value, predicates: remaining } as PredicateAnd | PredicateOr)
                        }
                      }
                    : undefined
                }
                depth={depth + 1}
                fieldContext={fieldContext}
              />
            )
          })}
          <button
            onClick={() => {
              const preds = (value as PredicateAnd | PredicateOr).predicates
              onChange({
                ...value,
                predicates: [...preds, { field: '', operator: 'EQ' as const }],
              } as PredicateAnd | PredicateOr)
            }}
            style={addCondBtnStyle}
          >
            + Add condition
          </button>
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const fieldInputStyle: React.CSSProperties = {
  padding: '0.25rem 0.4rem',
  border: '1px solid #cbd5e0',
  borderRadius: 4,
  fontSize: 12,
  width: 150,
  fontFamily: 'monospace',
  boxSizing: 'border-box',
}
const valueInputStyle: React.CSSProperties = {
  padding: '0.25rem 0.4rem',
  border: '1px solid #cbd5e0',
  borderRadius: 4,
  fontSize: 12,
  width: 110,
  boxSizing: 'border-box',
}
const selectStyle: React.CSSProperties = {
  padding: '0.25rem 0.35rem',
  border: '1px solid #cbd5e0',
  borderRadius: 4,
  fontSize: 12,
}
const groupBtnStyle: React.CSSProperties = {
  padding: '2px 7px',
  fontSize: 11,
  background: '#fff',
  color: '#4a5568',
  border: '1px solid #cbd5e0',
  borderRadius: 3,
  cursor: 'pointer',
}
const removeBtnStyle: React.CSSProperties = {
  padding: '2px 5px',
  fontSize: 12,
  background: 'none',
  color: '#a0aec0',
  border: 'none',
  cursor: 'pointer',
  lineHeight: 1,
}
const simplifyBtnStyle: React.CSSProperties = {
  padding: '2px 7px',
  fontSize: 11,
  background: '#fff',
  color: '#718096',
  border: '1px solid #cbd5e0',
  borderRadius: 3,
  cursor: 'pointer',
}
const addCondBtnStyle: React.CSSProperties = {
  padding: '3px 9px',
  fontSize: 12,
  background: '#ebf8ff',
  color: '#2b6cb0',
  border: '1px solid #bee3f8',
  borderRadius: 4,
  cursor: 'pointer',
  width: 'fit-content',
}
const matchLabelStyle: React.CSSProperties = {
  fontSize: 11,
  fontWeight: 700,
  color: '#718096',
  textTransform: 'uppercase',
  letterSpacing: '0.04em',
}
const matchBtnStyle: React.CSSProperties = {
  padding: '2px 8px',
  fontSize: 11,
  fontWeight: 700,
  textTransform: 'uppercase',
  border: '1px solid #cbd5e0',
  borderRadius: 3,
  cursor: 'pointer',
  letterSpacing: '0.04em',
}
const dropdownStyle: React.CSSProperties = {
  position: 'absolute',
  top: '100%',
  left: 0,
  zIndex: 100,
  marginTop: 2,
  background: '#fff',
  border: '1px solid #cbd5e0',
  borderRadius: 5,
  boxShadow: '0 4px 12px rgba(0,0,0,0.1)',
  minWidth: 220,
  maxHeight: 208,
  overflowY: 'auto',
}
const dropdownRowStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'baseline',
  justifyContent: 'space-between',
  gap: 8,
  padding: '5px 10px',
  cursor: 'pointer',
}
const dropdownPathStyle: React.CSSProperties = {
  fontFamily: 'monospace',
  fontSize: 12,
  color: '#2d3748',
  whiteSpace: 'nowrap',
}
const dropdownDescStyle: React.CSSProperties = {
  fontSize: 11,
  color: '#a0aec0',
  whiteSpace: 'nowrap',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  maxWidth: 120,
}
