import type { Predicate, PredicateLeaf, PredicateAnd, PredicateOr, PredicateNot, FilterOperator } from '#/transforms/types'
import { isLeafPredicate } from '#/transforms/types'

interface PredicateBuilderProps {
  value: Predicate | null | undefined
  onChange: (p: Predicate | null) => void
  /** When true, renders a '+ Add condition' button when value is null/undefined. */
  nullable?: boolean
  /** Nesting depth — controls border colour cycling. */
  depth?: number
  /** Renders an ✕ button that calls this when clicked. */
  onRemove?: () => void
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

export function PredicateBuilder({ value, onChange, nullable, depth = 0, onRemove }: PredicateBuilderProps) {
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
}: {
  value: PredicateLeaf
  onChange: (p: Predicate | null) => void
  onGroupify: () => void
  onRemove?: () => void
}) {
  const needsValue = value.operator !== 'IS_NULL' && value.operator !== 'IS_NOT_NULL'

  return (
    <div style={{ display: 'flex', gap: 4, alignItems: 'center', flexWrap: 'wrap' }}>
      <input
        type="text"
        placeholder="$.value.field"
        value={value.field}
        onChange={e => onChange({ ...value, field: e.target.value })}
        style={fieldInputStyle}
        title="JSONPath field expression"
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
// Compound editor (AND / OR / NOT)
// ---------------------------------------------------------------------------

function CompoundEditor({
  value,
  onChange,
  onSimplify,
  onRemove,
  depth,
}: {
  value: PredicateAnd | PredicateOr | PredicateNot
  onChange: (p: Predicate | null) => void
  onSimplify: () => void
  onRemove?: () => void
  depth: number
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
