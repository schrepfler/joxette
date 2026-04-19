import { useState } from 'react'
import type { FragmentDefinition, MessagePattern } from '#/transforms/types'
import { PatternBuilder } from './PatternBuilder'

interface Props {
  fragments: FragmentDefinition[]
  onChange: (fragments: FragmentDefinition[]) => void
}

const PRESET_COLORS = ['#4f8ef7', '#38a169', '#dd6b20', '#805ad5', '#d69e2e', '#e53e3e', '#319795', '#d53f8c']

function defaultPattern(): MessagePattern {
  return { predicate: { field: '', operator: 'EQ' }, quantifier: 'first' }
}

function emptyFragment(): FragmentDefinition {
  return {
    name: '',
    label: '',
    color: PRESET_COLORS[0],
    from: defaultPattern(),
    to: defaultPattern(),
  }
}

function patternSummary(p: MessagePattern): string {
  const pred = 'field' in p.predicate ? `${p.predicate.field || '…'} ${p.predicate.operator}` : '(compound)'
  const q = typeof p.quantifier === 'string'
    ? p.quantifier
    : 'nth' in p.quantifier
    ? `#${p.quantifier.nth}`
    : 'first after…'
  return `${q}: ${pred}`
}

export function FragmentManager({ fragments, onChange }: Props) {
  const [open, setOpen] = useState(false)
  const [editingIndex, setEditingIndex] = useState<number | null>(null)
  const [addingNew, setAddingNew] = useState(false)
  const [draft, setDraft] = useState<FragmentDefinition>(emptyFragment())

  function startAdd() {
    setDraft(emptyFragment())
    setEditingIndex(null)
    setAddingNew(true)
  }

  function startEdit(i: number) {
    setDraft({ ...fragments[i] })
    setEditingIndex(i)
    setAddingNew(false)
  }

  function cancelEdit() {
    setAddingNew(false)
    setEditingIndex(null)
  }

  function commitDraft() {
    if (!draft.name.trim()) return
    if (editingIndex !== null) {
      const updated = fragments.map((f, i) => (i === editingIndex ? draft : f))
      onChange(updated)
    } else {
      onChange([...fragments, draft])
    }
    setAddingNew(false)
    setEditingIndex(null)
  }

  function deleteFragment(i: number) {
    onChange(fragments.filter((_, idx) => idx !== i))
    if (editingIndex === i) cancelEdit()
  }

  const showForm = addingNew || editingIndex !== null

  return (
    <div style={containerStyle}>
      {/* Header */}
      <div
        style={headerStyle}
        onClick={() => setOpen(o => !o)}
        role="button"
        aria-expanded={open}
        tabIndex={0}
        onKeyDown={e => e.key === 'Enter' && setOpen(o => !o)}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: 13, fontWeight: 700, color: '#2d3748' }}>Phases</span>
          {fragments.length > 0 && (
            <span style={badgeStyle}>{fragments.length}</span>
          )}
          {fragments.length === 0 && !open && (
            <span style={{ fontSize: 12, color: '#a0aec0' }}>— no phases defined</span>
          )}
        </div>
        <span style={{ color: '#a0aec0', fontSize: 14 }}>{open ? '▲' : '▼'}</span>
      </div>

      {/* Body */}
      {open && (
        <div style={{ padding: '0.65rem' }}>
          {/* Fragment list */}
          {fragments.map((frag, i) => (
            <div key={frag.name} style={rowStyle}>
              <div style={swatchStyle(frag.color)} title={frag.color} />
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ fontSize: 13, fontWeight: 600, color: '#2d3748' }}>
                  {frag.name}
                  {frag.label && frag.label !== frag.name && (
                    <span style={{ fontSize: 12, color: '#718096', marginLeft: 6 }}>{frag.label}</span>
                  )}
                </div>
                <div style={anchorSummaryStyle}>
                  from: {patternSummary(frag.from)} &nbsp;→&nbsp; to: {patternSummary(frag.to)}
                  {frag.if && (
                    <span style={{ marginLeft: 6, color: '#805ad5' }}>
                      {frag.if.min_duration_ms != null ? `≥${frag.if.min_duration_ms}ms` : ''}
                      {frag.if.min_duration_ms != null && frag.if.max_duration_ms != null ? ' ' : ''}
                      {frag.if.max_duration_ms != null ? `≤${frag.if.max_duration_ms}ms` : ''}
                    </span>
                  )}
                </div>
              </div>
              <button
                style={iconBtnStyle}
                title="Edit"
                onClick={() => startEdit(i)}
              >
                ✏️
              </button>
              <button
                style={{ ...iconBtnStyle, color: '#e53e3e' }}
                title="Delete"
                onClick={() => deleteFragment(i)}
              >
                🗑
              </button>
            </div>
          ))}

          {fragments.length === 0 && !showForm && (
            <p style={{ fontSize: 13, color: '#a0aec0', textAlign: 'center', margin: '0.25rem 0 0.5rem' }}>
              No phases defined.
            </p>
          )}

          {/* Inline form */}
          {showForm && (
            <div style={formBoxStyle}>
              <div style={formTitleStyle}>
                {editingIndex !== null ? 'Edit phase' : 'Add phase'}
              </div>

              {/* Name + label */}
              <div style={{ display: 'flex', gap: 8, marginBottom: '0.5rem', flexWrap: 'wrap' }}>
                <label style={fieldLabelStyle}>
                  Name (identifier)
                  <input
                    type="text"
                    value={draft.name}
                    onChange={e => setDraft(d => ({ ...d, name: e.target.value.replace(/\s+/g, '_') }))}
                    placeholder="checkout"
                    style={textInputStyle}
                  />
                </label>
                <label style={fieldLabelStyle}>
                  Label (display)
                  <input
                    type="text"
                    value={draft.label}
                    onChange={e => setDraft(d => ({ ...d, label: e.target.value }))}
                    placeholder="Checkout Phase"
                    style={textInputStyle}
                  />
                </label>
              </div>

              {/* Color picker */}
              <div style={{ marginBottom: '0.5rem' }}>
                <div style={sectionLabelStyle}>Color</div>
                <div style={{ display: 'flex', gap: 5, alignItems: 'center', flexWrap: 'wrap' }}>
                  {PRESET_COLORS.map(c => (
                    <button
                      key={c}
                      onClick={() => setDraft(d => ({ ...d, color: c }))}
                      style={{
                        width: 20,
                        height: 20,
                        borderRadius: '50%',
                        background: c,
                        border: draft.color === c ? '2px solid #2d3748' : '2px solid transparent',
                        cursor: 'pointer',
                        padding: 0,
                      }}
                      title={c}
                    />
                  ))}
                  <input
                    type="text"
                    value={draft.color}
                    onChange={e => setDraft(d => ({ ...d, color: e.target.value }))}
                    placeholder="#4f8ef7"
                    style={{ ...textInputStyle, width: 80, fontFamily: 'monospace' }}
                  />
                  <div style={{ width: 20, height: 20, borderRadius: 4, background: draft.color, border: '1px solid #cbd5e0' }} />
                </div>
              </div>

              {/* From pattern */}
              <div style={{ marginBottom: '0.5rem' }}>
                <div style={sectionLabelStyle}>From anchor</div>
                <PatternBuilder
                  value={draft.from}
                  onChange={from => setDraft(d => ({ ...d, from }))}
                />
              </div>

              {/* To pattern */}
              <div style={{ marginBottom: '0.5rem' }}>
                <div style={sectionLabelStyle}>To anchor</div>
                <PatternBuilder
                  value={draft.to}
                  onChange={to => setDraft(d => ({ ...d, to }))}
                  showIfClause
                  ifClause={draft.if}
                  onIfClauseChange={c => setDraft(d => ({ ...d, if: c }))}
                />
              </div>

              {/* Actions */}
              <div style={{ display: 'flex', gap: 6, marginTop: '0.5rem' }}>
                <button
                  style={saveBtnStyle}
                  onClick={commitDraft}
                  disabled={!draft.name.trim()}
                >
                  {editingIndex !== null ? 'Save' : 'Add'}
                </button>
                <button style={cancelBtnStyle} onClick={cancelEdit}>
                  Cancel
                </button>
              </div>
            </div>
          )}

          {/* Add button */}
          {!showForm && (
            <button style={addBtnStyle} onClick={startAdd}>
              + Add phase
            </button>
          )}
        </div>
      )}
    </div>
  )
}

const swatchStyle = (color: string): React.CSSProperties => ({
  width: 14,
  height: 14,
  borderRadius: 3,
  background: color,
  flexShrink: 0,
  marginTop: 2,
  border: '1px solid rgba(0,0,0,0.12)',
})

const containerStyle: React.CSSProperties = {
  border: '1px solid #e2e8f0',
  borderRadius: 8,
  marginBottom: '0.65rem',
  overflow: 'hidden',
}
const headerStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  padding: '0.5rem 0.85rem',
  background: '#f7fafc',
  cursor: 'pointer',
  userSelect: 'none',
  borderBottom: '1px solid #e2e8f0',
}
const badgeStyle: React.CSSProperties = {
  background: '#805ad5',
  color: '#fff',
  fontSize: 11,
  fontWeight: 700,
  padding: '1px 7px',
  borderRadius: 10,
}
const rowStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'flex-start',
  gap: 8,
  padding: '0.4rem 0.3rem',
  borderBottom: '1px solid #f0f4f8',
}
const anchorSummaryStyle: React.CSSProperties = {
  fontSize: 11,
  color: '#718096',
  marginTop: 2,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
}
const iconBtnStyle: React.CSSProperties = {
  background: 'none',
  border: 'none',
  cursor: 'pointer',
  fontSize: 14,
  padding: '0 2px',
  color: '#718096',
  lineHeight: 1,
}
const formBoxStyle: React.CSSProperties = {
  border: '1px solid #bee3f8',
  borderRadius: 6,
  padding: '0.65rem',
  marginBottom: '0.5rem',
  background: '#ebf8ff',
}
const formTitleStyle: React.CSSProperties = {
  fontSize: 12,
  fontWeight: 700,
  color: '#2b6cb0',
  marginBottom: '0.5rem',
  textTransform: 'uppercase',
  letterSpacing: '0.04em',
}
const sectionLabelStyle: React.CSSProperties = {
  fontSize: 11,
  fontWeight: 700,
  color: '#718096',
  textTransform: 'uppercase',
  letterSpacing: '0.04em',
  marginBottom: '0.3rem',
}
const fieldLabelStyle: React.CSSProperties = {
  fontSize: 12,
  color: '#4a5568',
  display: 'flex',
  flexDirection: 'column',
  gap: 3,
}
const textInputStyle: React.CSSProperties = {
  padding: '0.25rem 0.4rem',
  border: '1px solid #cbd5e0',
  borderRadius: 4,
  fontSize: 12,
  width: 160,
  boxSizing: 'border-box',
}
const saveBtnStyle: React.CSSProperties = {
  padding: '0.3rem 0.85rem',
  background: '#3182ce',
  color: '#fff',
  border: 'none',
  borderRadius: 4,
  cursor: 'pointer',
  fontSize: 12,
}
const cancelBtnStyle: React.CSSProperties = {
  padding: '0.3rem 0.85rem',
  background: '#fff',
  color: '#4a5568',
  border: '1px solid #cbd5e0',
  borderRadius: 4,
  cursor: 'pointer',
  fontSize: 12,
}
const addBtnStyle: React.CSSProperties = {
  marginTop: '0.25rem',
  padding: '0.3rem 0.75rem',
  background: '#fff',
  color: '#805ad5',
  border: '1px solid #d6bcfa',
  borderRadius: 4,
  cursor: 'pointer',
  fontSize: 12,
}
