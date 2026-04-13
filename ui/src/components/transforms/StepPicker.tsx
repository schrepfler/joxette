import { useState } from 'react'
import { STEPS_BY_CATEGORY, type StepDef } from '#/transforms/definitions'
import type { TransformStep } from '#/transforms/types'

interface StepPickerProps {
  onSelect: (type: TransformStep['type']) => void
  onClose: () => void
}

export function StepPicker({ onSelect, onClose }: StepPickerProps) {
  const [search, setSearch] = useState('')
  const lower = search.toLowerCase()

  const filtered = STEPS_BY_CATEGORY.map(({ category, steps }) => ({
    category,
    steps: steps.filter(
      s => !lower || s.label.toLowerCase().includes(lower) || s.description.toLowerCase().includes(lower),
    ),
  })).filter(c => c.steps.length > 0)

  return (
    <div style={overlayStyle} onClick={e => { if (e.target === e.currentTarget) onClose() }}>
      <div style={modalStyle}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.75rem' }}>
          <h2 style={{ margin: 0, fontSize: 16, fontWeight: 700 }}>Add Transform Step</h2>
          <button style={closeBtnStyle} onClick={onClose} aria-label="Close">✕</button>
        </div>
        <input
          autoFocus
          type="text"
          placeholder="Search step types…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          style={searchStyle}
        />
        <div style={{ overflowY: 'auto', flex: 1 }}>
          {filtered.map(({ category, steps }) => (
            <div key={category} style={{ marginBottom: '0.75rem' }}>
              <div style={categoryLabelStyle}>{category}</div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: 6 }}>
                {steps.map(def => (
                  <StepOption key={def.type} def={def} onSelect={onSelect} />
                ))}
              </div>
            </div>
          ))}
          {filtered.length === 0 && (
            <p style={{ color: '#718096', fontSize: 13, textAlign: 'center', marginTop: '2rem' }}>
              No steps match "{search}"
            </p>
          )}
        </div>
      </div>
    </div>
  )
}

function StepOption({ def, onSelect }: { def: StepDef; onSelect: (type: TransformStep['type']) => void }) {
  return (
    <button
      style={stepOptionStyle}
      onClick={() => onSelect(def.type)}
      title={def.description}
    >
      <span style={{ fontWeight: 600, fontSize: 13 }}>{def.label}</span>
      <span style={{ fontSize: 11, color: '#718096', marginTop: 2, lineHeight: 1.3 }}>{def.description}</span>
    </button>
  )
}

const overlayStyle: React.CSSProperties = {
  position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', zIndex: 1000,
  display: 'flex', alignItems: 'center', justifyContent: 'center',
}
const modalStyle: React.CSSProperties = {
  background: '#fff', borderRadius: 8, padding: '1.25rem',
  width: 640, maxWidth: '95vw', maxHeight: '80vh',
  display: 'flex', flexDirection: 'column', boxShadow: '0 8px 32px rgba(0,0,0,0.18)',
}
const closeBtnStyle: React.CSSProperties = {
  background: 'none', border: 'none', fontSize: 18, cursor: 'pointer', color: '#718096', padding: '0 4px',
}
const searchStyle: React.CSSProperties = {
  padding: '0.45rem 0.7rem', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 14,
  width: '100%', boxSizing: 'border-box', marginBottom: '0.75rem',
}
const categoryLabelStyle: React.CSSProperties = {
  fontSize: 11, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.06em',
  color: '#4a5568', marginBottom: 4,
}
const stepOptionStyle: React.CSSProperties = {
  display: 'flex', flexDirection: 'column', textAlign: 'left',
  background: '#f7fafc', border: '1px solid #e2e8f0', borderRadius: 6, padding: '0.5rem 0.75rem',
  cursor: 'pointer', transition: 'background 0.12s',
}
