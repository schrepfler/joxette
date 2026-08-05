import { useId, useState } from 'react'
import { ModalPortal } from './ModalDialog'

interface TruncateDialogProps {
  label: string
  onConfirm: (before: string) => void
  onCancel: () => void
}

export function TruncateDialog({ label, onConfirm, onCancel }: TruncateDialogProps) {
  const [before, setBefore] = useState('')
  const titleId = useId()

  function handleConfirm() {
    if (!before) return
    onConfirm(new Date(before).toISOString())
  }

  return (
    <ModalPortal onClose={onCancel}>
      <div className="jx-overlay" onClick={onCancel}>
        <div
          role="dialog"
          aria-modal="true"
          aria-labelledby={titleId}
          className="jx-modal"
          style={{ minWidth: 360, maxWidth: 480 }}
          onClick={(e) => e.stopPropagation()}
        >
          <h2 id={titleId} style={{ margin: '0 0 0.5rem', fontSize: 17, fontWeight: 700, color: 'var(--ink-primary)' }}>
            Truncate {label}
          </h2>
          <p style={{ margin: '0 0 1.25rem', fontSize: 'var(--type-caption-size)', color: 'var(--ink-secondary)' }}>
            Delete all records <strong>before</strong> the selected date. This cannot be undone.
          </p>
          <div style={{ marginBottom: '1.5rem' }}>
            <label htmlFor={`${titleId}-before`} style={{ display: 'block', marginBottom: 4, fontSize: 'var(--type-caption-size)', fontWeight: 600, color: 'var(--ink-secondary)' }}>
              Before
            </label>
            <input
              id={`${titleId}-before`}
              type="datetime-local"
              className="jx-input-box"
              style={{ width: '100%', boxSizing: 'border-box' }}
              value={before}
              onChange={(e) => setBefore(e.target.value)}
            />
          </div>
          <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
            <button onClick={onCancel} style={cancelBtn}>Cancel</button>
            <button
              onClick={handleConfirm}
              disabled={!before}
              style={before ? confirmBtnActive : confirmBtnDisabled}
            >
              Truncate
            </button>
          </div>
        </div>
      </div>
    </ModalPortal>
  )
}

const cancelBtn: React.CSSProperties = {
  padding: '0.4rem 1rem',
  border: '1px solid var(--rule-strong)',
  borderRadius: 'var(--radius-sm)',
  cursor: 'pointer',
  background: 'transparent',
  color: 'var(--ink-secondary)',
  fontFamily: 'var(--font-body)',
  fontSize: 'var(--type-body-sm-size)',
}

const confirmBtnActive: React.CSSProperties = {
  padding: '0.4rem 1rem',
  border: '1px solid var(--signal-error)',
  borderRadius: 'var(--radius-sm)',
  cursor: 'pointer',
  background: 'var(--signal-error)',
  color: '#fff',
  fontFamily: 'var(--font-body)',
  fontSize: 'var(--type-body-sm-size)',
}

const confirmBtnDisabled: React.CSSProperties = {
  ...confirmBtnActive,
  cursor: 'not-allowed',
  background: 'color-mix(in oklab, var(--signal-error) 35%, var(--surface-sunken))',
  borderColor: 'color-mix(in oklab, var(--signal-error) 35%, var(--surface-sunken))',
}
