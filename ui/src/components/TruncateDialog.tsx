import { useEffect, useId, useRef, useState } from 'react'

interface TruncateDialogProps {
  label: string
  onConfirm: (before: string) => void
  onCancel: () => void
}

export function TruncateDialog({ label, onConfirm, onCancel }: TruncateDialogProps) {
  const [before, setBefore] = useState('')
  const titleId = useId()
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    inputRef.current?.focus()
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onCancel() }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [onCancel])

  function handleConfirm() {
    if (!before) return
    onConfirm(new Date(before).toISOString())
  }

  return (
    <div
      aria-hidden="true"
      style={{
        position: 'fixed',
        inset: 0,
        background: 'rgba(0,0,0,0.4)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 1000,
      }}
      onClick={onCancel}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        style={{
          background: '#fff',
          borderRadius: 8,
          padding: '1.5rem 2rem',
          minWidth: 360,
          maxWidth: 480,
          boxShadow: '0 10px 30px rgba(0,0,0,0.2)',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <h2 id={titleId} style={{ margin: '0 0 0.5rem', fontSize: 17, fontWeight: 700 }}>Truncate {label}</h2>
        <p style={{ margin: '0 0 1.25rem', fontSize: 14, color: '#718096' }}>
          Delete all records <strong>before</strong> the selected date. This cannot be undone.
        </p>
        <div style={{ marginBottom: '1.5rem' }}>
          <label htmlFor={`${titleId}-before`} style={{ display: 'block', marginBottom: 4, fontSize: 13, fontWeight: 600, color: '#4a5568' }}>
            Before
          </label>
          <input
            ref={inputRef}
            id={`${titleId}-before`}
            type="datetime-local"
            style={{
              padding: '0.4rem 0.6rem',
              border: '1px solid #cbd5e0',
              borderRadius: 4,
              fontSize: 14,
              width: '100%',
              boxSizing: 'border-box',
            }}
            value={before}
            onChange={(e) => setBefore(e.target.value)}
          />
        </div>
        <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
          <button
            onClick={onCancel}
            style={{
              padding: '0.4rem 1rem',
              border: '1px solid #cbd5e0',
              borderRadius: 4,
              cursor: 'pointer',
              background: '#fff',
            }}
          >
            Cancel
          </button>
          <button
            onClick={handleConfirm}
            disabled={!before}
            style={{
              padding: '0.4rem 1rem',
              border: 'none',
              borderRadius: 4,
              cursor: before ? 'pointer' : 'not-allowed',
              background: before ? '#e53e3e' : '#fed7d7',
              color: '#fff',
            }}
          >
            Truncate
          </button>
        </div>
      </div>
    </div>
  )
}
