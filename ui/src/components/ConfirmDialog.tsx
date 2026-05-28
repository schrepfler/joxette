import { useEffect, useId, useRef } from 'react'

interface ConfirmDialogProps {
  message: string
  onConfirm: () => void
  onCancel: () => void
}

export function ConfirmDialog({ message, onConfirm, onCancel }: ConfirmDialogProps) {
  const titleId = useId()
  const cancelRef = useRef<HTMLButtonElement>(null)

  useEffect(() => {
    cancelRef.current?.focus()
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onCancel() }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [onCancel])

  return (
    <div className="jx-overlay" onClick={onCancel} aria-hidden="true">
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        className="jx-modal"
        style={{ minWidth: 320, maxWidth: 480 }}
        onClick={(e) => e.stopPropagation()}
      >
        <p
          id={titleId}
          style={{
            margin: '0 0 24px',
            fontFamily: 'var(--font-body)',
            fontSize: 'var(--type-body-size)',
            color: 'var(--ink-primary)',
            lineHeight: 1.6,
          }}
        >
          {message}
        </p>
        <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
          <button ref={cancelRef} onClick={onCancel} style={cancelBtn}>Cancel</button>
          <button onClick={onConfirm} style={confirmBtn}>Confirm</button>
        </div>
      </div>
    </div>
  )
}

const cancelBtn: React.CSSProperties = {
  padding: '7px 16px',
  border: '1px solid var(--rule-strong)',
  borderRadius: 'var(--radius-sm)',
  cursor: 'pointer',
  background: 'transparent',
  color: 'var(--ink-secondary)',
  fontFamily: 'var(--font-body)',
  fontSize: 'var(--type-body-sm-size)',
  fontWeight: 500,
}

const confirmBtn: React.CSSProperties = {
  padding: '7px 16px',
  border: '1px solid var(--signal-error)',
  borderRadius: 'var(--radius-sm)',
  cursor: 'pointer',
  background: 'var(--signal-error)',
  color: '#fff',
  fontFamily: 'var(--font-body)',
  fontSize: 'var(--type-body-sm-size)',
  fontWeight: 500,
}
