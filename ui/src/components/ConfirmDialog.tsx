interface ConfirmDialogProps {
  message: string
  onConfirm: () => void
  onCancel: () => void
}

export function ConfirmDialog({ message, onConfirm, onCancel }: ConfirmDialogProps) {
  return (
    <div
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
        style={{
          background: '#fff',
          borderRadius: 8,
          padding: '1.5rem 2rem',
          minWidth: 320,
          maxWidth: 480,
          boxShadow: '0 10px 30px rgba(0,0,0,0.2)',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <p style={{ margin: '0 0 1.5rem', fontSize: 15 }}>{message}</p>
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
            onClick={onConfirm}
            style={{
              padding: '0.4rem 1rem',
              border: 'none',
              borderRadius: 4,
              cursor: 'pointer',
              background: '#e53e3e',
              color: '#fff',
            }}
          >
            Confirm
          </button>
        </div>
      </div>
    </div>
  )
}
