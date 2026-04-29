import { createContext, useCallback, useContext, useState } from 'react'

interface ToastItem {
  id: number
  message: string
  type: 'success' | 'error' | 'info'
}

interface ToastContextValue {
  toasts: ToastItem[]
  addToast: (message: string, type?: ToastItem['type']) => void
}

const ToastContext = createContext<ToastContextValue>({
  toasts: [],
  addToast: () => {},
})

let nextId = 0

const toastStyles: Record<ToastItem['type'], React.CSSProperties> = {
  success: {
    background: 'var(--surface-paper)',
    color: 'var(--signal-live-ink)',
    border: '1px solid var(--signal-live)',
    borderLeft: '3px solid var(--signal-live)',
  },
  error: {
    background: 'var(--surface-paper)',
    color: 'var(--signal-error-ink)',
    border: '1px solid var(--signal-error)',
    borderLeft: '3px solid var(--signal-error)',
  },
  info: {
    background: 'var(--surface-paper)',
    color: 'var(--ink-primary)',
    border: '1px solid var(--rule-strong)',
    borderLeft: '3px solid var(--accent)',
  },
}

export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = useState<ToastItem[]>([])

  const addToast = useCallback((message: string, type: ToastItem['type'] = 'info') => {
    const id = ++nextId
    setToasts((prev) => [...prev, { id, message, type }])
    setTimeout(() => {
      setToasts((prev) => prev.filter((t) => t.id !== id))
    }, 3500)
  }, [])

  return (
    <ToastContext.Provider value={{ toasts, addToast }}>
      {children}
      <div
        style={{
          position: 'fixed',
          bottom: 24,
          right: 24,
          display: 'flex',
          flexDirection: 'column',
          gap: 8,
          zIndex: 2000,
        }}
      >
        {toasts.map((t) => (
          <div
            key={t.id}
            style={{
              ...toastStyles[t.type],
              padding: '10px 16px',
              borderRadius: 'var(--radius-sm)',
              boxShadow: 'var(--shadow-xl)',
              fontFamily: 'var(--font-body)',
              fontSize: 'var(--type-body-sm-size)',
              fontWeight: 500,
              maxWidth: 360,
            }}
          >
            {t.message}
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  )
}

export function useToast() {
  return useContext(ToastContext)
}
