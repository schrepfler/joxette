import { useEffect, useId, useRef, type ReactNode } from 'react'

interface ModalDialogProps {
  title: string
  onClose: () => void
  children: ReactNode
  /** Additional styles forwarded to the inner modal panel. */
  style?: React.CSSProperties
  /** CSS class forwarded to the inner modal panel (default: "jx-modal"). */
  className?: string
  /** Focus the first interactive element automatically (default: true). */
  autoFocus?: boolean
}

/**
 * Accessible modal overlay wrapper.
 *
 * - Overlay is aria-hidden so assistive technology only reads the dialog.
 * - Inner panel has role="dialog", aria-modal="true", aria-labelledby pointing at the title.
 * - Escape key closes the dialog.
 * - Focus is moved to the first focusable element inside on mount.
 */
export function ModalDialog({
  title,
  onClose,
  children,
  style,
  className = 'jx-modal',
  autoFocus = true,
}: ModalDialogProps) {
  const titleId = useId()
  const panelRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (autoFocus) {
      const first = panelRef.current?.querySelector<HTMLElement>(
        'input:not([disabled]), select:not([disabled]), textarea:not([disabled]), button:not([disabled]), [tabindex]:not([tabindex="-1"])'
      )
      first?.focus()
    }

    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose() }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [onClose, autoFocus])

  return (
    <div className="jx-overlay" onClick={onClose} aria-hidden="true">
      <div
        ref={panelRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        className={className}
        style={style}
        onClick={(e) => e.stopPropagation()}
      >
        <h2 id={titleId} style={{ margin: '0 0 1.25rem', fontSize: 18, fontWeight: 700 }}>{title}</h2>
        {children}
      </div>
    </div>
  )
}
