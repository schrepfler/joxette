import { useEffect, useId, useRef, type ReactNode } from 'react'
import { createPortal } from 'react-dom'

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

// Portal target for every modal overlay in the app. Lazily created and
// appended directly to <body> so that a modal's DOM subtree is always a
// true sibling of the app root, never a descendant of it — that
// separation is what lets us mark the app root (and everything else in
// <body>) inert while a modal is open, without ever touching the modal's
// own subtree.
let modalPortalRoot: HTMLDivElement | null = null
function getModalPortalRoot(): HTMLDivElement {
  if (!modalPortalRoot) {
    modalPortalRoot = document.createElement('div')
    modalPortalRoot.id = 'jx-modal-root'
    document.body.appendChild(modalPortalRoot)
  }
  return modalPortalRoot
}

// Reference-counted so that if a second modal is ever opened while one is
// already open (e.g. a confirm dialog stacked on a form modal), the
// background is only restored once the last modal closes.
let openModalCount = 0
function setBackgroundInert(inert: boolean) {
  const portalRoot = getModalPortalRoot()
  for (const child of Array.from(document.body.children)) {
    if (child === portalRoot) continue
    if (inert) {
      // `inert` (native attribute) removes the subtree from focus,
      // pointer events, and the accessibility tree in one step.
      // `aria-hidden` is set alongside it for assistive-tech/browser
      // combinations that don't yet honour `inert`'s a11y effects.
      child.setAttribute('inert', '')
      child.setAttribute('aria-hidden', 'true')
    } else {
      child.removeAttribute('inert')
      child.removeAttribute('aria-hidden')
    }
  }
}

interface ModalPortalProps {
  onClose: () => void
  children: ReactNode
  /** Focus the first interactive element automatically (default: true). */
  autoFocus?: boolean
}

/**
 * Low-level accessible modal primitive: portals `children` to a dedicated
 * <body>-level node, marks the rest of <body> inert + aria-hidden while
 * mounted, focuses the first focusable element inside on mount, and closes
 * on Escape. Renders no chrome of its own (no overlay background, no
 * panel, no title) — callers own their layout. `ModalDialog` below is the
 * title+panel flavour built on top of this; `ConfirmDialog` and
 * `TruncateDialog` use this directly because their content doesn't fit
 * ModalDialog's title-as-<h2> shape.
 *
 * Because the rest of the document is inert while this is mounted, focus
 * can never leave the portaled subtree via Tab/Shift-Tab — that doubles
 * as the focus trap, with no manual key interception required.
 */
export function ModalPortal({ onClose, children, autoFocus = true }: ModalPortalProps) {
  const panelRef = useRef<HTMLDivElement>(null)
  const previousActiveElementRef = useRef<HTMLElement | null>(null)

  // Mount-only: capture whatever had focus before this modal opened, mark
  // the background inert, and auto-focus the first focusable element inside
  // the panel. Deliberately NOT dependent on `onClose`/`autoFocus` — every
  // call site passes an inline `onClose` arrow, which gets a new identity on
  // every parent re-render (e.g. AddMatcherModal's parent re-renders every
  // 250ms while a replay stream is live). Depending on `onClose` here meant
  // this effect tore down and re-ran on every such re-render, yanking focus
  // back to the first field mid-typing. On unmount, restore focus to the
  // pre-open element (WAI-ARIA APG requirement for dialogs) — guarded in
  // case that element was itself removed from the DOM while the modal was
  // open.
  useEffect(() => {
    previousActiveElementRef.current = document.activeElement as HTMLElement | null

    openModalCount += 1
    if (openModalCount === 1) setBackgroundInert(true)

    if (autoFocus) {
      const first = panelRef.current?.querySelector<HTMLElement>(
        'input:not([disabled]), select:not([disabled]), textarea:not([disabled]), button:not([disabled]), [tabindex]:not([tabindex="-1"])'
      )
      first?.focus()
    }

    return () => {
      openModalCount -= 1
      if (openModalCount === 0) setBackgroundInert(false)

      const previous = previousActiveElementRef.current
      if (previous && document.contains(previous) && typeof previous.focus === 'function') {
        previous.focus()
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // Escape-to-close: kept in its own effect, depending on `onClose`, so it
  // always calls the latest callback. Unlike the autofocus effect above,
  // re-subscribing this listener when `onClose`'s identity changes has no
  // visible side effect — no focus is moved, nothing flickers — so it's
  // safe (and necessary for correctness) to depend on it here.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose() }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [onClose])

  return createPortal(<div ref={panelRef}>{children}</div>, getModalPortalRoot())
}

/**
 * Accessible modal overlay wrapper — title + panel chrome on top of
 * ModalPortal.
 *
 * - Renders through ModalPortal: a true <body>-level sibling of the app
 *   root, with the app root marked inert + aria-hidden while open. The
 *   dialog's own subtree is never aria-hidden — assistive technology
 *   reads it normally.
 * - Inner panel has role="dialog", aria-modal="true", aria-labelledby
 *   pointing at the title.
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

  return (
    <ModalPortal onClose={onClose} autoFocus={autoFocus}>
      <div className="jx-overlay" onClick={onClose}>
        <div
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
    </ModalPortal>
  )
}
