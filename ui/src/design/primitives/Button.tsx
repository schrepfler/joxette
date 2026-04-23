import { forwardRef, type ButtonHTMLAttributes, type ReactNode } from 'react'

type Variant = 'primary' | 'secondary' | 'ghost' | 'danger'
type Size = 'sm' | 'md'

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: Variant
  size?: Size
  leading?: ReactNode
  trailing?: ReactNode
  fullWidth?: boolean
}

/**
 * <Button>
 *
 * Three variants, one language.
 *   - primary   : accent-filled. The one thing the reader should press.
 *   - secondary : ink on paper, hairline border. The expected action.
 *   - ghost     : unframed, text-only, hover reveals a wash.
 *   - danger    : inverted accent for destructive actions; used sparingly.
 *
 * No drop shadows. Padding is optically tuned, not geometric.
 * Motion on press is a subtle translate, not a scale bounce.
 */
export const Button = forwardRef<HTMLButtonElement, ButtonProps>(function Button(
  { variant = 'secondary', size = 'md', leading, trailing, fullWidth, children, style, className, ...rest },
  ref,
) {
  const py = size === 'sm' ? 5 : 8
  const px = size === 'sm' ? 10 : 14
  const fontSize = size === 'sm' ? '0.8125rem' : '0.875rem'

  const base = {
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 8,
    padding: `${py}px ${px}px`,
    width: fullWidth ? '100%' : undefined,
    fontFamily: 'var(--font-body)',
    fontSize,
    fontWeight: 560,
    letterSpacing: '0.005em',
    lineHeight: 1.2,
    borderRadius: 'var(--radius-sm)',
    border: '1px solid transparent',
    cursor: 'pointer',
    transition:
      'background-color var(--duration-quick) var(--ease-out-soft),' +
      ' border-color var(--duration-quick) var(--ease-out-soft),' +
      ' color var(--duration-quick) var(--ease-out-soft),' +
      ' transform var(--duration-quick) var(--ease-out-soft)',
  } as const

  const variants: Record<Variant, React.CSSProperties> = {
    primary: {
      background: 'var(--accent)',
      color: 'var(--accent-ink)',
      borderColor: 'var(--accent)',
    },
    secondary: {
      background: 'transparent',
      color: 'var(--ink-primary)',
      borderColor: 'var(--rule-strong)',
    },
    ghost: {
      background: 'transparent',
      color: 'var(--ink-secondary)',
      borderColor: 'transparent',
    },
    danger: {
      background: 'transparent',
      color: 'var(--signal-error)',
      borderColor: 'color-mix(in oklab, var(--signal-error) 45%, transparent)',
    },
  }

  return (
    <button
      ref={ref}
      {...rest}
      data-variant={variant}
      className={['jx-button', className].filter(Boolean).join(' ')}
      style={{ ...base, ...variants[variant], ...style }}
    >
      {leading}
      {children}
      {trailing}
    </button>
  )
})

// Attach the hover/active styles via a stylesheet in head — keeps the
// primitive self-contained without requiring a global stylesheet edit.
if (typeof document !== 'undefined' && !document.getElementById('jx-button-style')) {
  const el = document.createElement('style')
  el.id = 'jx-button-style'
  el.textContent = `
.jx-button:hover:not(:disabled) { transform: translateY(-0.5px); }
.jx-button:active:not(:disabled) { transform: translateY(0.5px); }
.jx-button:disabled { opacity: .45; cursor: not-allowed; transform: none; }

.jx-button[data-variant="primary"]:hover:not(:disabled) {
  background: var(--accent-muted);
  border-color: var(--accent-muted);
}
.jx-button[data-variant="secondary"]:hover:not(:disabled) {
  background: var(--ink-wash);
  border-color: var(--ink-primary);
}
.jx-button[data-variant="ghost"]:hover:not(:disabled) {
  background: var(--ink-wash);
  color: var(--ink-primary);
}
.jx-button[data-variant="danger"]:hover:not(:disabled) {
  background: color-mix(in oklab, var(--signal-error) 10%, transparent);
  border-color: var(--signal-error);
  color: var(--signal-error);
}
`
  document.head.appendChild(el)
}
