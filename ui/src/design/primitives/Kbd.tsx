import type { HTMLAttributes } from 'react'

/**
 * <Kbd> — mono, ruled, subtle. For inline keyboard shortcuts.
 */
export function Kbd({ children, style, ...rest }: HTMLAttributes<HTMLElement>) {
  return (
    <kbd
      {...rest}
      style={{
        fontFamily: 'var(--font-mono)',
        fontSize: '0.6875rem',
        fontWeight: 500,
        letterSpacing: '0.02em',
        color: 'var(--ink-secondary)',
        border: '1px solid var(--rule-strong)',
        borderBottomWidth: 2,
        borderRadius: 'var(--radius-xs)',
        padding: '0 5px',
        lineHeight: '1.25rem',
        display: 'inline-flex',
        alignItems: 'center',
        background: 'var(--surface-raised)',
        ...style,
      }}
    >
      {children}
    </kbd>
  )
}
