import type { HTMLAttributes } from 'react'

/**
 * <Tabular> — mono type with tabular-nums. Use for every offset, timestamp,
 * count, duration, or byte-size on screen. Numbers in columns must line up;
 * that is non-negotiable in this design system.
 */
export function Tabular({
  children,
  className,
  muted,
  size = 'sm',
  ...rest
}: HTMLAttributes<HTMLSpanElement> & {
  muted?: boolean
  size?: 'xs' | 'sm' | 'md'
}) {
  const scale = size === 'xs' ? '0.75rem' : size === 'md' ? '0.875rem' : '0.8125rem'
  return (
    <span
      {...rest}
      data-tabular="true"
      className={className}
      style={{
        fontFamily: 'var(--font-mono)',
        fontSize: scale,
        fontWeight: 440,
        lineHeight: 'var(--type-mono-leading)',
        letterSpacing: 'var(--type-mono-tracking)',
        color: muted ? 'var(--ink-tertiary)' : 'var(--ink-primary)',
        fontVariantNumeric: 'tabular-nums',
        ...rest.style,
      }}
    >
      {children}
    </span>
  )
}
