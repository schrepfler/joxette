import type { CSSProperties } from 'react'

/**
 * 1px --rule divider. Used liberally. Prefer this to any other form of
 * separation between sections; boxes and cards are a last resort.
 */
export function Hairline({
  orientation = 'horizontal',
  strong = false,
  inset = 0,
  className,
  style,
}: {
  orientation?: 'horizontal' | 'vertical'
  strong?: boolean
  inset?: number | string
  className?: string
  style?: CSSProperties
}) {
  const color = strong ? 'var(--rule-strong)' : 'var(--rule)'
  if (orientation === 'vertical') {
    return (
      <span
        aria-hidden
        className={className}
        style={{
          display: 'inline-block',
          width: 1,
          alignSelf: 'stretch',
          background: color,
          marginBlock: inset,
          ...style,
        }}
      />
    )
  }
  return (
    <hr
      aria-hidden
      className={className}
      style={{
        border: 0,
        borderTop: `1px solid ${color}`,
        margin: 0,
        marginInline: inset,
        height: 0,
        ...style,
      }}
    />
  )
}
