import type { CSSProperties, ReactNode } from 'react'

/**
 * <StatusDot> — a tiny glyph, not just a coloured circle.
 *
 * Each shape carries different semantics:
 *   - filled  : present, steady
 *   - ring    : transient, mid-activity (e.g. draining history)
 *   - square  : terminal, warn (overflow, disconnected)
 *   - pulse   : live / tailing
 *
 * Shape matters: in a monochrome or colour-blind rendering, the dot still
 * reads correctly. Colour is the redundant cue, not the primary one.
 */
export type DotShape = 'filled' | 'ring' | 'square' | 'pulse' | 'slash'

export function StatusDot({
  shape,
  color,
  size = 10,
  label,
}: {
  shape: DotShape
  color: string
  size?: number
  label?: string
}) {
  const base: CSSProperties = {
    width: size,
    height: size,
    flexShrink: 0,
    display: 'inline-block',
  }

  if (shape === 'filled') {
    return <span aria-label={label} style={{ ...base, background: color, borderRadius: '50%' }} />
  }
  if (shape === 'ring') {
    return (
      <span
        aria-label={label}
        className="jx-dot-ring"
        style={{
          ...base,
          border: `1.5px solid ${color}`,
          borderTopColor: 'transparent',
          borderRadius: '50%',
        }}
      />
    )
  }
  if (shape === 'square') {
    return <span aria-label={label} style={{ ...base, background: color, borderRadius: 1 }} />
  }
  if (shape === 'pulse') {
    return (
      <span
        aria-label={label}
        style={{
          ...base,
          position: 'relative',
          display: 'inline-block',
        }}
      >
        <span
          style={{
            position: 'absolute',
            inset: 0,
            background: color,
            borderRadius: '50%',
          }}
        />
        <span
          className="jx-dot-pulse"
          style={{
            position: 'absolute',
            inset: 0,
            background: color,
            borderRadius: '50%',
            opacity: 0.4,
          }}
        />
      </span>
    )
  }
  if (shape === 'slash') {
    return (
      <span
        aria-label={label}
        style={{
          ...base,
          position: 'relative',
          borderRadius: '50%',
          border: `1.25px solid ${color}`,
        }}
      >
        <span
          style={{
            position: 'absolute',
            top: '50%',
            left: 0,
            right: 0,
            height: 1.25,
            background: color,
            transform: 'translateY(-50%) rotate(-45deg)',
          }}
        />
      </span>
    )
  }
  return null
}

/**
 * <Badge> — a composed chip: dot + label. No pill roundness, no fill.
 * Sits on paper; the dot carries the colour weight, the text carries tone.
 */
export function Badge({
  children,
  dot,
  tone = 'neutral',
  style,
}: {
  children: ReactNode
  dot?: { shape: DotShape; color: string }
  tone?: 'neutral' | 'live' | 'warn' | 'error' | 'accent' | 'muted'
  style?: CSSProperties
}) {
  const toneColor = (() => {
    switch (tone) {
      case 'live':   return 'var(--signal-live-ink)'
      case 'warn':   return 'var(--signal-warn-ink)'
      case 'error':  return 'var(--signal-error-ink)'
      case 'accent': return 'var(--accent)'
      case 'muted':  return 'var(--ink-tertiary)'
      default:       return 'var(--ink-secondary)'
    }
  })()

  return (
    <span
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: 8,
        fontFamily: 'var(--font-body)',
        fontSize: 'var(--type-caption-size)',
        letterSpacing: 'var(--type-caption-tracking)',
        fontWeight: 500,
        color: toneColor,
        lineHeight: 1.2,
        ...style,
      }}
    >
      {dot && <StatusDot shape={dot.shape} color={dot.color} />}
      {children}
    </span>
  )
}

if (typeof document !== 'undefined' && !document.getElementById('jx-dot-style')) {
  const el = document.createElement('style')
  el.id = 'jx-dot-style'
  el.textContent = `
@keyframes jx-dot-spin { to { transform: rotate(360deg) } }
@keyframes jx-dot-pulse { 0%,100% { transform: scale(1); opacity: .4 } 50% { transform: scale(1.7); opacity: 0 } }
.jx-dot-ring { animation: jx-dot-spin 1.1s linear infinite; }
.jx-dot-pulse { animation: jx-dot-pulse 1.8s var(--ease-in-out-soft) infinite; }
@media (prefers-reduced-motion: reduce) {
  .jx-dot-ring, .jx-dot-pulse { animation: none; }
}
`
  document.head.appendChild(el)
}
