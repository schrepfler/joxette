import { useEffect, useRef, useState, type CSSProperties } from 'react'

/**
 * <SignatureRule>
 *
 * A single hairline that draws from left to right over --duration-slow when
 * `active` flips to true. This is Joxette's one indulgent moment: the
 * transition from `draining` (replaying history) to `tailing` (live follow)
 * should feel momentous, not noisy.
 *
 * Place it directly below a status badge. On activation, the hairline
 * scales from 0 to 1 on its x-axis, easing out. No glow, no pulse — once
 * the rule is drawn, it holds steady. The live `StatusDot` next to it
 * carries the ongoing motion.
 *
 * When `active` flips back to false, the rule retracts to the right with
 * a quicker duration — drawing in is a commitment, retreating is a note.
 */
export function SignatureRule({
  active,
  color = 'var(--accent)',
  thickness = 1,
  className,
  style,
}: {
  active: boolean
  color?: string
  thickness?: number
  className?: string
  style?: CSSProperties
}) {
  const wasActiveRef = useRef(active)
  const [phase, setPhase] = useState<'idle' | 'drawing' | 'drawn' | 'retracting'>(
    active ? 'drawn' : 'idle',
  )

  useEffect(() => {
    if (active && !wasActiveRef.current) {
      setPhase('drawing')
      const t = setTimeout(() => setPhase('drawn'), 720)
      wasActiveRef.current = true
      return () => clearTimeout(t)
    }
    if (!active && wasActiveRef.current) {
      setPhase('retracting')
      const t = setTimeout(() => setPhase('idle'), 280)
      wasActiveRef.current = false
      return () => clearTimeout(t)
    }
    return
  }, [active])

  const scaleX = phase === 'idle' ? 0 : phase === 'drawing' || phase === 'drawn' ? 1 : 0
  const origin = phase === 'retracting' ? 'right' : 'left'
  const duration = phase === 'drawing' ? 'var(--duration-slow)' : phase === 'retracting' ? 'var(--duration-quick)' : 'var(--duration-default)'

  return (
    <div
      aria-hidden
      className={className}
      style={{
        height: thickness,
        overflow: 'hidden',
        ...style,
      }}
    >
      <div
        style={{
          height: thickness,
          background: color,
          transformOrigin: origin,
          transform: `scaleX(${scaleX})`,
          transition: `transform ${duration} var(--ease-out-soft)`,
        }}
      />
    </div>
  )
}
