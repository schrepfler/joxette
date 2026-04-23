import type { CSSProperties } from 'react'

/**
 * Status vocabulary for record streams.
 *
 * - `idle`         — no stream running (before the user clicks Start)
 * - `draining`     — historical replay in progress (follow mode, before `follow` preamble)
 * - `tailing`      — live tail active (follow mode, after `follow` preamble)
 * - `streaming`    — non-follow stream in progress
 * - `done`         — non-follow stream completed naturally
 * - `overflow`     — terminal, subscription dropped (slow consumer)
 * - `disconnected` — follow stream ended unexpectedly (any EOF in follow mode)
 * - `stopped`      — user aborted the stream
 * - `error`        — transport error
 */
export type StreamStatus =
  | 'idle'
  | 'draining'
  | 'tailing'
  | 'streaming'
  | 'done'
  | 'overflow'
  | 'disconnected'
  | 'stopped'
  | 'error'

interface Props {
  status: StreamStatus
  count?: number
  errorMessage?: string | null
}

// Keyframes emitted once, lazily, so the component can ship its own animations
// without depending on the global stylesheet.
const STYLE_ID = 'stream-status-badge-keyframes'
function ensureKeyframes() {
  if (typeof document === 'undefined') return
  if (document.getElementById(STYLE_ID)) return
  const el = document.createElement('style')
  el.id = STYLE_ID
  el.textContent = `
@keyframes stream-spin { to { transform: rotate(360deg) } }
@keyframes stream-pulse { 0%,100% { opacity: 1 } 50% { opacity: .35 } }
`.trim()
  document.head.appendChild(el)
}

export function StreamStatusBadge({ status, count, errorMessage }: Props) {
  ensureKeyframes()
  const n = (count ?? 0).toLocaleString()

  switch (status) {
    case 'idle':
      return null

    case 'draining':
      return (
        <span style={{ ...textBase, color: '#4a5568' }}>
          <span style={spinnerStyle} aria-label="Draining history" />
          Replaying history — {n} records
        </span>
      )

    case 'tailing':
      return (
        <span style={{ ...textBase, color: '#276749' }}>
          <span style={{ ...dotStyle, background: '#38a169', animation: 'stream-pulse 1.4s ease-in-out infinite' }} />
          Live — {n} records
        </span>
      )

    case 'streaming':
      return (
        <span style={{ ...textBase, color: '#718096' }}>
          <span style={{ ...dotStyle, background: '#3182ce' }} />
          Receiving… {n} records
        </span>
      )

    case 'done':
      return (
        <span style={{ ...textBase, color: '#276749' }}>
          <span style={{ ...dotStyle, background: '#38a169' }} />✓ Complete — {n} records
        </span>
      )

    case 'overflow':
      return (
        <span style={{ ...textBase, color: '#c53030' }}>
          <span style={{ ...dotStyle, background: '#e53e3e' }} />✗ Dropped — client too slow
        </span>
      )

    case 'disconnected':
      return (
        <span style={{ ...textBase, color: '#b7791f' }}>
          <span style={{ ...dotStyle, background: '#dd6b20' }} />Disconnected — {n} records
        </span>
      )

    case 'stopped':
      return (
        <span style={{ ...textBase, color: '#4a5568' }}>
          <span style={{ ...dotStyle, background: '#a0aec0' }} />Stopped — {n} records
        </span>
      )

    case 'error':
      return (
        <span style={{ ...textBase, color: '#c53030' }}>
          ✗ {errorMessage ?? 'Stream error'}
        </span>
      )
  }
}

const textBase: CSSProperties = {
  fontSize: 13,
  display: 'inline-flex',
  alignItems: 'center',
  gap: 6,
}

const dotStyle: CSSProperties = {
  width: 8,
  height: 8,
  borderRadius: '50%',
  display: 'inline-block',
}

const spinnerStyle: CSSProperties = {
  width: 10,
  height: 10,
  borderRadius: '50%',
  border: '2px solid #cbd5e0',
  borderTopColor: '#3182ce',
  display: 'inline-block',
  animation: 'stream-spin 0.8s linear infinite',
}
