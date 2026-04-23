import type { CSSProperties } from 'react'
import { StatusDot } from '../design/primitives/Badge'
import { SignatureRule } from '../design/primitives/SignatureRule'
import { Tabular } from '../design/primitives/Tabular'

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
  /**
   * When true, the signature rule draws in underneath the badge during
   * draining→tailing transitions. This is the design-system moment; let it
   * breathe. Defaults to true.
   */
  withRule?: boolean
}

interface Descriptor {
  label: string
  dot: React.ReactNode | null
  tone: 'neutral' | 'accent' | 'live' | 'warn' | 'error' | 'muted'
  showCount: boolean
  countLabel?: string
}

function describe(status: StreamStatus, _count: number, errorMessage?: string | null): Descriptor {
  switch (status) {
    case 'idle':
      return { label: 'Ready', dot: <StatusDot shape="ring" color="var(--ink-tertiary)" />, tone: 'muted', showCount: false }
    case 'draining':
      return {
        label: 'Replaying history',
        dot: <StatusDot shape="ring" color="var(--accent)" />,
        tone: 'accent',
        showCount: true,
        countLabel: 'records so far',
      }
    case 'tailing':
      return {
        label: 'Live',
        dot: <StatusDot shape="pulse" color="var(--signal-live)" />,
        tone: 'live',
        showCount: true,
        countLabel: 'records',
      }
    case 'streaming':
      return {
        label: 'Receiving',
        dot: <StatusDot shape="ring" color="var(--accent)" />,
        tone: 'accent',
        showCount: true,
      }
    case 'done':
      return {
        label: 'Complete',
        dot: <StatusDot shape="filled" color="var(--signal-live)" />,
        tone: 'live',
        showCount: true,
      }
    case 'overflow':
      return {
        label: 'Dropped — client too slow',
        dot: <StatusDot shape="square" color="var(--signal-error)" />,
        tone: 'error',
        showCount: false,
      }
    case 'disconnected':
      return {
        label: 'Disconnected',
        dot: <StatusDot shape="slash" color="var(--signal-warn)" />,
        tone: 'warn',
        showCount: true,
      }
    case 'stopped':
      return {
        label: 'Stopped',
        dot: <StatusDot shape="filled" color="var(--ink-tertiary)" />,
        tone: 'muted',
        showCount: true,
      }
    case 'error':
      return {
        label: errorMessage ?? 'Stream error',
        dot: <StatusDot shape="square" color="var(--signal-error)" />,
        tone: 'error',
        showCount: false,
      }
  }
}

const toneInk: Record<Descriptor['tone'], string> = {
  neutral: 'var(--ink-primary)',
  accent:  'var(--ink-primary)',
  live:    'var(--signal-live)',
  warn:    'var(--signal-warn)',
  error:   'var(--signal-error)',
  muted:   'var(--ink-tertiary)',
}

export function StreamStatusBadge({ status, count, errorMessage, withRule = true }: Props) {
  if (status === 'idle') return null

  const n = count ?? 0
  const d = describe(status, n, errorMessage)
  const tailing = status === 'tailing'

  const labelStyle: CSSProperties = {
    fontFamily: 'var(--font-body)',
    fontSize: 'var(--type-caption-size)',
    fontWeight: 540,
    letterSpacing: '0.01em',
    color: toneInk[d.tone],
    lineHeight: 1.25,
  }

  return (
    <div
      role="status"
      aria-live="polite"
      style={{
        display: 'inline-flex',
        flexDirection: 'column',
        gap: 8,
        minWidth: 0,
      }}
    >
      <div style={{ display: 'inline-flex', alignItems: 'center', gap: 10, minWidth: 0 }}>
        {d.dot}
        <span style={labelStyle}>{d.label}</span>
        {d.showCount && (
          <>
            <span
              aria-hidden
              style={{
                width: 1,
                height: 12,
                background: 'var(--rule-strong)',
                display: 'inline-block',
              }}
            />
            <Tabular size="xs" muted>
              {n.toLocaleString()}
              {d.countLabel && <span style={{ marginLeft: 4, fontFamily: 'var(--font-body)', letterSpacing: '0.005em' }}>{d.countLabel}</span>}
            </Tabular>
          </>
        )}
      </div>
      {withRule && (
        <SignatureRule active={tailing} color="var(--signal-live)" />
      )}
    </div>
  )
}
