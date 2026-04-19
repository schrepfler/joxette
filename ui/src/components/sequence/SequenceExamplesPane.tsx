import { useState } from 'react'
import type { CassetteRecord } from '../../api/client'
import type { MatchedSequence, MatchStep } from '../../transforms/types'
import { STEP_COLORS, useSequenceStore } from '../../stores/sequenceStore'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  return `${(ms / 1000).toFixed(1)}s`
}

function gapMs(a: string, b: string): number {
  return new Date(b).getTime() - new Date(a).getTime()
}

/** Return the anchor index (0-based) that this message corresponds to, or -1 for wildcards. */
function anchorIndexFor(msg: CassetteRecord, anchorTimestamps: string[]): number {
  return anchorTimestamps.indexOf(msg.timestamp)
}

// ---------------------------------------------------------------------------
// WildcardPill
// ---------------------------------------------------------------------------

interface WildcardGroup {
  messages: CassetteRecord[]
}

function WildcardPill({ group }: { group: WildcardGroup }) {
  const [open, setOpen] = useState(false)
  const count = group.messages.length

  if (count === 0) return null

  if (count <= 3) {
    return (
      <>
        {group.messages.map((m) => {
          const label = (() => {
            try {
              const v = JSON.parse(m.value ?? '')
              return v?.type ?? m.topic
            } catch {
              return m.topic
            }
          })()
          return (
            <span
              key={`${m.partition}-${m.offset}`}
              style={{
                display: 'inline-block',
                padding: '2px 6px',
                borderRadius: 10,
                fontSize: 11,
                background: 'var(--color-surface-raised, #3a3a3a)',
                color: 'var(--color-text-muted, #aaa)',
                border: '1px solid var(--color-border, #555)',
                whiteSpace: 'nowrap',
              }}
            >
              {label}
            </span>
          )
        })}
      </>
    )
  }

  return (
    <span
      title={open ? undefined : `${count} wildcard messages`}
      onClick={() => setOpen((o) => !o)}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        padding: '2px 8px',
        borderRadius: 10,
        fontSize: 11,
        background: 'var(--color-surface-raised, #3a3a3a)',
        color: 'var(--color-text-muted, #aaa)',
        border: '1px solid var(--color-border, #555)',
        cursor: 'pointer',
        whiteSpace: 'nowrap',
        gap: 4,
      }}
    >
      {open ? (
        <>
          {group.messages.map((m) => {
            const label = (() => {
              try {
                const v = JSON.parse(m.value ?? '')
                return v?.type ?? m.topic
              } catch {
                return m.topic
              }
            })()
            return (
              <span key={`${m.partition}-${m.offset}`} style={{ marginRight: 4 }}>
                {label}
              </span>
            )
          })}
        </>
      ) : (
        <>···&nbsp;<span style={{ fontSize: 10, opacity: 0.7 }}>×{count}</span></>
      )}
    </span>
  )
}

// ---------------------------------------------------------------------------
// AnchorChip
// ---------------------------------------------------------------------------

interface AnchorChipProps {
  label: string
  color: string
  isLast: boolean
  incomplete: boolean
}

function AnchorChip({ label, color, isLast, incomplete }: AnchorChipProps) {
  return (
    <span
      style={{
        display: 'inline-block',
        padding: '3px 10px',
        borderRadius: 12,
        fontSize: 12,
        fontWeight: 600,
        background: color,
        color: '#fff',
        whiteSpace: 'nowrap',
        boxShadow: incomplete && isLast
          ? `0 2px 0 0 #e05c5c`
          : undefined,
        borderBottom: incomplete && isLast
          ? '2px solid #e05c5c'
          : undefined,
      }}
    >
      {label}
    </span>
  )
}

// ---------------------------------------------------------------------------
// MessageRow (chip strip for one sequence)
// ---------------------------------------------------------------------------

type ChipItem =
  | { kind: 'anchor'; msg: CassetteRecord; stepIndex: number; label: string }
  | { kind: 'wildcards'; msgs: CassetteRecord[] }

function buildChipItems(
  messages: CassetteRecord[],
  anchorTimestamps: string[],
  steps: MatchStep[],
): ChipItem[] {
  const items: ChipItem[] = []
  let wildcardBuf: CassetteRecord[] = []

  for (const msg of messages) {
    const aIdx = anchorIndexFor(msg, anchorTimestamps)
    if (aIdx >= 0) {
      if (wildcardBuf.length > 0) {
        items.push({ kind: 'wildcards', msgs: wildcardBuf })
        wildcardBuf = []
      }
      const step = steps[aIdx]
      const label = step?.label ?? (() => {
        try {
          const v = JSON.parse(msg.value ?? '')
          return v?.type ?? msg.topic
        } catch {
          return msg.topic
        }
      })()
      items.push({ kind: 'anchor', msg, stepIndex: aIdx, label })
    } else {
      wildcardBuf.push(msg)
    }
  }
  if (wildcardBuf.length > 0) items.push({ kind: 'wildcards', msgs: wildcardBuf })
  return items
}

// ---------------------------------------------------------------------------
// AnchorOnlyRow (top row — proportionally spaced anchors)
// ---------------------------------------------------------------------------

function AnchorOnlyRow({
  seq,
  steps,
  incomplete,
}: {
  seq: MatchedSequence
  steps: MatchStep[]
  incomplete: boolean
}) {
  const anchors = seq.anchorTimestamps
  if (anchors.length === 0) return null

  const totalMs = anchors.length > 1
    ? new Date(anchors[anchors.length - 1]).getTime() - new Date(anchors[0]).getTime()
    : 0

  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        gap: 4,
        minHeight: 26,
        overflowX: 'auto',
        paddingBottom: 2,
      }}
    >
      {anchors.map((ts, i) => {
        const step = steps[i]
        const color = STEP_COLORS[i % STEP_COLORS.length]
        const label = step?.label ?? `Step ${i + 1}`
        const isLast = i === anchors.length - 1

        const spacerWidth = i > 0 && totalMs > 0
          ? Math.max(16, Math.round(
              ((new Date(ts).getTime() - new Date(anchors[i - 1]).getTime()) / totalMs) * 120,
            ))
          : 0

        const gapLabel = i > 0
          ? formatDuration(gapMs(anchors[i - 1], ts))
          : null

        return (
          <div key={ts + i} style={{ display: 'flex', alignItems: 'center', gap: 4, flexShrink: 0 }}>
            {i > 0 && (
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 4,
                  width: spacerWidth,
                  minWidth: 40,
                  justifyContent: 'center',
                }}
              >
                <span
                  style={{
                    height: 1,
                    flex: 1,
                    background: 'var(--color-border, #555)',
                  }}
                />
                <span
                  style={{
                    fontSize: 10,
                    color: 'var(--color-text-muted, #aaa)',
                    whiteSpace: 'nowrap',
                  }}
                >
                  ({gapLabel})
                </span>
                <span
                  style={{
                    height: 1,
                    flex: 1,
                    background: 'var(--color-border, #555)',
                  }}
                />
              </div>
            )}
            <AnchorChip
              label={label}
              color={color}
              isLast={isLast}
              incomplete={incomplete}
            />
          </div>
        )
      })}
    </div>
  )
}

// ---------------------------------------------------------------------------
// AllMessagesRow (bottom row — all chips in order)
// ---------------------------------------------------------------------------

function AllMessagesRow({
  seq,
  steps,
  incomplete,
}: {
  seq: MatchedSequence
  steps: MatchStep[]
  incomplete: boolean
}) {
  const items = buildChipItems(seq.messages, seq.anchorTimestamps, steps)
  const lastAnchorItemIdx = items.reduceRight((found, item, i) =>
    found === -1 && item.kind === 'anchor' ? i : found, -1)

  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        gap: 4,
        minHeight: 26,
        overflowX: 'auto',
        flexWrap: 'nowrap',
      }}
    >
      {items.map((item, idx) => {
        if (item.kind === 'anchor') {
          const color = STEP_COLORS[item.stepIndex % STEP_COLORS.length]
          const isLast = item.stepIndex === seq.anchorTimestamps.length - 1
          return (
            <div key={`a-${item.msg.partition}-${item.msg.offset}`} style={{ flexShrink: 0 }}>
              <AnchorChip
                label={item.label}
                color={color}
                isLast={isLast && idx === lastAnchorItemIdx}
                incomplete={incomplete}
              />
            </div>
          )
        }
        return (
          <div key={`w-${idx}`} style={{ flexShrink: 0, display: 'flex', gap: 4 }}>
            <WildcardPill group={{ messages: item.msgs }} />
          </div>
        )
      })}
    </div>
  )
}

// ---------------------------------------------------------------------------
// SequenceRow
// ---------------------------------------------------------------------------

interface SequenceRowProps {
  seq: MatchedSequence
  steps: MatchStep[]
  totalSteps: number
}

function SequenceRow({ seq, steps, totalSteps }: SequenceRowProps) {
  const [expanded, setExpanded] = useState(false)
  const incomplete = seq.anchorTimestamps.length < totalSteps

  return (
    <div
      style={{
        borderBottom: '1px solid var(--color-border, #333)',
        cursor: 'pointer',
      }}
    >
      <div
        onClick={() => setExpanded((e) => !e)}
        style={{
          padding: '8px 12px',
          display: 'flex',
          flexDirection: 'column',
          gap: 4,
        }}
      >
        <AnchorOnlyRow seq={seq} steps={steps} incomplete={incomplete} />
        <AllMessagesRow seq={seq} steps={steps} incomplete={incomplete} />
        {seq.durationMs > 0 && (
          <div style={{ fontSize: 10, color: 'var(--color-text-muted, #aaa)', marginTop: 2 }}>
            total: {formatDuration(seq.durationMs)}
          </div>
        )}
      </div>

      {expanded && (
        <div
          style={{
            padding: '8px 12px',
            background: 'var(--color-surface-raised, #1e1e2e)',
            borderTop: '1px solid var(--color-border, #333)',
          }}
        >
          <div
            style={{
              fontSize: 11,
              color: 'var(--color-text-muted, #aaa)',
              marginBottom: 6,
            }}
          >
            {seq.messages.length} messages · {formatDuration(seq.durationMs)}
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {seq.messages.map((msg, i) => {
              const aIdx = anchorIndexFor(msg, seq.anchorTimestamps)
              const color = aIdx >= 0 ? STEP_COLORS[aIdx % STEP_COLORS.length] : undefined
              return (
                <div
                  key={`${msg.partition}-${msg.offset}-${i}`}
                  style={{
                    borderLeft: color ? `3px solid ${color}` : '3px solid #555',
                    paddingLeft: 8,
                  }}
                >
                  <div style={{ fontSize: 10, color: 'var(--color-text-muted, #aaa)', marginBottom: 2 }}>
                    {msg.timestamp} · {msg.topic} · p{msg.partition}@{msg.offset}
                  </div>
                  <pre
                    style={{
                      margin: 0,
                      fontSize: 11,
                      whiteSpace: 'pre-wrap',
                      wordBreak: 'break-all',
                      color: 'var(--color-text, #eee)',
                      maxHeight: 120,
                      overflow: 'auto',
                    }}
                  >
                    {msg.value ?? 'null'}
                  </pre>
                </div>
              )
            })}
          </div>
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Skeleton
// ---------------------------------------------------------------------------

function SkeletonRow() {
  return (
    <div
      style={{
        padding: '12px',
        borderBottom: '1px solid var(--color-border, #333)',
        display: 'flex',
        flexDirection: 'column',
        gap: 6,
      }}
    >
      {[80, 140].map((w) => (
        <div
          key={w}
          style={{
            height: 22,
            width: w,
            borderRadius: 11,
            background: 'var(--color-surface-raised, #2a2a3a)',
            animation: 'pulse 1.4s ease-in-out infinite',
          }}
        />
      ))}
    </div>
  )
}

// ---------------------------------------------------------------------------
// SequenceExamplesPane
// ---------------------------------------------------------------------------

export function SequenceExamplesPane() {
  const { results, loading, query } = useSequenceStore()
  const steps = query.steps

  if (loading) {
    return (
      <div style={{ flex: 1, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
        <ExamplesHeader totalMessages={null} matchRate={null} exampleCount={0} />
        <div style={{ flex: 1, overflowY: 'auto' }}>
          <SkeletonRow />
          <SkeletonRow />
          <SkeletonRow />
        </div>
      </div>
    )
  }

  if (!results || results.examples.length === 0) {
    return (
      <div style={{ flex: 1, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
        <ExamplesHeader
          totalMessages={results?.totalMessages ?? null}
          matchRate={results?.matchRate ?? null}
          exampleCount={0}
        />
        <div
          style={{
            flex: 1,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: 'var(--color-text-muted, #aaa)',
            fontSize: 13,
          }}
        >
          No sequences match — try relaxing the conditions
        </div>
      </div>
    )
  }

  return (
    <div style={{ flex: 1, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
      <ExamplesHeader
        totalMessages={results.totalMessages}
        matchRate={results.matchRate}
        exampleCount={results.examples.length}
      />
      <div style={{ flex: 1, overflowY: 'auto' }}>
        {results.examples.map((seq, i) => (
          <SequenceRow
            key={i}
            seq={seq}
            steps={steps}
            totalSteps={steps.length}
          />
        ))}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// ExamplesHeader
// ---------------------------------------------------------------------------

function ExamplesHeader({
  totalMessages,
  matchRate,
  exampleCount,
}: {
  totalMessages: number | null
  matchRate: number | null
  exampleCount: number
}) {
  const filteredPct =
    matchRate != null ? `filtered to ${Math.round(matchRate * 100)}%` : null

  return (
    <div
      style={{
        padding: '10px 12px 6px',
        borderBottom: '1px solid var(--color-border, #333)',
        display: 'flex',
        flexDirection: 'column',
        gap: 2,
      }}
    >
      <div
        style={{
          fontSize: 12,
          color: 'var(--color-text, #eee)',
          fontWeight: 500,
        }}
      >
        {totalMessages != null
          ? `${totalMessages.toLocaleString()} messages${filteredPct ? ` · ${filteredPct}` : ''}`
          : 'Loading…'}
      </div>
      {exampleCount > 0 && (
        <div style={{ fontSize: 11, color: 'var(--color-text-muted, #aaa)' }}>
          Displaying {exampleCount} example sequence{exampleCount !== 1 ? 's' : ''}
        </div>
      )}
    </div>
  )
}
