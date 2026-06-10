/**
 * SolExamplesPane — Motif-style multi-sequence result view for SOL queries.
 *
 * One row per actor (entity): the event sequence as chips, with matched tag
 * regions overlaid as coloured span labels above the chips. User tags get their
 * palette colour, the implicit Suffix region a neutral label, and unnamed gap
 * regions (the `*` wildcards between tags) a thin neutral bar. Unmatched rows
 * render as plain chips.
 */

import type { SolSequenceExample } from '../api/client'
import { SOL_NEUTRAL, type SolTagColor } from './sol-colors'

interface Props {
  examples: SolSequenceExample[]
  totalSequences: number
  matchedSequences: number
  tagColors: Record<string, SolTagColor>
}

/** A contiguous run of events covered by the same tag (or by none). */
interface Segment {
  tag: string | null
  from: number
  to: number
}

/** Partition [0, length) into contiguous segments by covering tag. */
function segment(example: SolSequenceExample): Segment[] {
  const n = example.events.length
  const cover: (string | null)[] = new Array(n).fill(null)
  // PREFIX stays uncovered (Motif renders prefix events plain); user tags win.
  for (const [name, span] of Object.entries(example.tags)) {
    if (name === 'PREFIX') continue
    for (let i = span.from; i < Math.min(span.to, n); i++) {
      if (cover[i] === null || cover[i] === 'SUFFIX') cover[i] = name
    }
  }
  const segments: Segment[] = []
  let start = 0
  for (let i = 1; i <= n; i++) {
    if (i === n || cover[i] !== cover[start]) {
      segments.push({ tag: cover[start], from: start, to: i })
      start = i
    }
  }
  return segments
}

const HEADER_H = 20

function SpanHeader({ tag, color }: { tag: string | null; color?: SolTagColor }) {
  if (!tag || !color) {
    return <div style={{ height: HEADER_H }} />
  }
  const label = tag === 'SUFFIX' ? 'Suffix' : tag
  return (
    <div style={{ height: HEADER_H, display: 'flex', flexDirection: 'column', justifyContent: 'flex-end' }}>
      <span
        style={{
          alignSelf: 'flex-start',
          background: color.strong,
          color: '#fff',
          fontFamily: 'var(--font-mono)',
          fontSize: 'var(--type-micro-size)',
          fontWeight: 600,
          padding: '1px 7px',
          borderRadius: '3px 3px 0 0',
          whiteSpace: 'nowrap',
          maxWidth: '100%',
          overflow: 'hidden',
          textOverflow: 'ellipsis',
        }}
      >
        {label}
      </span>
      <div style={{ height: 3, background: color.strong, borderRadius: '0 2px 0 0' }} />
    </div>
  )
}

/** Thin neutral bar marking the unnamed gap (`*`) region between two tags. */
function GapHeader() {
  return (
    <div style={{ height: HEADER_H, display: 'flex', flexDirection: 'column', justifyContent: 'flex-end' }}>
      <div style={{ height: 3, background: SOL_NEUTRAL.strong, opacity: 0.55, borderRadius: 2 }} />
    </div>
  )
}

function EventChip({ name, wash }: { name: string; wash?: string }) {
  return (
    <span
      style={{
        background: wash ?? 'var(--surface-raised)',
        border: '1px solid var(--rule)',
        borderRadius: 'var(--radius-xs)',
        padding: '2px 8px',
        fontFamily: 'var(--font-mono)',
        fontSize: 'var(--type-caption-size)',
        color: 'var(--ink-primary)',
        whiteSpace: 'nowrap',
      }}
    >
      {name}
    </span>
  )
}

function ExampleRow({ example, tagColors }: { example: SolSequenceExample; tagColors: Record<string, SolTagColor> }) {
  const segments = segment(example)
  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'flex-end',
        gap: 0,
        padding: '7px 0',
        borderBottom: '1px solid var(--rule)',
      }}
    >
      {/* Actor cell */}
      <div
        style={{
          width: 110,
          flexShrink: 0,
          fontFamily: 'var(--font-mono)',
          fontSize: 'var(--type-caption-size)',
          color: 'var(--ink-secondary)',
          textAlign: 'right',
          paddingRight: 14,
          paddingBottom: 4,
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }}
        title={example.entityId}
      >
        {example.entityId}
      </div>

      {/* Segments */}
      <div style={{ display: 'flex', gap: 6, alignItems: 'flex-end' }}>
        {segments.map((seg, i) => {
          const isGap = seg.tag === null && i > 0 && i < segments.length - 1
            && segments[i - 1].tag !== null && segments[i + 1].tag !== null
          const color = seg.tag ? (tagColors[seg.tag] ?? SOL_NEUTRAL) : undefined
          return (
            <div key={i} style={{ display: 'flex', flexDirection: 'column', gap: 2, minWidth: 0 }}>
              {seg.tag ? <SpanHeader tag={seg.tag} color={color} /> : isGap ? <GapHeader /> : <div style={{ height: HEADER_H }} />}
              <div style={{ display: 'flex', gap: 4 }}>
                {example.events.slice(seg.from, seg.to).map((name, j) => (
                  <EventChip key={j} name={name} wash={seg.tag ? color?.wash : undefined} />
                ))}
              </div>
            </div>
          )
        })}
        {example.truncated && (
          <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)', paddingBottom: 4 }}>…</span>
        )}
      </div>
    </div>
  )
}

export function SolExamplesPane({ examples, totalSequences, matchedSequences, tagColors }: Props) {
  if (examples.length === 0) {
    return (
      <div style={{ padding: 24, color: 'var(--ink-tertiary)', fontSize: 'var(--type-body-sm-size)' }}>
        No sequences found.
      </div>
    )
  }
  return (
    <div style={{ display: 'flex', flexDirection: 'column', minWidth: 0 }}>
      {/* Column header */}
      <div style={{ display: 'flex', borderBottom: '1px solid var(--rule)', paddingBottom: 6 }}>
        <div style={{ width: 110, flexShrink: 0, textAlign: 'right', paddingRight: 14 }}>
          <span style={headerLabel}>Actor</span>
          <div style={{ fontSize: 'var(--type-micro-size)', color: 'var(--ink-tertiary)', fontFamily: 'var(--font-mono)' }}>entity_id</div>
        </div>
        <span style={headerLabel}>Sequence</span>
      </div>

      {/* Rows — single shared horizontal scroll like Motif */}
      <div style={{ overflowX: 'auto' }}>
        {examples.map(ex => (
          <ExampleRow key={ex.entityId} example={ex} tagColors={tagColors} />
        ))}
      </div>

      {/* Footer */}
      <div
        style={{
          display: 'flex',
          gap: 8,
          alignItems: 'center',
          padding: '8px 2px',
          fontSize: 'var(--type-caption-size)',
          color: 'var(--ink-secondary)',
        }}
      >
        <strong>{totalSequences.toLocaleString()} sequences</strong>
        <span style={{ color: 'var(--ink-tertiary)' }}>›</span>
        <span>{matchedSequences.toLocaleString()} matched</span>
        <span style={{ color: 'var(--ink-tertiary)' }}>›</span>
        <span>Displaying {examples.length} example sequence{examples.length !== 1 ? 's' : ''}</span>
      </div>
    </div>
  )
}

const headerLabel: React.CSSProperties = {
  fontSize: 'var(--type-body-sm-size)',
  fontWeight: 600,
  color: 'var(--ink-primary)',
}
