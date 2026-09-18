/**
 * SolResultSequence — a single SOL match result rendered as one horizontally
 * virtualized row of event chips, grouped into coloured tag-span headers.
 *
 * Same visual language as SolExamplesPane's per-entity rows, but for exactly
 * one sequence (a single entity's full match, or a topic match) which can be
 * thousands of events long — virtualized with @tanstack/react-virtual so only
 * the chips actually on screen are mounted, with dynamic per-chip widths via
 * measureElement (event names vary a lot in length).
 *
 * Two display modes:
 *   'all'          — every event in the sequence, as a chip
 *   'tagged-only'  — untagged runs collapsed into a single "+N untagged" marker,
 *                    for scanning a long sequence where only the matched region matters
 *
 * `highlightMessageType` (from the entity page's "By message type" sidebar,
 * DatasetSummaryPanel) dims every chip whose event name doesn't match — a
 * visual filter, not a data one: chip positions and tag-span math are index-
 * based against the original sequence, so removing events would desync both.
 */

import { useRef, useState } from 'react'
import { useVirtualizer } from '@tanstack/react-virtual'
import type { EntityRecord, SolTagSpan } from '../api/client'
import { EventChip, SpanHeader, GapHeader, HEADER_H } from './SolExamplesPane'
import { SOL_NEUTRAL, type SolTagColor } from './sol-colors'
import { segmentSequence, flattenSequence } from './sol-sequence-segments'

interface Props {
  records: EntityRecord[]
  tags: Record<string, SolTagSpan>
  tagColors: Record<string, SolTagColor>
  onOpenEvent: (index: number) => void
  /** Dims chips whose event name isn't this one. `null`/undefined = no dimming. */
  highlightMessageType?: string | null
}

const CHIP_ROW_HEIGHT = HEADER_H + 28
const DIMMED_OPACITY = 0.3

export function SolResultSequence({ records, tags, tagColors, onOpenEvent, highlightMessageType }: Props) {
  const [mode, setMode] = useState<'all' | 'tagged-only'>('all')
  const [focusedPos, setFocusedPos] = useState<number | null>(null)

  const events = records.map(r => r.messageType ?? '—')
  const segments = segmentSequence(events.length, tags)
  const items = flattenSequence(events, segments, mode)

  const scrollRef = useRef<HTMLDivElement>(null)
  const virtualizer = useVirtualizer({
    count: items.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => 90,
    horizontal: true,
    overscan: 20,
  })

  function eventPositions(): number[] {
    const positions: number[] = []
    items.forEach((it, i) => { if (it.kind === 'event') positions.push(i) })
    return positions
  }

  function moveFocus(delta: number) {
    const positions = eventPositions()
    if (positions.length === 0) return
    const currentIdx = focusedPos === null ? -1 : positions.indexOf(focusedPos)
    const nextIdx = Math.max(0, Math.min(positions.length - 1, currentIdx + delta))
    const nextPos = positions[nextIdx]
    setFocusedPos(nextPos)
    virtualizer.scrollToIndex(nextPos, { align: 'auto' })
  }

  function switchMode(next: 'all' | 'tagged-only') {
    setMode(next)
    setFocusedPos(null)
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8, minWidth: 0 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <span style={{ fontSize: 'var(--type-body-sm-size)', fontWeight: 600, color: 'var(--ink-primary)' }}>
          Sequence
        </span>
        <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>
          {events.length.toLocaleString()} event{events.length !== 1 ? 's' : ''}
        </span>
        <span style={{ flex: 1 }} />
        <button
          data-testid="btn-sequence-mode-toggle"
          onClick={() => switchMode(mode === 'all' ? 'tagged-only' : 'all')}
          style={toggleBtnStyle}
        >
          {mode === 'all' ? 'Show tagged only' : 'Show all events'}
        </button>
      </div>

      <div
        ref={scrollRef}
        style={{
          overflowX: 'auto', overflowY: 'hidden',
          border: '1px solid var(--rule)', borderRadius: 'var(--radius-sm)',
          padding: '8px 10px',
        }}
        onKeyDown={e => {
          if (e.key === 'ArrowLeft') { e.preventDefault(); moveFocus(-1) }
          else if (e.key === 'ArrowRight') { e.preventDefault(); moveFocus(1) }
        }}
      >
        {items.length === 0
          ? <div style={{ padding: '8px 0', color: 'var(--ink-tertiary)', fontSize: 'var(--type-body-sm-size)' }}>No events.</div>
          : (
            <div style={{ position: 'relative', height: CHIP_ROW_HEIGHT, width: virtualizer.getTotalSize() }}>
              {virtualizer.getVirtualItems().map(vi => {
                const item = items[vi.index]
                return (
                  <div
                    key={vi.key}
                    data-index={vi.index}
                    ref={virtualizer.measureElement}
                    style={{ position: 'absolute', top: 0, left: 0, transform: `translateX(${vi.start}px)`, paddingRight: 4 }}
                  >
                    {item.kind === 'gap' ? (
                      <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                        <GapHeader />
                        <span style={{
                          display: 'inline-block', padding: '2px 8px',
                          fontSize: 'var(--type-caption-size)', fontFamily: 'var(--font-mono)', fontStyle: 'italic',
                          color: 'var(--ink-tertiary)', border: '1px dashed var(--rule)', borderRadius: 'var(--radius-xs)',
                          whiteSpace: 'nowrap',
                        }}>
                          +{(item.to - item.from).toLocaleString()} untagged
                        </span>
                      </div>
                    ) : (
                      <div style={{
                        display: 'flex', flexDirection: 'column', gap: 2,
                        opacity: highlightMessageType && item.name !== highlightMessageType ? DIMMED_OPACITY : 1,
                        transition: 'opacity 120ms',
                      }}>
                        {item.isSegmentStart
                          ? <SpanHeader tag={item.segmentTag} color={item.segmentTag ? (tagColors[item.segmentTag] ?? SOL_NEUTRAL) : undefined} />
                          : <div style={{ height: HEADER_H }} />}
                        <EventChip
                          name={item.name}
                          wash={item.segmentTag ? (tagColors[item.segmentTag] ?? SOL_NEUTRAL).wash : undefined}
                          focused={focusedPos === vi.index}
                          chipRef={() => {}}
                          onClick={() => { setFocusedPos(vi.index); onOpenEvent(item.index) }}
                          onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onOpenEvent(item.index) } }}
                        />
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          )}
      </div>
      <div style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>
        ←→ navigate · Enter / click to inspect
      </div>
    </div>
  )
}

const toggleBtnStyle: React.CSSProperties = {
  padding: '3px 10px', background: 'transparent', color: 'var(--ink-secondary)',
  border: '1px solid var(--rule)', borderRadius: 'var(--radius-sm)', cursor: 'pointer',
  fontFamily: 'var(--font-body)', fontSize: 'var(--type-caption-size)', whiteSpace: 'nowrap',
}
