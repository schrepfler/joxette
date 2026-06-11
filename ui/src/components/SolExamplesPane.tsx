/**
 * SolExamplesPane — Motif-style multi-sequence result view for SOL queries.
 *
 * One row per entity: event chips with tag-region headers overlaid.
 * Click any chip (or press Enter on it) to open a popup showing the event's
 * position, name, and tag membership.
 *
 * Keyboard nav:
 *   ↑/↓   move between entity rows
 *   ←/→   move between event chips within the focused/popup entity
 *   Enter  open popup on focused chip
 *   Esc    close popup / deselect
 */

import { useEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { useQuery } from '@tanstack/react-query'
import { cassettesApi, type EntityRecord, type SolSequenceExample } from '../api/client'
import { JsonView } from './JsonView'
import { SOL_NEUTRAL, type SolTagColor } from './sol-colors'

interface Props {
  entityType: string
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

function segment(example: SolSequenceExample): Segment[] {
  const n = example.events.length
  const cover: (string | null)[] = new Array(n).fill(null)
  for (const [name, span] of Object.entries(example.tags)) {
    if (name === 'PREFIX') continue
    for (let i = span.from; i < Math.min(span.to, n); i++) {
      if (cover[i] === null || cover[i] === 'SUFFIX') cover[i] = name
    }
  }
  const segs: Segment[] = []
  let start = 0
  for (let i = 1; i <= n; i++) {
    if (i === n || cover[i] !== cover[start]) {
      segs.push({ tag: cover[start], from: start, to: i })
      start = i
    }
  }
  return segs
}

const IMPLICIT_TAGS = new Set(['SEQ', 'MATCHED', 'PREFIX', 'SUFFIX'])

/** All user-defined tags that cover event at index i. */
function tagsAtIndex(example: SolSequenceExample, i: number): string[] {
  return Object.entries(example.tags)
    .filter(([name, span]) => !IMPLICIT_TAGS.has(name) && i >= span.from && i < span.to)
    .map(([name]) => name)
}

const HEADER_H = 20

function SpanHeader({ tag, color }: { tag: string | null; color?: SolTagColor }) {
  if (!tag || !color) return <div style={{ height: HEADER_H }} />
  const label = tag === 'SUFFIX' ? 'Suffix' : tag
  return (
    <div style={{ height: HEADER_H, display: 'flex', flexDirection: 'column', justifyContent: 'flex-end' }}>
      <span style={{
        alignSelf: 'flex-start', background: color.strong, color: '#fff',
        fontFamily: 'var(--font-mono)', fontSize: 'var(--type-micro-size)', fontWeight: 600,
        padding: '1px 7px', borderRadius: '3px 3px 0 0', whiteSpace: 'nowrap',
        maxWidth: '100%', overflow: 'hidden', textOverflow: 'ellipsis',
      }}>
        {label}
      </span>
      <div style={{ height: 3, background: color.strong, borderRadius: '0 2px 0 0' }} />
    </div>
  )
}

function GapHeader() {
  return (
    <div style={{ height: HEADER_H, display: 'flex', flexDirection: 'column', justifyContent: 'flex-end' }}>
      <div style={{ height: 3, background: SOL_NEUTRAL.strong, opacity: 0.55, borderRadius: 2 }} />
    </div>
  )
}

// ── Event chip (button) ────────────────────────────────────────────────────────

function EventChip({
  name, wash, focused, chipRef, onClick, onKeyDown,
}: {
  name: string
  wash?: string
  focused: boolean
  chipRef: (el: HTMLButtonElement | null) => void
  onClick: () => void
  onKeyDown: (e: React.KeyboardEvent) => void
}) {
  return (
    <button
      ref={chipRef}
      tabIndex={focused ? 0 : -1}
      onClick={onClick}
      onKeyDown={onKeyDown}
      style={{
        background: wash ?? 'var(--surface-raised)',
        border: focused
          ? '1px solid var(--accent)'
          : '1px solid var(--rule)',
        outline: focused ? '2px solid color-mix(in oklab, var(--accent) 35%, transparent)' : 'none',
        outlineOffset: 1,
        borderRadius: 'var(--radius-xs)',
        padding: '2px 8px',
        fontFamily: 'var(--font-mono)',
        fontSize: 'var(--type-caption-size)',
        color: 'var(--ink-primary)',
        whiteSpace: 'nowrap',
        cursor: 'pointer',
      }}
    >
      {name}
    </button>
  )
}

// ── Shared decode helper ───────────────────────────────────────────────────────

function decodeB64(s: string | null): string | null {
  if (!s) return null
  try { return atob(s.replace(/-/g, '+').replace(/_/g, '/')) } catch { return s }
}

// ── Event popup ────────────────────────────────────────────────────────────────

function EventPopup({
  entityType,
  example,
  eventIdx,
  tagColors,
  onNavigateEvent,
  onNavigateRow,
  onClose,
}: {
  entityType: string
  example: SolSequenceExample
  eventIdx: number
  tagColors: Record<string, SolTagColor>
  onNavigateEvent: (delta: number) => void
  onNavigateRow: (delta: number) => void
  onClose: () => void
}) {
  const panelRef = useRef<HTMLDivElement>(null)
  const eventName = example.events[eventIdx]
  const tags = tagsAtIndex(example, eventIdx)
  const hasPrev = eventIdx > 0
  const hasNext = eventIdx < example.events.length - 1

  // Fetch full records for this entity lazily — fires once per entity popup.
  // limit=1000 is a reasonable ceiling; most entity sequences are far smaller.
  const recordsQuery = useQuery({
    queryKey: ['sol-popup-records', entityType, example.entityId],
    queryFn: () => cassettesApi.getEntityRecords(entityType, example.entityId, { limit: 1000 }),
    staleTime: 60_000,
  })
  const record: EntityRecord | undefined = recordsQuery.data?.data[eventIdx]

  useEffect(() => { panelRef.current?.focus() }, [eventIdx])

  function handleKey(e: React.KeyboardEvent) {
    if (e.key === 'ArrowLeft')  { e.preventDefault(); onNavigateEvent(-1) }
    if (e.key === 'ArrowRight') { e.preventDefault(); onNavigateEvent(1) }
    if (e.key === 'ArrowUp')    { e.preventDefault(); onNavigateRow(-1) }
    if (e.key === 'ArrowDown')  { e.preventDefault(); onNavigateRow(1) }
    if (e.key === 'Escape')     { onClose() }
  }

  // Mini position bar
  const barCells = example.events.map((_, i) => {
    const cellTags = tagsAtIndex(example, i)
    const color = cellTags.length > 0 ? (tagColors[cellTags[0]] ?? SOL_NEUTRAL).strong : 'var(--rule)'
    return { color, isCurrent: i === eventIdx }
  })

  // Decode payload
  let parsedValue: object | null = null
  let rawValue: string | null = null
  if (record?.value) {
    try { parsedValue = JSON.parse(decodeB64(record.value) ?? '') as object }
    catch { rawValue = record.value }
  }
  const keyStr = decodeB64(record?.key ?? null)

  return createPortal(
    <div
      style={{
        position: 'fixed', inset: 0, zIndex: 200,
        background: 'rgba(0,0,0,0.45)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
      }}
      onClick={e => { if (e.target === e.currentTarget) onClose() }}
    >
      <div
        ref={panelRef}
        tabIndex={-1}
        onKeyDown={handleKey}
        style={{
          width: 'min(820px, 96vw)',
          maxHeight: '82vh',
          display: 'flex', flexDirection: 'column',
          background: 'var(--surface-paper)',
          border: '1px solid var(--rule)',
          borderTop: '3px solid var(--accent)',
          borderRadius: 'var(--radius-sm)',
          boxShadow: 'var(--shadow-lg, 0 8px 32px rgba(0,0,0,.22))',
          outline: 'none',
          overflow: 'hidden',
        }}
      >
        {/* Header */}
        <div style={{
          display: 'flex', alignItems: 'center', gap: 8,
          padding: '10px 14px', borderBottom: '1px solid var(--rule)',
          background: 'var(--surface-raised)', flexShrink: 0,
        }}>
          <button onClick={() => onNavigateEvent(-1)} disabled={!hasPrev} style={navBtn(!hasPrev)} title="Previous event (←)">‹</button>
          <span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>
            {eventIdx + 1} / {example.events.length}
          </span>
          <button onClick={() => onNavigateEvent(1)} disabled={!hasNext} style={navBtn(!hasNext)} title="Next event (→)">›</button>

          <span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-body-sm-size)', fontWeight: 600, color: 'var(--ink-primary)', marginLeft: 4 }}>
            {eventName}
          </span>
          <span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>
            {example.entityId}
          </span>

          {recordsQuery.isFetching && (
            <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>loading…</span>
          )}

          <span style={{ flex: 1 }} />
          <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>← → event · ↑↓ entity · Esc</span>
          <button onClick={onClose} style={{ ...navBtn(false), fontWeight: 700, fontSize: '1rem', marginLeft: 4 }}>✕</button>
        </div>

        {/* Body: two columns */}
        <div style={{ display: 'grid', gridTemplateColumns: '240px 1fr', overflow: 'hidden', flex: 1 }}>

          {/* Left: position bar + tags + metadata */}
          <div style={{ padding: '12px 14px', borderRight: '1px solid var(--rule)', overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: 14 }}>

            {/* Mini sequence bar */}
            <div>
              <SectionLabel>Position in sequence</SectionLabel>
              <div style={{ display: 'flex', gap: 2, alignItems: 'center', flexWrap: 'wrap' }}>
                {barCells.map((cell, i) => (
                  <div key={i} title={`${example.events[i]} (${i})`} style={{
                    width: cell.isCurrent ? 10 : 6, height: cell.isCurrent ? 20 : 12,
                    background: cell.isCurrent ? 'var(--accent)' : cell.color,
                    borderRadius: 2, opacity: cell.isCurrent ? 1 : 0.65, flexShrink: 0,
                  }} />
                ))}
                {example.truncated && <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)', marginLeft: 4 }}>…</span>}
              </div>
            </div>

            {/* Tags */}
            <div>
              <SectionLabel>Tags</SectionLabel>
              {tags.length === 0
                ? <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>not covered</span>
                : <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                    {tags.map(tag => {
                      const color = tagColors[tag] ?? SOL_NEUTRAL
                      const span = example.tags[tag]
                      return (
                        <span key={tag} style={{
                          background: color.strong, color: '#fff',
                          fontFamily: 'var(--font-mono)', fontSize: 'var(--type-caption-size)', fontWeight: 600,
                          padding: '2px 9px', borderRadius: 'var(--radius-xs)',
                        }}>
                          {tag}
                          {span && <span style={{ opacity: 0.7, fontWeight: 400, marginLeft: 5 }}>[{span.from}–{span.to - 1}]</span>}
                        </span>
                      )
                    })}
                  </div>
              }
            </div>

            {/* Metadata — only when record loaded */}
            {record && (
              <div>
                <SectionLabel>Metadata</SectionLabel>
                <MetaTable rows={[
                  ['Type',      record.messageType ?? '—'],
                  ['Topic',     record.topic],
                  ['Partition', String(record.partition)],
                  ['Offset',    String(record.offset)],
                  ['Timestamp', record.timestamp],
                  ['Recorded',  record.recordedAt],
                  ['Key',       keyStr ?? '—'],
                ]} />
                {record.headers && record.headers.length > 0 && (
                  <>
                    <SectionLabel style={{ marginTop: 10 }}>Headers</SectionLabel>
                    <MetaTable rows={record.headers.map(h => [h.key, decodeB64(h.value) ?? ''] as [string, string])} />
                  </>
                )}
              </div>
            )}
          </div>

          {/* Right: value */}
          <div style={{ padding: '12px 14px', overflowY: 'auto', minWidth: 0 }}>
            <SectionLabel>Value</SectionLabel>
            {recordsQuery.isLoading && (
              <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>Loading…</span>
            )}
            {recordsQuery.isError && (
              <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--signal-error)' }}>
                Failed to load records
              </span>
            )}
            {record && (
              parsedValue
                ? <JsonView src={parsedValue} />
                : rawValue
                  ? <pre style={{ margin: 0, fontSize: 'var(--type-caption-size)', fontFamily: 'var(--font-mono)', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>{rawValue}</pre>
                  : <span style={{ color: 'var(--ink-tertiary)', fontSize: 'var(--type-caption-size)' }}>no value</span>
            )}
            {!recordsQuery.isLoading && !record && !recordsQuery.isError && (
              <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>
                No record at index {eventIdx} — sequence may be truncated
              </span>
            )}
          </div>
        </div>
      </div>
    </div>,
    document.body,
  )
}

function SectionLabel({ children, style }: { children: React.ReactNode; style?: React.CSSProperties }) {
  return (
    <div style={{
      fontSize: 'var(--type-micro-size)', fontWeight: 700,
      letterSpacing: 'var(--type-micro-tracking)', textTransform: 'uppercase',
      color: 'var(--ink-tertiary)', marginBottom: 6, ...style,
    }}>
      {children}
    </div>
  )
}

function MetaTable({ rows }: { rows: [string, string][] }) {
  return (
    <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: 'var(--type-caption-size)' }}>
      <tbody>
        {rows.map(([label, val]) => (
          <tr key={label}>
            <td style={{ color: 'var(--ink-tertiary)', paddingRight: 10, paddingTop: 2, paddingBottom: 2, whiteSpace: 'nowrap', verticalAlign: 'top' }}>{label}</td>
            <td style={{ fontFamily: 'var(--font-mono)', color: 'var(--ink-primary)', wordBreak: 'break-all', paddingTop: 2, paddingBottom: 2 }}>{val}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

// ── Example row ────────────────────────────────────────────────────────────────

function ExampleRow({
  example,
  tagColors,
  focused,           // which event chip index is keyboard-focused (-1 = row focused, no chip)
  rowFocused,
  onChipClick,
  onRowFocus,
  chipRefs,
}: {
  example: SolSequenceExample
  tagColors: Record<string, SolTagColor>
  focused: number    // focused chip index within this row (-1 = none)
  rowFocused: boolean
  onChipClick: (eventIdx: number) => void
  onRowFocus: () => void
  chipRefs: React.MutableRefObject<(HTMLButtonElement | null)[]>
}) {
  const segments = segment(example)

  function handleChipKey(e: React.KeyboardEvent, absIdx: number) {
    if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onChipClick(absIdx) }
    // ←/→ handled by parent via popup; here we just let focus move naturally
  }

  return (
    <div
      role="row"
      aria-selected={rowFocused}
      onFocus={onRowFocus}
      style={{
        display: 'flex', alignItems: 'flex-end', gap: 0,
        padding: '7px 0', borderBottom: '1px solid var(--rule)',
        background: rowFocused ? 'color-mix(in oklab, var(--accent) 5%, transparent)' : undefined,
      }}
    >
      {/* Entity id */}
      <div style={{
        width: 110, flexShrink: 0,
        fontFamily: 'var(--font-mono)', fontSize: 'var(--type-caption-size)',
        color: rowFocused ? 'var(--accent)' : 'var(--ink-secondary)',
        fontWeight: rowFocused ? 600 : 400,
        textAlign: 'right', paddingRight: 14, paddingBottom: 4,
        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
      }} title={example.entityId}>
        {example.entityId}
      </div>

      {/* Segments */}
      <div style={{ display: 'flex', gap: 6, alignItems: 'flex-end' }}>
        {segments.map((seg, si) => {
          const isGap = seg.tag === null && si > 0 && si < segments.length - 1
            && segments[si - 1].tag !== null && segments[si + 1].tag !== null
          const color = seg.tag ? (tagColors[seg.tag] ?? SOL_NEUTRAL) : undefined
          return (
            <div key={si} style={{ display: 'flex', flexDirection: 'column', gap: 2, minWidth: 0 }}>
              {seg.tag ? <SpanHeader tag={seg.tag} color={color} /> : isGap ? <GapHeader /> : <div style={{ height: HEADER_H }} />}
              <div style={{ display: 'flex', gap: 4 }}>
                {example.events.slice(seg.from, seg.to).map((name, j) => {
                  const absIdx = seg.from + j
                  return (
                    <EventChip
                      key={j}
                      name={name}
                      wash={seg.tag ? color?.wash : undefined}
                      focused={focused === absIdx}
                      chipRef={el => { chipRefs.current[absIdx] = el }}
                      onClick={() => onChipClick(absIdx)}
                      onKeyDown={e => handleChipKey(e, absIdx)}
                    />
                  )
                })}
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

// ── Main component ─────────────────────────────────────────────────────────────

export function SolExamplesPane({ entityType, examples, totalSequences, matchedSequences, tagColors }: Props) {
  // Which row (entity) is focused
  const [focusedRow, setFocusedRow] = useState<number | null>(null)
  // Popup state: which (row, event) is open
  const [popup, setPopup] = useState<{ row: number; eventIdx: number } | null>(null)

  // chipRefs[rowIdx] = array of button refs for that row's chips
  const chipRefs = useRef<(HTMLButtonElement | null)[][]>([])

  function ensureChipRefs(rowIdx: number, count: number) {
    if (!chipRefs.current[rowIdx]) chipRefs.current[rowIdx] = new Array(count).fill(null)
  }

  function openPopup(row: number, eventIdx: number) {
    setFocusedRow(row)
    setPopup({ row, eventIdx })
  }

  function navigateEvent(delta: number) {
    if (!popup) return
    const n = examples[popup.row].events.length
    const next = Math.max(0, Math.min(n - 1, popup.eventIdx + delta))
    setPopup({ ...popup, eventIdx: next })
  }

  function navigateRow(delta: number) {
    if (!popup) return
    const nextRow = Math.max(0, Math.min(examples.length - 1, popup.row + delta))
    // keep same event index if possible
    const nextEventIdx = Math.min(popup.eventIdx, examples[nextRow].events.length - 1)
    setFocusedRow(nextRow)
    setPopup({ row: nextRow, eventIdx: nextEventIdx })
  }

  function closePopup() {
    const row = popup?.row ?? focusedRow ?? 0
    const eventIdx = popup?.eventIdx ?? 0
    setPopup(null)
    // restore focus to the chip that was open
    chipRefs.current[row]?.[eventIdx]?.focus()
  }

  function navigateRowsWithKeyboard(e: React.KeyboardEvent) {
    if (popup) return // popup handles its own keys
    if (e.key === 'ArrowDown') { e.preventDefault(); setFocusedRow(r => Math.min(examples.length - 1, (r ?? -1) + 1)) }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setFocusedRow(r => Math.max(0, (r ?? examples.length) - 1)) }
    else if (e.key === 'Escape') { setFocusedRow(null) }
  }

  if (examples.length === 0) {
    return <div style={{ padding: 24, color: 'var(--ink-tertiary)', fontSize: 'var(--type-body-sm-size)' }}>No sequences found.</div>
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', minWidth: 0 }} onKeyDown={navigateRowsWithKeyboard}>
      {/* Column header */}
      <div style={{ display: 'flex', borderBottom: '1px solid var(--rule)', paddingBottom: 6 }}>
        <div style={{ width: 110, flexShrink: 0, textAlign: 'right', paddingRight: 14 }}>
          <span style={headerLabel}>Actor</span>
          <div style={{ fontSize: 'var(--type-micro-size)', color: 'var(--ink-tertiary)', fontFamily: 'var(--font-mono)' }}>entity_id</div>
        </div>
        <span style={headerLabel}>Sequence</span>
        <span style={{ marginLeft: 'auto', fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)', alignSelf: 'flex-end' }}>
          click chip · ↑↓ entity · ←→ event
        </span>
      </div>

      {/* Rows */}
      <div style={{ overflowX: 'auto' }}>
        {examples.map((ex, ri) => {
          ensureChipRefs(ri, ex.events.length)
          return (
            <ExampleRow
              key={ex.entityId}
              example={ex}
              tagColors={tagColors}
              rowFocused={focusedRow === ri}
              focused={focusedRow === ri && popup === null ? 0 : -1}
              onRowFocus={() => setFocusedRow(ri)}
              onChipClick={eventIdx => openPopup(ri, eventIdx)}
              chipRefs={{ current: chipRefs.current[ri] ??= [] }}
            />
          )
        })}
      </div>

      {/* Footer */}
      <div style={{ display: 'flex', gap: 8, alignItems: 'center', padding: '8px 2px', fontSize: 'var(--type-caption-size)', color: 'var(--ink-secondary)' }}>
        <strong>{totalSequences.toLocaleString()} sequences</strong>
        <span style={{ color: 'var(--ink-tertiary)' }}>›</span>
        <span>{matchedSequences.toLocaleString()} matched</span>
        <span style={{ color: 'var(--ink-tertiary)' }}>›</span>
        <span>Displaying {examples.length} example{examples.length !== 1 ? 's' : ''}</span>
      </div>

      {popup !== null && (
        <EventPopup
          entityType={entityType}
          example={examples[popup.row]}
          eventIdx={popup.eventIdx}
          tagColors={tagColors}
          onNavigateEvent={navigateEvent}
          onNavigateRow={navigateRow}
          onClose={closePopup}
        />
      )}
    </div>
  )
}

// ── Styles ─────────────────────────────────────────────────────────────────────

const headerLabel: React.CSSProperties = {
  fontSize: 'var(--type-body-sm-size)', fontWeight: 600, color: 'var(--ink-primary)',
}

function navBtn(disabled: boolean): React.CSSProperties {
  return {
    padding: '2px 8px', background: 'none',
    border: '1px solid var(--rule)', borderRadius: 'var(--radius-xs)',
    cursor: disabled ? 'default' : 'pointer',
    color: disabled ? 'var(--ink-tertiary)' : 'var(--ink-primary)',
    fontFamily: 'var(--font-mono)', fontSize: '1rem', lineHeight: 1,
    opacity: disabled ? 0.4 : 1,
  }
}
