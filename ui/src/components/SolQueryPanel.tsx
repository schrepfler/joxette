import { useEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { useMutation, useQuery } from '@tanstack/react-query'
import { cassettesApi, type EntityRecord } from '../api/client'
import { JsonView } from './JsonView'
import { SolEditor } from './SolEditor'
import { SolSequenceInspector } from './SolSequenceInspector'
import { SolToolbar } from './SolToolbar'

interface Props {
  mode: 'entity' | 'topic'
  entityType?: string
  entityId?: string
  topic?: string
  from?: string
  to?: string
}

// ── Colour helpers ─────────────────────────────────────────────────────────────

/** Stable hue from a string via djb2 — same event type always gets same colour */
function eventHue(name: string): number {
  let h = 5381
  for (let i = 0; i < name.length; i++) h = (h * 33) ^ name.charCodeAt(i)
  return Math.abs(h) % 360
}
function eventPill(name: string): React.CSSProperties {
  const hue = eventHue(name)
  return {
    background: `hsl(${hue}, 55%, 88%)`,
    color: `hsl(${hue}, 50%, 30%)`,
    border: `1px solid hsl(${hue}, 45%, 75%)`,
    borderRadius: 'var(--radius-xs)',
    padding: '1px 7px',
    fontSize: 'var(--type-caption-size)',
    fontFamily: 'var(--font-mono)',
    whiteSpace: 'nowrap' as const,
  }
}

// ── Main component ─────────────────────────────────────────────────────────────

export function SolQueryPanel({ mode, entityType, entityId, topic, from, to }: Props) {
  const [query, setQuery] = useState(
    'match A(event_name) >> * >> B(other_event)\nif duration(A, B) < 5min',
  )
  const [selectedTags, setSelectedTags] = useState<Set<string>>(new Set())
  const [typeField, setTypeField] = useState('')

  function toggleTag(name: string) {
    setSelectedTags(prev => {
      const next = new Set(prev)
      if (next.has(name)) next.delete(name)
      else next.add(name)
      return next
    })
  }

  // Fetch field paths ($.value.xxx, $.key, …) for SET/FILTER/IF autocompletion
  const fieldsQuery = useQuery({
    queryKey: ['fields', mode, entityType ?? topic],
    queryFn: () =>
      mode === 'entity' && entityType
        ? cassettesApi.getEntityFields(entityType)
        : topic
          ? cassettesApi.getTopicFields(topic)
          : Promise.resolve([]),
    staleTime: 300_000,
    enabled: !!(entityType || topic),
  })
  const fieldPaths = fieldsQuery.data ?? []

  // Fetch distinct message_type names for MATCH autocompletion
  const messageTypesQuery = useQuery({
    queryKey: ['message-types', mode, entityType ?? topic],
    queryFn: () =>
      mode === 'entity' && entityType
        ? cassettesApi.getEntityMessageTypes(entityType)
        : topic
          ? cassettesApi.getTopicMessageTypes(topic)
          : Promise.resolve([]),
    staleTime: 300_000,
    enabled: !!(entityType || topic),
  })
  const messageTypes = messageTypesQuery.data ?? []

  const mutation = useMutation({
    mutationFn: () => {
      if (mode === 'entity' && entityType && entityId) {
        return cassettesApi.solMatchEntity(entityType, entityId, query, from, to)
      }
      if (mode === 'topic' && topic) {
        return cassettesApi.solMatchTopic(topic, query, from, to, typeField.trim() || undefined)
      }
      return Promise.reject(new Error('Missing entity/topic params'))
    },
    onSuccess: () => setSelectedTags(new Set()),
  })

  const result = mutation.data
  const hasResult = !!result

  // Build an index set of which record positions are covered by selected tags
  const filteredRecords = (() => {
    if (!result || selectedTags.size === 0 || !result.tags) return result?.records ?? []
    const covered = new Set<number>()
    for (const name of selectedTags) {
      const span = result.tags[name]
      if (span) {
        for (let i = span.from; i < span.to; i++) covered.add(i)
      }
    }
    return result.records.filter((_, i) => covered.has(i))
  })()

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>

      {/* ── Toolbar ───────────────────────────────────────────────────── */}
      <SolToolbar
        onRun={() => mutation.mutate()}
        isPending={mutation.isPending}
        messageTypes={messageTypes}
        fieldPaths={fieldPaths}
        onQueryChange={setQuery}
      >
        {/* Type field — only shown for topic mode where message_type may be null */}
        {mode === 'topic' && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)', whiteSpace: 'nowrap' }}>
              type field
            </span>
            <input
              value={typeField}
              onChange={e => setTypeField(e.target.value)}
              placeholder="e.g. type"
              autoComplete="off"
              autoCorrect="off"
              autoCapitalize="off"
              spellCheck={false}
              style={{
                width: 110,
                padding: '3px 7px',
                border: '1px solid var(--rule)',
                borderRadius: 'var(--radius-xs)',
                fontFamily: 'var(--font-mono)',
                fontSize: 'var(--type-caption-size)',
                color: 'var(--ink-primary)',
                background: typeField ? 'color-mix(in oklab, var(--accent) 8%, transparent)' : 'var(--surface-paper)',
              }}
              title="JSON field path to use as event name when message_type is null (e.g. type, eventType)"
            />
          </div>
        )}
      </SolToolbar>

      {/* ── CodeMirror SOL editor ─────────────────────────────────────── */}
      <SolEditor
        value={query}
        onChange={setQuery}
        onRun={() => mutation.mutate()}
        messageTypes={messageTypes}
        fieldPaths={fieldPaths}
        minHeight={120}
        disabled={mutation.isPending}
      />

      {/* ── Error ─────────────────────────────────────────────────────── */}
      {mutation.error && (
        <div style={{ padding: '8px 12px', background: 'color-mix(in oklab, var(--signal-error) 10%, transparent)', border: '1px solid color-mix(in oklab, var(--signal-error) 30%, transparent)', borderRadius: 'var(--radius-sm)', fontSize: 'var(--type-body-sm-size)', color: 'var(--signal-error)', fontFamily: 'var(--font-mono)' }}>
          {(mutation.error as Error).message}
        </div>
      )}

      {/* ── Result ────────────────────────────────────────────────────── */}
      {hasResult && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>

          {/* Status bar */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 12, padding: '6px 12px', background: 'var(--surface-raised)', border: '1px solid var(--rule)', borderRadius: 'var(--radius-sm)', fontSize: 'var(--type-body-sm-size)' }}>
            <span style={{ fontWeight: 600, color: result.matched ? 'var(--signal-success)' : 'var(--ink-tertiary)' }}>
              {result.matched ? '✓ Matched' : '○ No match'}
            </span>
            <span style={{ color: 'var(--ink-secondary)' }}>
              {selectedTags.size > 0
                ? <>{filteredRecords.length.toLocaleString()} <span style={{ color: 'var(--ink-tertiary)' }}>/ {result.records.length.toLocaleString()}</span> event{result.records.length !== 1 ? 's' : ''}</>
                : <>{result.records.length.toLocaleString()} event{result.records.length !== 1 ? 's' : ''}</>
              }
            </span>
            {result.unexpectedNulls.length > 0 && (
              <span title={result.unexpectedNulls.join('\n')} style={{ color: 'var(--signal-warn-ink)', cursor: 'help' }}>
                ⚠ {result.unexpectedNulls.length} null{result.unexpectedNulls.length !== 1 ? 's' : ''}
              </span>
            )}
          </div>

          {/* Sequence inspector — tag coverage bars */}
          {result.tags && result.sequenceLength > 0 && Object.keys(result.tags).length > 0 && (
            <SolSequenceInspector
              tags={result.tags}
              sequenceLength={result.sequenceLength}
              selectedTags={selectedTags}
              onTagToggle={toggleTag}
            />
          )}

          {/* Matched events table */}
          {filteredRecords.length > 0 && (
            <SolResultTable records={filteredRecords} />
          )}
        </div>
      )}
    </div>
  )
}

// ── Message popup ──────────────────────────────────────────────────────────────

function decodeB64(s: string | null): string | null {
  if (!s) return null
  try { return atob(s.replace(/-/g, '+').replace(/_/g, '/')) } catch { return s }
}

function MessagePopup({
  records,
  index,
  onNavigate,   // delta -1 / +1 within sequence (←/→)
  onClose,
}: {
  records: EntityRecord[]
  index: number
  onNavigate: (delta: number) => void
  onClose: () => void
}) {
  const panelRef = useRef<HTMLDivElement>(null)
  const r = records[index]
  const hasPrev = index > 0
  const hasNext = index < records.length - 1

  // Focus the panel on open so keyboard events land here
  useEffect(() => { panelRef.current?.focus() }, [index])

  // Decode value
  let parsedValue: object | null = null
  let rawValue: string | null = null
  if (r.value) {
    try { parsedValue = JSON.parse(decodeB64(r.value) ?? '') as object }
    catch { rawValue = r.value }
  }
  const keyStr = decodeB64(r.key)

  function handleKey(e: React.KeyboardEvent) {
    if (e.key === 'ArrowLeft')  { e.preventDefault(); onNavigate(-1) }
    if (e.key === 'ArrowRight') { e.preventDefault(); onNavigate(1) }
    if (e.key === 'Escape')     { onClose() }
  }

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
          width: 'min(780px, 96vw)',
          maxHeight: '80vh',
          display: 'flex',
          flexDirection: 'column',
          background: 'var(--surface-paper)',
          border: '1px solid var(--rule)',
          borderTop: '3px solid var(--accent)',
          borderRadius: 'var(--radius-sm)',
          boxShadow: 'var(--shadow-lg, 0 8px 32px rgba(0,0,0,.22))',
          overflow: 'hidden',
          outline: 'none',
        }}
      >
        {/* Header */}
        <div style={{
          display: 'flex', alignItems: 'center', gap: 8,
          padding: '10px 14px',
          borderBottom: '1px solid var(--rule)',
          background: 'var(--surface-raised)',
          flexShrink: 0,
        }}>
          <button onClick={() => onNavigate(-1)} disabled={!hasPrev} style={popupNavBtn(!hasPrev)} title="Previous message (←)">‹</button>
          <span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>
            {index + 1} / {records.length}
          </span>
          <button onClick={() => onNavigate(1)} disabled={!hasNext} style={popupNavBtn(!hasNext)} title="Next message (→)">›</button>

          <span style={{ marginLeft: 8 }}>
            {r.messageType
              ? <span style={eventPill(r.messageType)}>{r.messageType}</span>
              : <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>no type</span>}
          </span>
          <span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-caption-size)', color: 'var(--ink-secondary)' }}>
            {r.timestamp.slice(0, 19).replace('T', ' ')}
          </span>

          <span style={{ flex: 1 }} />
          <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>← → navigate · Esc close</span>
          <button onClick={onClose} style={{ ...popupNavBtn(false), marginLeft: 4, fontWeight: 700, fontSize: '1rem' }}>✕</button>
        </div>

        {/* Body: split meta / value */}
        <div style={{ display: 'grid', gridTemplateColumns: '240px 1fr', overflow: 'hidden', flex: 1 }}>

          {/* Left: metadata + headers */}
          <div style={{ padding: '12px 14px', borderRight: '1px solid var(--rule)', overflowY: 'auto' }}>
            <SectionLabel>Metadata</SectionLabel>
            <MetaTable rows={[
              ['Type',       r.messageType ?? '—'],
              ['Topic',      r.topic],
              ['Partition',  String(r.partition)],
              ['Offset',     String(r.offset)],
              ['Timestamp',  r.timestamp],
              ['Recorded',   r.recordedAt],
              ['Entity',     r.entityId],
              ['Key',        keyStr ?? '—'],
            ]} />
            {r.headers && r.headers.length > 0 && (
              <>
                <SectionLabel style={{ marginTop: 12 }}>Headers</SectionLabel>
                <MetaTable rows={r.headers.map(h => [h.key, decodeB64(h.value) ?? ''])} />
              </>
            )}
          </div>

          {/* Right: value */}
          <div style={{ padding: '12px 14px', overflowY: 'auto', minWidth: 0 }}>
            <SectionLabel>Value</SectionLabel>
            {parsedValue
              ? <JsonView src={parsedValue} />
              : rawValue
                ? <pre style={{ margin: 0, fontSize: 'var(--type-caption-size)', fontFamily: 'var(--font-mono)', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>{rawValue}</pre>
                : <span style={{ color: 'var(--ink-tertiary)', fontSize: 'var(--type-caption-size)' }}>no value</span>}
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
            <td style={{ color: 'var(--ink-tertiary)', paddingRight: 10, paddingTop: 3, paddingBottom: 3, whiteSpace: 'nowrap', verticalAlign: 'top' }}>{label}</td>
            <td style={{ fontFamily: 'var(--font-mono)', color: 'var(--ink-primary)', wordBreak: 'break-all', paddingTop: 3, paddingBottom: 3 }}>{val}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function popupNavBtn(disabled: boolean): React.CSSProperties {
  return {
    padding: '2px 8px', background: 'none',
    border: '1px solid var(--rule)', borderRadius: 'var(--radius-xs)',
    cursor: disabled ? 'default' : 'pointer',
    color: disabled ? 'var(--ink-tertiary)' : 'var(--ink-primary)',
    fontFamily: 'var(--font-mono)', fontSize: '1rem', lineHeight: 1,
    opacity: disabled ? 0.4 : 1,
  }
}

// ── Result table ───────────────────────────────────────────────────────────────

function SolResultTable({ records }: { records: EntityRecord[] }) {
  const [popupIdx, setPopupIdx] = useState<number | null>(null)
  const [focusedIdx, setFocusedIdx] = useState<number | null>(null)
  const rowRefs = useRef<(HTMLTableRowElement | null)[]>([])

  function navigateRow(delta: number) {
    const n = records.length
    if (n === 0) return
    const next = focusedIdx === null
      ? (delta > 0 ? 0 : n - 1)
      : Math.max(0, Math.min(n - 1, focusedIdx + delta))
    setFocusedIdx(next)
    rowRefs.current[next]?.focus()
    rowRefs.current[next]?.scrollIntoView({ block: 'nearest' })
  }

  function openPopup(i: number) {
    setPopupIdx(i)
    setFocusedIdx(i)
  }

  function navigatePopup(delta: number) {
    if (popupIdx === null) return
    const next = Math.max(0, Math.min(records.length - 1, popupIdx + delta))
    setPopupIdx(next)
    setFocusedIdx(next)
  }

  return (
    <>
      <div
        style={{ border: '1px solid var(--rule)', borderRadius: 'var(--radius-sm)', overflow: 'hidden' }}
        onKeyDown={e => {
          if (e.key === 'ArrowDown') { e.preventDefault(); navigateRow(1) }
          else if (e.key === 'ArrowUp') { e.preventDefault(); navigateRow(-1) }
        }}
      >
        <table aria-label="SOL query results" style={{ width: '100%', borderCollapse: 'collapse', fontSize: 'var(--type-body-sm-size)' }}>
          <thead>
            <tr style={{ background: 'var(--surface-raised)' }}>
              {['Timestamp', 'Type', 'Topic', 'Partition / Offset', ''].map((h, i) => (
                <th key={i} style={thSt}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {records.map((r, i) => {
              const isFocused = focusedIdx === i
              return (
                <tr
                  key={`${r.partition}-${r.offset}`}
                  ref={el => { rowRefs.current[i] = el }}
                  tabIndex={0}
                  style={{
                    cursor: 'pointer',
                    background: isFocused ? 'color-mix(in oklab, var(--accent) 8%, transparent)' : undefined,
                    outline: isFocused ? '2px solid color-mix(in oklab, var(--accent) 35%, transparent)' : 'none',
                    outlineOffset: '-2px',
                  }}
                  onClick={() => openPopup(i)}
                  onFocus={() => setFocusedIdx(i)}
                  onKeyDown={e => {
                    if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); openPopup(i) }
                    else if (e.key === 'ArrowDown') { e.preventDefault(); navigateRow(1) }
                    else if (e.key === 'ArrowUp') { e.preventDefault(); navigateRow(-1) }
                  }}
                  onMouseEnter={e => { if (!isFocused) e.currentTarget.style.background = 'var(--surface-raised)' }}
                  onMouseLeave={e => { if (!isFocused) e.currentTarget.style.background = '' }}
                >
                  <td style={tdSt}><span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-caption-size)' }}>{r.timestamp.slice(0, 19).replace('T', ' ')}</span></td>
                  <td style={tdSt}>{r.messageType ? <span style={eventPill(r.messageType)}>{r.messageType}</span> : <span style={{ color: 'var(--ink-tertiary)' }}>—</span>}</td>
                  <td style={tdSt}><span style={{ color: 'var(--ink-secondary)' }}>{r.topic}</span></td>
                  <td style={tdSt}><span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>{r.partition} / {r.offset}</span></td>
                  <td style={{ ...tdSt, color: 'var(--accent)', fontSize: 'var(--type-caption-size)', textAlign: 'right' }}>▸</td>
                </tr>
              )
            })}
          </tbody>
        </table>
        <div style={{ padding: '5px 12px', background: 'var(--surface-raised)', borderTop: '1px solid var(--rule)', fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>
          ↑↓ navigate · Enter / click to inspect
        </div>
      </div>

      {popupIdx !== null && (
        <MessagePopup
          records={records}
          index={popupIdx}
          onNavigate={navigatePopup}
          onClose={() => {
            setPopupIdx(null)
            rowRefs.current[focusedIdx ?? 0]?.focus()
          }}
        />
      )}
    </>
  )
}

// ── Micro styles ───────────────────────────────────────────────────────────────

const thSt: React.CSSProperties = {
  textAlign: 'left', padding: '8px 12px',
  fontSize: 'var(--type-micro-size)', fontWeight: 700,
  letterSpacing: 'var(--type-micro-tracking)', textTransform: 'uppercase',
  color: 'var(--ink-tertiary)', borderBottom: '1px solid var(--rule)',
}

const tdSt: React.CSSProperties = {
  padding: '8px 12px', borderBottom: '1px solid var(--rule)',
  verticalAlign: 'middle',
}
