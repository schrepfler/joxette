import { useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { useMutation, useQuery } from '@tanstack/react-query'
import { cassettesApi, type EntityRecord } from '../api/client'
import { JsonView } from './JsonView'
import { SolEditor } from './SolEditor'
import { SolResultSequence } from './SolResultSequence'
import { SolSequenceInspector } from './SolSequenceInspector'
import { SolToolbar } from './SolToolbar'
import { buildTagColors, resolveTagOrder } from './sol-colors'
import { decodeB64, tryParseValue } from '../lib/encoding'

interface Props {
  mode: 'entity' | 'topic'
  entityType?: string
  entityId?: string
  topic?: string
  from?: string
  to?: string
  /** From the entity page's "By message type" sidebar — dims non-matching sequence chips. */
  highlightMessageType?: string | null
}

// ── Colour helpers ─────────────────────────────────────────────────────────────

const IMPLICIT_TAGS = new Set(['SEQ', 'MATCHED', 'PREFIX', 'SUFFIX'])

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

export function SolQueryPanel({ mode, entityType, entityId, topic, from, to, highlightMessageType }: Props) {
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

  const [popupIdx, setPopupIdx] = useState<number | null>(null)
  function navigatePopup(delta: number) {
    if (popupIdx === null || !result) return
    setPopupIdx(Math.max(0, Math.min(result.records.length - 1, popupIdx + delta)))
  }

  // Tag → colour, in pattern order. Before the first run the order comes from
  // a client-side parse of the match clause; once a result exists, from the
  // result's own tag names — so editing the query afterward without re-running
  // can't desync the displayed result's colours from a still-executed run's.
  const resultTagNames = result?.tags
    ? Object.keys(result.tags).filter(name => !IMPLICIT_TAGS.has(name))
    : undefined
  const tagColors = useMemo(
    () => buildTagColors(resolveTagOrder(query, resultTagNames)),
    [query, resultTagNames],
  )

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

      {/* ── Error ─────────────────────────────────────────────────────── */}
      {mutation.error && (
        <div style={{ padding: '8px 12px', background: 'color-mix(in oklab, var(--signal-error) 10%, transparent)', border: '1px solid color-mix(in oklab, var(--signal-error) 30%, transparent)', borderRadius: 'var(--radius-sm)', fontSize: 'var(--type-body-sm-size)', color: 'var(--signal-error)', fontFamily: 'var(--font-mono)' }}>
          {(mutation.error as Error).message}
        </div>
      )}

      {/* ── Editor + coverage stats | matched events ─────────────────── */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: hasResult ? 'minmax(340px, 480px) 1fr' : '1fr',
          gap: 16,
          alignItems: 'start',
        }}
      >
        <div style={{ minWidth: 0, display: 'flex', flexDirection: 'column', gap: 8 }}>
          <SolEditor
            value={query}
            onChange={setQuery}
            onRun={() => mutation.mutate()}
            messageTypes={messageTypes}
            fieldPaths={fieldPaths}
            tagColors={tagColors}
            minHeight={120}
            disabled={mutation.isPending}
          />

          {hasResult && (
            <>
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
                  tagColors={tagColors}
                  selectedTags={selectedTags}
                  onTagToggle={toggleTag}
                />
              )}
            </>
          )}
        </div>

        {/* Matched events — the sequence timeline, alongside the editor rather than below it */}
        {hasResult && result.records.length > 0 && (
          <div style={{ minWidth: 0 }}>
            <SolResultSequence
              records={result.records}
              tags={result.tags}
              tagColors={tagColors}
              onOpenEvent={setPopupIdx}
              highlightMessageType={highlightMessageType}
            />
          </div>
        )}
      </div>

      {hasResult && popupIdx !== null && (
        <MessagePopup
          records={result.records}
          index={popupIdx}
          onNavigate={navigatePopup}
          onClose={() => setPopupIdx(null)}
        />
      )}
    </div>
  )
}

// ── Message popup ──────────────────────────────────────────────────────────────

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
    const result = tryParseValue(r.value)
    if (result) parsedValue = result.parsed as object
    else rawValue = r.value
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

