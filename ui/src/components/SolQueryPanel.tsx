import { useState } from 'react'
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

// ── Result table ───────────────────────────────────────────────────────────────

function SolResultTable({ records }: { records: EntityRecord[] }) {
  const [expandedIdx, setExpandedIdx] = useState<number | null>(null)

  return (
    <div style={{ border: '1px solid var(--rule)', borderRadius: 'var(--radius-sm)', overflow: 'hidden' }}>
      <table aria-label="SOL query results" style={{ width: '100%', borderCollapse: 'collapse', fontSize: 'var(--type-body-sm-size)' }}>
        <thead>
          <tr style={{ background: 'var(--surface-raised)' }}>
            {['Timestamp', 'Type', 'Topic', 'Offset', 'Value'].map(h => (
              <th key={h} style={thSt}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {records.map((r, i) => (
            <>
              <tr
                key={`${r.partition}-${r.offset}`}
                tabIndex={r.value ? 0 : undefined}
                aria-expanded={r.value ? expandedIdx === i : undefined}
                style={{ cursor: r.value ? 'pointer' : 'default', background: expandedIdx === i ? 'var(--accent-wash)' : undefined }}
                onClick={() => r.value && setExpandedIdx(expandedIdx === i ? null : i)}
                onKeyDown={r.value ? (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); setExpandedIdx(expandedIdx === i ? null : i) } } : undefined}
                onMouseEnter={e => { if (expandedIdx !== i) e.currentTarget.style.background = 'var(--surface-raised)' }}
                onMouseLeave={e => { if (expandedIdx !== i) e.currentTarget.style.background = '' }}
              >
                <td style={tdSt}><span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-caption-size)' }}>{r.timestamp.slice(0, 19).replace('T', ' ')}</span></td>
                <td style={tdSt}>{r.messageType ? <span style={eventPill(r.messageType)}>{r.messageType}</span> : <span style={{ color: 'var(--ink-tertiary)' }}>—</span>}</td>
                <td style={tdSt}><span style={{ color: 'var(--ink-secondary)' }}>{r.topic}</span></td>
                <td style={tdSt}><span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>{r.offset}</span></td>
                <td style={tdSt}>
                  {r.value ? (
                    <span style={{ color: 'var(--accent)', fontSize: 'var(--type-caption-size)' }}>
                      {expandedIdx === i ? '▾ collapse' : '▸ expand'}
                    </span>
                  ) : <span style={{ color: 'var(--ink-tertiary)' }}>—</span>}
                </td>
              </tr>
              {expandedIdx === i && r.value && (
                <tr key={`${r.partition}-${r.offset}-exp`}>
                  <td colSpan={5} style={{ padding: '8px 12px', background: 'var(--surface-raised)', borderBottom: '1px solid var(--rule)' }}>
                    <ValueExpanded raw={r.value} />
                  </td>
                </tr>
              )}
            </>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function ValueExpanded({ raw }: { raw: string }) {
  try {
    const bytes = atob(raw.replace(/-/g, '+').replace(/_/g, '/'))
    const parsed = JSON.parse(bytes)
    return <JsonView src={parsed as object} />
  } catch {
    return <pre style={{ margin: 0, fontSize: 'var(--type-caption-size)', fontFamily: 'var(--font-mono)', overflowX: 'auto' }}>{raw}</pre>
  }
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
