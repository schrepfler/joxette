import { useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import { cassettesApi, type EntityRecord, type SolMatchResponse } from '../api/client'
import { JsonView } from './JsonView'
import { SolEditor } from './SolEditor'
import { SolSequenceInspector } from './SolSequenceInspector'

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

// ── SOL recipe library ─────────────────────────────────────────────────────────

const RECIPES: { label: string; sol: string }[] = [
  {
    label: 'Simple funnel (A → B)',
    sol: 'match A(event_a) >> * >> B(event_b)',
  },
  {
    label: 'Funnel with time constraint',
    sol: 'match A(event_a) >> * >> B(event_b)\nif duration(A, B) < 5min',
  },
  {
    label: 'Sessionize (gap > 30 min)',
    sol: 'match split Session()+\nif duration(Session[-1], SUFFIX[0]) > 30min',
  },
  {
    label: 'Filter to matched only',
    sol: 'match Target(event_name)\nfilter MATCHED',
  },
  {
    label: 'Count occurrences of event',
    sol: 'match split E(event_name)\ncombine count = max(split_index)',
  },
  {
    label: 'Remove duplicate consecutive events',
    sol: 'match split A(event_name){2,}\nreplace A with A[-1]\ncombine',
  },
  {
    label: 'Keep events before a target',
    sol: 'match start >> PREFIX()* >> Target(event_name)\nreplace SEQ with PREFIX >> Target',
  },
  {
    label: 'Compute duration between two events',
    sol: 'match A(event_a) >> * >> B(event_b)\nset time_between = duration(A, B)\nfilter MATCHED',
  },
  {
    label: 'Last-touch attribution',
    sol: 'match LastTouch(event_a | event_b | event_c) >>\n  (^event_a, ^event_b, ^event_c)* >> outcome_event',
  },
  {
    label: 'Exclude sequences containing event',
    sol: 'match start >> (^unwanted_event)* >> end\nfilter MATCHED',
  },
]

// ── Main component ─────────────────────────────────────────────────────────────

export function SolQueryPanel({ mode, entityType, entityId, topic, from, to }: Props) {
  const [query, setQuery] = useState(
    'match A(event_name) >> * >> B(other_event)\nif duration(A, B) < 5min',
  )
  const [recipesOpen, setRecipesOpen] = useState(false)
  const [selectedTags, setSelectedTags] = useState<Set<string>>(new Set())

  function toggleTag(name: string) {
    setSelectedTags(prev => {
      const next = new Set(prev)
      if (next.has(name)) next.delete(name)
      else next.add(name)
      return next
    })
  }

  // Fetch event-name vocabulary for autocomplete
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
  const eventNames = fieldsQuery.data ?? []

  const mutation = useMutation({
    mutationFn: () => {
      if (mode === 'entity' && entityType && entityId) {
        return cassettesApi.solMatchEntity(entityType, entityId, query, from, to)
      }
      if (mode === 'topic' && topic) {
        return cassettesApi.solMatchTopic(topic, query, from, to)
      }
      return Promise.reject(new Error('Missing entity/topic params'))
    },
    onSuccess: () => setSelectedTags(new Set()),
  })

  function applyRecipe(sol: string) {
    setQuery(sol)
    setRecipesOpen(false)
  }

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

      {/* ── Editor header ─────────────────────────────────────────────── */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
        <span style={{ fontSize: 'var(--type-body-sm-size)', fontWeight: 600, color: 'var(--ink-primary)', marginRight: 4 }}>
          SOL Query
        </span>

        {/* Recipes dropdown */}
        <div style={{ position: 'relative' }}>
          <button
            style={secondaryBtnSm}
            onClick={() => setRecipesOpen(o => !o)}
          >
            ☰ Recipes ▾
          </button>
          {recipesOpen && (
            <div
              style={{
                position: 'absolute', top: '100%', left: 0, zIndex: 50,
                background: 'var(--surface-paper)',
                border: '1px solid var(--rule)',
                borderRadius: 'var(--radius-sm)',
                boxShadow: 'var(--shadow-md)',
                minWidth: 260, marginTop: 4,
              }}
            >
              {RECIPES.map(r => (
                <button
                  key={r.label}
                  onClick={() => applyRecipe(r.sol)}
                  style={{
                    display: 'block', width: '100%', textAlign: 'left',
                    padding: '8px 14px',
                    background: 'none', border: 'none', cursor: 'pointer',
                    fontSize: 'var(--type-body-sm-size)',
                    color: 'var(--ink-primary)',
                    fontFamily: 'var(--font-body)',
                  }}
                  onMouseEnter={e => (e.currentTarget.style.background = 'var(--ink-wash)')}
                  onMouseLeave={e => (e.currentTarget.style.background = 'none')}
                >
                  {r.label}
                </button>
              ))}
            </div>
          )}
        </div>

        <div style={{ flex: 1 }} />

        {eventNames.length > 0 && (
          <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>
            {eventNames.length} event types · Ctrl+Space for autocomplete
          </span>
        )}
        <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>
          ⌘↵ to run
        </span>
        <button
          style={{ ...primaryBtnSm, minWidth: 72 }}
          disabled={mutation.isPending}
          onClick={() => mutation.mutate()}
        >
          {mutation.isPending ? 'Running…' : '▶ Run'}
        </button>
      </div>

      {/* ── SOL editor textarea ───────────────────────────────────────── */}
      {/* ── CodeMirror SOL editor ─────────────────────────────────────── */}
      <SolEditor
        value={query}
        onChange={setQuery}
        onRun={() => mutation.mutate()}
        eventNames={eventNames}
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
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 'var(--type-body-sm-size)' }}>
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
                style={{ cursor: r.value ? 'pointer' : 'default', background: expandedIdx === i ? 'var(--accent-wash)' : undefined }}
                onClick={() => setExpandedIdx(expandedIdx === i ? null : i)}
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

const primaryBtnSm: React.CSSProperties = {
  padding: '5px 12px',
  background: 'var(--accent)', color: 'var(--accent-ink)',
  border: '1px solid var(--accent)',
  borderRadius: 'var(--radius-sm)', cursor: 'pointer',
  fontFamily: 'var(--font-body)', fontSize: 'var(--type-body-sm-size)', fontWeight: 500,
}

const secondaryBtnSm: React.CSSProperties = {
  padding: '5px 10px',
  background: 'transparent', color: 'var(--ink-secondary)',
  border: '1px solid var(--rule)',
  borderRadius: 'var(--radius-sm)', cursor: 'pointer',
  fontFamily: 'var(--font-body)', fontSize: 'var(--type-body-sm-size)',
}

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
