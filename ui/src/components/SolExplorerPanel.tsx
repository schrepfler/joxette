/**
 * SolExplorerPanel — Motif-style SOL workbench for an entity type.
 *
 * Left: SOL query editor (autocomplete from the cassette's event types, tokens
 * coloured to match result spans) with a recipe dropdown, plus the sequence
 * model summary. Right: the examples pane — one row per entity sequence with
 * matched tag spans overlaid.
 *
 * Runs `POST /cassettes/entities/{type}/sol-examples` across up to
 * `maxSequences` entities.
 */

import { useMemo, useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import { cassettesApi } from '../api/client'
import { SolEditor } from './SolEditor'
import { SolExamplesPane } from './SolExamplesPane'
import { SolModelPanel } from './SolModelPanel'
import { buildTagColors, extractPatternTags } from './sol-colors'
import { SOL_RECIPES } from './sol-recipes'

interface Props {
  entityType: string
}

const SCAN_OPTIONS = [100, 500, 2000] as const

export function SolExplorerPanel({ entityType }: Props) {
  const [query, setQuery] = useState('match event_a >> * >> event_b')
  const [recipesOpen, setRecipesOpen] = useState(false)
  const [maxSequences, setMaxSequences] = useState<number>(500)

  const messageTypesQuery = useQuery({
    queryKey: ['message-types', 'entity', entityType],
    queryFn: () => cassettesApi.getEntityMessageTypes(entityType),
    staleTime: 300_000,
  })
  const fieldsQuery = useQuery({
    queryKey: ['fields', 'entity', entityType],
    queryFn: () => cassettesApi.getEntityFields(entityType),
    staleTime: 300_000,
  })
  const messageTypes = messageTypesQuery.data ?? []
  const fieldPaths = fieldsQuery.data ?? []

  const mutation = useMutation({
    mutationFn: () => cassettesApi.solExamples(entityType, query, { maxSequences }),
  })
  const result = mutation.data

  // Tag → colour, in pattern order. Before the first run the order comes from a
  // light client-side parse of the match clause; afterwards from the backend
  // model (authoritative), so editor tokens and result spans always agree.
  const tagColors = useMemo(() => {
    const fromModel = result?.model
      .filter(r => !r.gap && r.label && r.label !== 'Prefix' && r.label !== 'Suffix')
      .map(r => r.label as string)
    const ordered = fromModel && fromModel.length > 0 ? fromModel : extractPatternTags(query)
    return buildTagColors(ordered)
  }, [result, query])

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>

      {/* ── Toolbar ──────────────────────────────────────────────────── */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
        <span style={{ fontSize: 'var(--type-body-sm-size)', fontWeight: 600, color: 'var(--ink-primary)' }}>
          SOL Query
        </span>

        {/* Recipe dropdown */}
        <div style={{ position: 'relative' }}>
          <button style={secondaryBtnSm} onClick={() => setRecipesOpen(o => !o)}>
            ⌨ Add recipe ▾
          </button>
          {recipesOpen && (
            <div
              style={{
                position: 'absolute', top: '100%', left: 0, zIndex: 50,
                background: 'var(--surface-paper)',
                border: '1px solid var(--rule)',
                borderRadius: 'var(--radius-sm)',
                boxShadow: 'var(--shadow-md)',
                minWidth: 380, marginTop: 4,
                maxHeight: 420, overflowY: 'auto',
              }}
            >
              <div style={{ padding: '8px 14px 4px', fontSize: 'var(--type-micro-size)', fontWeight: 700, textTransform: 'uppercase', letterSpacing: 'var(--type-micro-tracking)', color: 'var(--ink-tertiary)' }}>
                Start with a recipe
              </div>
              {SOL_RECIPES.map(r => (
                <button
                  key={r.label}
                  onClick={() => { setQuery(r.sol); setRecipesOpen(false) }}
                  style={{
                    display: 'block', width: '100%', textAlign: 'left',
                    padding: '8px 14px',
                    background: 'none', border: 'none', cursor: 'pointer',
                    fontFamily: 'var(--font-body)',
                  }}
                  onMouseEnter={e => (e.currentTarget.style.background = 'var(--ink-wash)')}
                  onMouseLeave={e => (e.currentTarget.style.background = 'none')}
                >
                  <div style={{ fontSize: 'var(--type-body-sm-size)', color: 'var(--ink-primary)' }}>{r.label}</div>
                  <div style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)', marginTop: 2 }}>{r.description}</div>
                </button>
              ))}
            </div>
          )}
        </div>

        <div style={{ flex: 1 }} />

        {/* Scan size */}
        <label style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>
          scan
          <select
            value={maxSequences}
            onChange={e => setMaxSequences(Number(e.target.value))}
            style={{
              padding: '3px 6px', border: '1px solid var(--rule)', borderRadius: 'var(--radius-xs)',
              background: 'var(--surface-paper)', color: 'var(--ink-primary)',
              fontFamily: 'var(--font-body)', fontSize: 'var(--type-caption-size)',
            }}
          >
            {SCAN_OPTIONS.map(n => <option key={n} value={n}>{n.toLocaleString()} sequences</option>)}
          </select>
        </label>

        {messageTypes.length > 0 && (
          <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>
            {messageTypes.length} event types · Ctrl+Space
          </span>
        )}
        <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>⌘↵ to run</span>
        <button
          style={{ ...primaryBtnSm, minWidth: 72 }}
          disabled={mutation.isPending}
          onClick={() => mutation.mutate()}
        >
          {mutation.isPending ? 'Running…' : '▶ Run'}
        </button>
      </div>

      {/* ── Error ────────────────────────────────────────────────────── */}
      {mutation.error && (
        <div style={{ padding: '8px 12px', background: 'color-mix(in oklab, var(--signal-error) 10%, transparent)', border: '1px solid color-mix(in oklab, var(--signal-error) 30%, transparent)', borderRadius: 'var(--radius-sm)', fontSize: 'var(--type-body-sm-size)', color: 'var(--signal-error)', fontFamily: 'var(--font-mono)' }}>
          {(mutation.error as Error).message}
        </div>
      )}

      {/* ── Editor + model | examples ────────────────────────────────── */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: result ? 'minmax(340px, 420px) 1fr' : '1fr',
          gap: 16,
          alignItems: 'start',
        }}
      >
        <div style={{ minWidth: 0 }}>
          <SolEditor
            value={query}
            onChange={setQuery}
            onRun={() => mutation.mutate()}
            messageTypes={messageTypes}
            fieldPaths={fieldPaths}
            tagColors={tagColors}
            minHeight={96}
            disabled={mutation.isPending}
          />
          {result && (
            <SolModelPanel
              model={result.model}
              totalSequences={result.totalSequences}
              tagColors={tagColors}
            />
          )}
        </div>

        {result && (
          <div style={{ minWidth: 0, border: '1px solid var(--rule)', borderRadius: 'var(--radius-sm)', padding: '8px 12px' }}>
            <SolExamplesPane
              examples={result.examples}
              totalSequences={result.totalSequences}
              matchedSequences={result.matchedSequences}
              tagColors={tagColors}
            />
          </div>
        )}
      </div>
    </div>
  )
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
