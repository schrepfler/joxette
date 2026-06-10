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
import { SolToolbar } from './SolToolbar'
import { buildTagColors, extractPatternTags } from './sol-colors'

interface Props {
  entityType: string
}

const SCAN_OPTIONS = [100, 500, 2000] as const

export function SolExplorerPanel({ entityType }: Props) {
  const [query, setQuery] = useState('match event_a >> * >> event_b')
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
      <SolToolbar
        onRun={() => mutation.mutate()}
        isPending={mutation.isPending}
        messageTypes={messageTypes}
        fieldPaths={fieldPaths}
        onQueryChange={setQuery}
      >
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
      </SolToolbar>

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

