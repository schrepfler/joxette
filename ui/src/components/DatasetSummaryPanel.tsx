import { useQuery } from '@tanstack/react-query'
import { cassettesApi, type CassetteSummary } from '../api/client'
import { Tabular } from '../design/primitives/Tabular'
import { ErrorMessage } from './ErrorMessage'

export interface DatasetSummarySelection {
  dimension: string
  value: string | null
}

interface DatasetSummaryPanelProps {
  kind: 'topic' | 'entity'
  topic?: string
  entityType?: string
  entityId?: string
  from?: string
  to?: string
  selected: DatasetSummarySelection | null
  onSelect: (dimension: string, value: string | null) => void
}

const DIMENSION_LABELS: Record<string, string> = {
  partition: 'By partition',
  sourceTopic: 'By source topic',
  messageType: 'By message type',
}

export function DatasetSummaryPanel({ kind, topic, entityType, entityId, from, to, selected, onSelect }: DatasetSummaryPanelProps) {
  const id = kind === 'topic' ? topic : `${entityType}/${entityId}`

  const query = useQuery({
    queryKey: ['cassettes', kind, id, 'summary', { from, to }],
    queryFn: () =>
      kind === 'topic'
        ? cassettesApi.getTopicSummary(topic!, { from, to })
        : cassettesApi.getEntitySummary(entityType!, entityId!, { from, to }),
    staleTime: 30_000,
  })

  if (query.isLoading) {
    return (
      <aside style={{ width: 240, flexShrink: 0 }} aria-busy="true" aria-label="Dataset summary loading">
        {[0, 1].map(i => (
          <div key={i} style={{ height: 120, borderRadius: 6, background: 'var(--surface-raised)', marginBottom: 16 }} />
        ))}
      </aside>
    )
  }

  if (query.error || !query.data) {
    return (
      <aside style={{ width: 240, flexShrink: 0 }}>
        <ErrorMessage message={(query.error as Error)?.message ?? 'Failed to load summary'} />
      </aside>
    )
  }

  const summary: CassetteSummary = query.data
  const dimensionKeys = Object.keys(summary.dimensions)

  return (
    <aside style={{ width: 240, flexShrink: 0, display: 'flex', flexDirection: 'column', gap: 20 }} aria-label="Dataset summary">
      {dimensionKeys.map(dimKey => {
        const rows = summary.dimensions[dimKey] ?? []
        return (
          <div key={dimKey}>
            <div
              style={{
                fontSize: 11,
                fontWeight: 600,
                textTransform: 'uppercase',
                letterSpacing: '0.06em',
                color: 'var(--ink-secondary)',
                marginBottom: 8,
              }}
            >
              {DIMENSION_LABELS[dimKey] ?? dimKey}
            </div>
            {rows.length === 0 ? (
              <div style={{ fontSize: 12, color: 'var(--ink-tertiary)' }}>No data</div>
            ) : (
              <ul style={{ listStyle: 'none', margin: 0, padding: 0, display: 'flex', flexDirection: 'column', gap: 2 }}>
                {rows.map(row => {
                  const isSelected = selected?.dimension === dimKey && selected?.value === row.value
                  return (
                    <li key={row.value ?? '__null__'}>
                      <button
                        type="button"
                        onClick={() => onSelect(dimKey, row.value)}
                        style={{
                          display: 'flex',
                          justifyContent: 'space-between',
                          alignItems: 'center',
                          width: '100%',
                          padding: '4px 6px',
                          background: isSelected ? 'var(--accent)' : 'transparent',
                          color: isSelected ? 'var(--accent-ink)' : 'var(--ink-primary)',
                          border: 'none',
                          borderRadius: 4,
                          cursor: 'pointer',
                          fontSize: 13,
                          textAlign: 'left',
                        }}
                      >
                        <span>{row.value ?? '(none)'}</span>
                        <Tabular size="xs" style={{ color: 'inherit' }}>{row.count.toLocaleString()}</Tabular>
                      </button>
                    </li>
                  )
                })}
              </ul>
            )}
          </div>
        )
      })}
    </aside>
  )
}
