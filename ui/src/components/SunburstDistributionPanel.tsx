/**
 * SunburstDistributionPanel
 *
 * Shows a field-value histogram for all sequences that pass through a
 * sunburst arc. Fetches up to MAX_SEQS entity event sequences, extracts
 * a user-chosen numeric field from every event value payload, and renders
 * bucketed counts as a bar chart.
 */

import { useState, useMemo } from 'react'
import { useQueries } from '@tanstack/react-query'
import { cassettesApi } from '../api/client'
import { extractNumeric } from './SequenceBarcodeView'
import { LoadingSpinner } from './LoadingSpinner'

const MAX_SEQS = 20
const NUM_BUCKETS = 10

interface Props {
  entityType: string
  nodeName: string
  seqIds: string[]
  onClose: () => void
}

interface Bucket {
  label: string
  count: number
}

function buildHistogram(values: number[], numBuckets: number): Bucket[] {
  if (values.length === 0) return []
  const min = Math.min(...values)
  const max = Math.max(...values)
  if (min === max) {
    return [{ label: String(min), count: values.length }]
  }
  const step = (max - min) / numBuckets
  const counts = new Array<number>(numBuckets).fill(0)
  for (const v of values) {
    const idx = Math.min(Math.floor((v - min) / step), numBuckets - 1)
    counts[idx]++
  }
  return counts.map((count, i) => ({
    label: `${(min + i * step).toFixed(1)}–${(min + (i + 1) * step).toFixed(1)}`,
    count,
  }))
}

export function SunburstDistributionPanel({ entityType, nodeName, seqIds, onClose }: Props) {
  const [fieldRaw, setFieldRaw] = useState('')
  const [activeField, setActiveField] = useState('')

  const ids = seqIds.slice(0, MAX_SEQS)

  const results = useQueries({
    queries: ids.map(id => ({
      queryKey: ['cassettes', 'entities', entityType, id, 'records', { limit: 500 }],
      queryFn: () => cassettesApi.getEntityRecords(entityType, id, { limit: 500, order: 'asc' as const }),
      staleTime: 120_000,
      enabled: !!activeField,
    })),
  })

  const loading = results.some(r => r.isLoading)

  const { values, missing } = useMemo(() => {
    if (!activeField) return { values: [], missing: 0 }
    let hit = 0, miss = 0
    const vals: number[] = []
    for (const r of results) {
      for (const rec of r.data?.data ?? []) {
        const v = extractNumeric(rec.value ?? null, activeField)
        if (v != null) { vals.push(v); hit++ } else miss++
      }
    }
    return { values: vals, missing: miss }
  }, [results, activeField])

  const buckets = useMemo(() => buildHistogram(values, NUM_BUCKETS), [values])
  const maxCount = Math.max(...buckets.map(b => b.count), 1)

  function apply() {
    setActiveField(fieldRaw.trim())
  }

  return (
    <div style={{
      border: '1px solid var(--rule)',
      borderRadius: 'var(--radius-sm)',
      background: 'var(--surface-paper)',
      overflow: 'hidden',
    }}>
      {/* Header */}
      <div style={{
        display: 'flex', alignItems: 'center', gap: 8,
        padding: '8px 12px',
        borderBottom: '1px solid var(--rule)',
        background: 'var(--surface-raised)',
      }}>
        <span style={{ fontSize: 'var(--type-body-sm-size)', fontWeight: 600, color: 'var(--ink-primary)', flex: 1 }}>
          Distribution — <span style={{ fontFamily: 'var(--font-mono)' }}>{nodeName}</span>
          <span style={{ fontWeight: 400, color: 'var(--ink-tertiary)', marginLeft: 8 }}>
            {seqIds.length} sequence{seqIds.length !== 1 ? 's' : ''}
            {seqIds.length > MAX_SEQS && ` (showing first ${MAX_SEQS})`}
          </span>
        </span>
        <button
          style={{
            background: 'none', border: 'none', cursor: 'pointer',
            fontSize: 16, color: 'var(--ink-tertiary)', padding: '0 4px',
            lineHeight: 1,
          }}
          onClick={onClose}
        >✕</button>
      </div>

      {/* Field input */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '8px 12px', borderBottom: '1px solid var(--rule)' }}>
        <span style={{ fontSize: 'var(--type-body-sm-size)', color: 'var(--ink-secondary)', flexShrink: 0 }}>Field</span>
        <input
          value={fieldRaw}
          onChange={e => setFieldRaw(e.target.value)}
          placeholder="e.g. amount or order.total"
          style={{
            flex: 1, padding: '4px 8px',
            border: '1px solid var(--rule)',
            borderRadius: 'var(--radius-xs)',
            fontFamily: 'var(--font-mono)',
            fontSize: 'var(--type-caption-size)',
            color: 'var(--ink-primary)',
            background: 'var(--surface-paper)',
          }}
          onKeyDown={e => { if (e.key === 'Enter') apply() }}
        />
        <button
          style={{
            padding: '4px 10px',
            background: 'var(--accent)', color: 'var(--accent-ink)',
            border: '1px solid var(--accent)',
            borderRadius: 'var(--radius-sm)', cursor: 'pointer',
            fontFamily: 'var(--font-body)', fontSize: 'var(--type-body-sm-size)', fontWeight: 500,
            flexShrink: 0,
          }}
          disabled={loading}
          onClick={apply}
        >
          {loading ? '…' : 'Show'}
        </button>
      </div>

      {/* Chart area */}
      <div style={{ padding: '12px 16px', minHeight: 100 }}>
        {!activeField && (
          <p style={{ margin: 0, fontSize: 'var(--type-body-sm-size)', color: 'var(--ink-tertiary)' }}>
            Enter a dot-path field name to see its value distribution across events in these sequences.
          </p>
        )}
        {activeField && loading && <LoadingSpinner />}
        {activeField && !loading && values.length === 0 && (
          <p style={{ margin: 0, fontSize: 'var(--type-body-sm-size)', color: 'var(--ink-tertiary)' }}>
            No numeric values found for <code style={{ fontFamily: 'var(--font-mono)' }}>{activeField}</code>.
          </p>
        )}
        {activeField && !loading && values.length > 0 && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            {buckets.map((b, i) => (
              <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{
                  width: 120, flexShrink: 0, textAlign: 'right',
                  fontSize: 'var(--type-caption-size)', fontFamily: 'var(--font-mono)',
                  color: 'var(--ink-tertiary)', whiteSpace: 'nowrap',
                }}>
                  {b.label}
                </span>
                <div style={{ flex: 1, height: 16, background: 'var(--rule)', borderRadius: 'var(--radius-xs)', overflow: 'hidden' }}>
                  <div style={{
                    height: '100%',
                    width: `${(b.count / maxCount) * 100}%`,
                    background: 'var(--accent)',
                    borderRadius: 'var(--radius-xs)',
                    minWidth: b.count > 0 ? 2 : 0,
                    opacity: 0.8,
                  }} />
                </div>
                <span style={{
                  width: 36, flexShrink: 0, textAlign: 'right',
                  fontSize: 'var(--type-caption-size)', fontFamily: 'var(--font-mono)',
                  color: 'var(--ink-secondary)', fontVariantNumeric: 'tabular-nums',
                }}>
                  {b.count}
                </span>
              </div>
            ))}
            <div style={{ marginTop: 6, fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>
              {values.length.toLocaleString()} values · {missing.toLocaleString()} missing
              {' · '}min {Math.min(...values)} · max {Math.max(...values)}
              {' · '}mean {(values.reduce((a, b) => a + b, 0) / values.length).toFixed(2)}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
