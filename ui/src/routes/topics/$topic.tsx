import { createFileRoute } from '@tanstack/react-router'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
} from '@tanstack/react-table'
import { useForm } from '@tanstack/react-form'
import { useState } from 'react'
import { topicsApi, cassettesApi, type CassetteRecord } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'
import { ConfirmDialog } from '../../components/ConfirmDialog'
import { useToast } from '../../components/Toast'
import { useDebounce } from '../../hooks/useDebounce'

export const Route = createFileRoute('/topics/$topic')({
  component: TopicDetailPage,
})

const colHelper = createColumnHelper<CassetteRecord>()

const trunc = (s: string | null, n: number) =>
  s == null ? '—' : s.length > n ? s.slice(0, n) + '…' : s

function formatBytes(b: number) {
  if (b < 1024) return `${b} B`
  if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`
  return `${(b / 1024 / 1024).toFixed(1)} MB`
}

function TopicDetailPage() {
  const { topic } = Route.useParams()
  const qc = useQueryClient()
  const { addToast } = useToast()

  // Filter state (raw)
  const [fromRaw, setFromRaw] = useState('')
  const [toRaw, setToRaw] = useState('')
  const [partitionRaw, setPartitionRaw] = useState('')
  const [offsetFromRaw, setOffsetFromRaw] = useState('')
  const [offsetToRaw, setOffsetToRaw] = useState('')
  const [cursor, setCursor] = useState<string | undefined>()
  const [cursors, setCursors] = useState<string[]>([])
  const [showConfirmTruncate, setShowConfirmTruncate] = useState(false)

  // Debounced filters
  const from = useDebounce(fromRaw, 300)
  const to = useDebounce(toRaw, 300)
  const partition = useDebounce(partitionRaw, 300)
  const offsetFrom = useDebounce(offsetFromRaw, 300)
  const offsetTo = useDebounce(offsetToRaw, 300)

  const topicQuery = useQuery({
    queryKey: ['topics', topic],
    queryFn: () => topicsApi.get(topic),
  })

  const statsQuery = useQuery({
    queryKey: ['cassettes', 'topics', topic, 'stats'],
    queryFn: () => cassettesApi.getTopicStats(topic),
  })

  const recordsQuery = useQuery({
    queryKey: ['cassettes', 'topics', topic, 'records', { from, to, partition, offsetFrom, offsetTo, cursor }],
    queryFn: () =>
      cassettesApi.getTopicRecords(topic, {
        from: from || undefined,
        to: to || undefined,
        partition: partition ? Number(partition) : undefined,
        offset_from: offsetFrom ? Number(offsetFrom) : undefined,
        offset_to: offsetTo ? Number(offsetTo) : undefined,
        cursor,
        limit: 50,
      }),
  })

  const updateMutation = useMutation({
    mutationFn: (mode: string) => topicsApi.update(topic, { mode }),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['topics', topic] }); addToast('Topic updated', 'success') },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const compactMutation = useMutation({
    mutationFn: () => cassettesApi.compactTopic(topic),
    onSuccess: () => addToast('Compaction started', 'success'),
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const truncateMutation = useMutation({
    mutationFn: () => cassettesApi.truncateTopic(topic),
    onSuccess: (d) => { void qc.invalidateQueries({ queryKey: ['cassettes', 'topics', topic] }); addToast(`Deleted ${d.deleted} records`, 'success') },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const form = useForm({
    defaultValues: { mode: topicQuery.data?.mode ?? '' },
    onSubmit: async ({ value }) => updateMutation.mutate(value.mode),
  })

  const columns = [
    colHelper.accessor('timestamp', { header: 'Timestamp', cell: i => i.getValue().slice(0, 19).replace('T', ' ') }),
    colHelper.accessor('partition', { header: 'Partition' }),
    colHelper.accessor('offset', { header: 'Offset' }),
    colHelper.accessor('key', { header: 'Key', cell: i => trunc(i.getValue(), 40) }),
    colHelper.accessor('value', { header: 'Value', cell: i => trunc(i.getValue(), 80) }),
    colHelper.accessor('recordedAt', { header: 'Recorded At', cell: i => i.getValue().slice(0, 19).replace('T', ' ') }),
  ]

  const table = useReactTable({
    data: recordsQuery.data?.data ?? [],
    columns,
    getCoreRowModel: getCoreRowModel(),
  })

  function nextPage() {
    if (recordsQuery.data?.nextCursor) {
      setCursors(prev => [...prev, cursor ?? ''])
      setCursor(recordsQuery.data!.nextCursor)
    }
  }
  function prevPage() {
    const prev = cursors[cursors.length - 1]
    setCursors(c => c.slice(0, -1))
    setCursor(prev || undefined)
  }

  return (
    <Layout>
      {topicQuery.isLoading && <LoadingSpinner />}
      {topicQuery.error && <ErrorMessage message={(topicQuery.error as Error).message} />}
      {topicQuery.data && (
        <>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: '1.5rem' }}>
            <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700 }}>{topic}</h1>
            <span style={{ background: topicQuery.data.paused ? '#fed7d7' : '#c6f6d5', color: topicQuery.data.paused ? '#9b2c2c' : '#276749', padding: '2px 8px', borderRadius: 12, fontSize: 12 }}>
              {topicQuery.data.paused ? 'Paused' : 'Active'}
            </span>
          </div>

          {/* Stats */}
          {statsQuery.data && (
            <div style={{ display: 'flex', gap: 16, marginBottom: '1.5rem', flexWrap: 'wrap' }}>
              {[
                ['Records', statsQuery.data.rowCount.toLocaleString()],
                ['Size', formatBytes(statsQuery.data.estimatedSizeBytes)],
                ['Table', statsQuery.data.tableName],
              ].map(([k, v]) => (
                <div key={k} style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 6, padding: '0.6rem 1rem', minWidth: 120 }}>
                  <div style={{ fontSize: 11, color: '#718096', marginBottom: 2 }}>{k}</div>
                  <div style={{ fontSize: 16, fontWeight: 600 }}>{v}</div>
                </div>
              ))}
            </div>
          )}

          {/* Edit form */}
          <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem', marginBottom: '1.5rem' }}>
            <h3 style={{ margin: '0 0 0.75rem', fontSize: 15 }}>Edit Topic</h3>
            <form onSubmit={(e) => { e.preventDefault(); void form.handleSubmit() }} style={{ display: 'flex', gap: 12, alignItems: 'flex-end' }}>
              <form.Field name="mode">
                {(field) => (
                  <div>
                    <label style={labelStyle}>Mode</label>
                    <select style={{ ...inputStyle, width: 160 }} value={field.state.value} onChange={e => field.handleChange(e.target.value)}>
                      <option value="general">general</option>
                      <option value="entity_only">entity_only</option>
                      <option value="both">both</option>
                    </select>
                  </div>
                )}
              </form.Field>
              <button type="submit" disabled={updateMutation.isPending} style={primaryBtnStyle}>Save</button>
            </form>
          </div>

          {/* Actions */}
          <div style={{ display: 'flex', gap: 8, marginBottom: '1.5rem' }}>
            <button style={{ ...primaryBtnStyle, background: '#805ad5' }} onClick={() => compactMutation.mutate()} disabled={compactMutation.isPending}>
              {compactMutation.isPending ? 'Compacting…' : 'Compact'}
            </button>
            <button style={{ ...primaryBtnStyle, background: '#e53e3e' }} onClick={() => setShowConfirmTruncate(true)}>
              Truncate
            </button>
          </div>

          {/* Replay panel */}
          <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem' }}>
            <h3 style={{ margin: '0 0 0.75rem', fontSize: 15 }}>Replay Records</h3>
            {/* Filters */}
            <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', marginBottom: '1rem' }}>
              {[
                ['From', fromRaw, setFromRaw, 'datetime-local'],
                ['To', toRaw, setToRaw, 'datetime-local'],
                ['Partition', partitionRaw, setPartitionRaw, 'number'],
                ['Offset From', offsetFromRaw, setOffsetFromRaw, 'number'],
                ['Offset To', offsetToRaw, setOffsetToRaw, 'number'],
              ].map(([label, val, setter, type]) => (
                <div key={label as string}>
                  <label style={labelStyle}>{label as string}</label>
                  <input
                    type={type as string}
                    style={{ ...inputStyle, width: 170 }}
                    value={val as string}
                    onChange={e => (setter as React.Dispatch<React.SetStateAction<string>>)(e.target.value)}
                  />
                </div>
              ))}
            </div>

            {recordsQuery.isLoading && <LoadingSpinner />}
            {recordsQuery.error && <ErrorMessage message={(recordsQuery.error as Error).message} />}
            {!recordsQuery.isLoading && (
              <>
                <table style={tableStyle}>
                  <thead>
                    {table.getHeaderGroups().map(hg => (
                      <tr key={hg.id}>
                        {hg.headers.map(h => <th key={h.id} style={thStyle}>{flexRender(h.column.columnDef.header, h.getContext())}</th>)}
                      </tr>
                    ))}
                  </thead>
                  <tbody>
                    {table.getRowModel().rows.map(row => (
                      <tr key={row.id}>
                        {row.getVisibleCells().map(cell => <td key={cell.id} style={tdStyle}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}
                      </tr>
                    ))}
                  </tbody>
                </table>
                <div style={{ display: 'flex', gap: 8, marginTop: '0.75rem', alignItems: 'center' }}>
                  <button style={secondaryBtnStyle} disabled={cursors.length === 0} onClick={prevPage}>← Prev</button>
                  <button style={secondaryBtnStyle} disabled={!recordsQuery.data?.hasMore} onClick={nextPage}>Next →</button>
                  <span style={{ fontSize: 13, color: '#718096' }}>{recordsQuery.data?.data.length ?? 0} records</span>
                </div>
              </>
            )}
          </div>
        </>
      )}
      {showConfirmTruncate && (
        <ConfirmDialog
          message={`Truncate all records for topic "${topic}"? This cannot be undone.`}
          onConfirm={() => { truncateMutation.mutate(); setShowConfirmTruncate(false) }}
          onCancel={() => setShowConfirmTruncate(false)}
        />
      )}
    </Layout>
  )
}

const labelStyle: React.CSSProperties = { display: 'block', marginBottom: 4, fontSize: 13, fontWeight: 600, color: '#4a5568' }
const inputStyle: React.CSSProperties = { padding: '0.4rem 0.6rem', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 14 }
const primaryBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const secondaryBtnStyle: React.CSSProperties = { padding: '0.35rem 0.8rem', background: '#fff', color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 13 }
const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', background: '#fff', fontSize: 13 }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.5rem 0.6rem', background: '#edf2f7', fontWeight: 600, color: '#4a5568', borderBottom: '1px solid #e2e8f0' }
const tdStyle: React.CSSProperties = { padding: '0.45rem 0.6rem', borderBottom: '1px solid #e2e8f0' }
