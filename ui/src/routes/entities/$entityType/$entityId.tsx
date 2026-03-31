import { createFileRoute, useNavigate } from '@tanstack/react-router'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
} from '@tanstack/react-table'
import { useState } from 'react'
import { cassettesApi, type EntityRecord } from '../../../api/client'
import { Layout } from '../../../components/Layout'
import { LoadingSpinner } from '../../../components/LoadingSpinner'
import { ErrorMessage } from '../../../components/ErrorMessage'
import { ConfirmDialog } from '../../../components/ConfirmDialog'
import { useToast } from '../../../components/Toast'
import { useDebounce } from '../../../hooks/useDebounce'

export const Route = createFileRoute('/entities/$entityType/$entityId')({
  component: EntityInstancePage,
})

const colHelper = createColumnHelper<EntityRecord>()

const trunc = (s: string | null, n: number) =>
  s == null ? '—' : s.length > n ? s.slice(0, n) + '…' : s

function EntityInstancePage() {
  const { entityType, entityId } = Route.useParams()
  const navigate = useNavigate()
  const qc = useQueryClient()
  const { addToast } = useToast()
  const [fromRaw, setFromRaw] = useState('')
  const [toRaw, setToRaw] = useState('')
  const from = useDebounce(fromRaw, 300)
  const to = useDebounce(toRaw, 300)
  const [cursor, setCursor] = useState<string | undefined>()
  const [cursors, setCursors] = useState<string[]>([])
  const [showConfirmDelete, setShowConfirmDelete] = useState(false)

  const statsQuery = useQuery({
    queryKey: ['cassettes', 'entities', entityType, entityId, 'stats'],
    queryFn: () => cassettesApi.getEntityStats(entityType, entityId),
  })

  const recordsQuery = useQuery({
    queryKey: ['cassettes', 'entities', entityType, entityId, 'records', { from, to, cursor }],
    queryFn: () => cassettesApi.getEntityRecords(entityType, entityId, {
      from: from || undefined,
      to: to || undefined,
      cursor,
      limit: 50,
    }),
  })

  const deleteMutation = useMutation({
    mutationFn: () => cassettesApi.deleteEntity(entityType, entityId),
    onSuccess: (d) => {
      void qc.invalidateQueries({ queryKey: ['cassettes', 'entities', entityType] })
      addToast(`Deleted ${d.deleted} records`, 'success')
      void navigate({ to: '/entities/$entityType', params: { entityType } })
    },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const columns = [
    colHelper.accessor('timestamp', { header: 'Timestamp', cell: i => i.getValue().slice(0, 19).replace('T', ' ') }),
    colHelper.accessor('topic', { header: 'Topic' }),
    colHelper.accessor('partition', { header: 'Partition' }),
    colHelper.accessor('offset', { header: 'Offset' }),
    colHelper.accessor('key', { header: 'Key', cell: i => trunc(i.getValue(), 40) }),
    colHelper.accessor('value', { header: 'Value', cell: i => trunc(i.getValue(), 80) }),
    colHelper.accessor('recordedAt', { header: 'Recorded At', cell: i => i.getValue().slice(0, 19).replace('T', ' ') }),
  ]

  const table = useReactTable({ data: recordsQuery.data?.data ?? [], columns, getCoreRowModel: getCoreRowModel() })

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

  const stats = statsQuery.data

  return (
    <Layout>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <div style={{ fontSize: 12, color: '#718096', marginBottom: 4 }}>{entityType}</div>
          <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700 }}>{entityId}</h1>
        </div>
        <button
          style={{ padding: '0.45rem 1rem', background: '#e53e3e', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14 }}
          onClick={() => setShowConfirmDelete(true)}
        >
          GDPR Delete
        </button>
      </div>

      {statsQuery.isLoading && <LoadingSpinner />}
      {statsQuery.error && <ErrorMessage message={(statsQuery.error as Error).message} />}
      {stats && (
        <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem', marginBottom: '1.5rem' }}>
          <h3 style={{ margin: '0 0 0.75rem', fontSize: 15 }}>Stats</h3>
          <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: '0.75rem' }}>
            {[
              ['Messages', stats.messageCount.toLocaleString()],
              ['First Message', stats.firstMessage?.slice(0, 19).replace('T', ' ') ?? '—'],
              ['Last Message', stats.lastMessage?.slice(0, 19).replace('T', ' ') ?? '—'],
              ['First Seen', stats.firstSeen?.slice(0, 19).replace('T', ' ') ?? '—'],
              ['Last Seen', stats.lastSeen?.slice(0, 19).replace('T', ' ') ?? '—'],
            ].map(([k, v]) => (
              <div key={k} style={{ background: '#f7fafc', border: '1px solid #e2e8f0', borderRadius: 6, padding: '0.5rem 0.85rem', minWidth: 140 }}>
                <div style={{ fontSize: 11, color: '#718096', marginBottom: 2 }}>{k}</div>
                <div style={{ fontSize: 14, fontWeight: 600 }}>{v}</div>
              </div>
            ))}
          </div>
          {Object.keys(stats.countByTopic).length > 0 && (
            <div>
              <div style={{ fontSize: 13, fontWeight: 600, color: '#4a5568', marginBottom: 4 }}>Messages by Topic</div>
              <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                {Object.entries(stats.countByTopic).map(([t, count]) => (
                  <div key={t} style={{ background: '#ebf8ff', border: '1px solid #bee3f8', borderRadius: 4, padding: '2px 10px', fontSize: 13 }}>
                    <strong>{t}</strong>: {count}
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Replay */}
      <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem' }}>
        <h3 style={{ margin: '0 0 0.75rem', fontSize: 15 }}>Replay Records</h3>
        <div style={{ display: 'flex', gap: 10, marginBottom: '1rem', flexWrap: 'wrap' }}>
          {[['From', fromRaw, setFromRaw], ['To', toRaw, setToRaw]].map(([label, val, setter]) => (
            <div key={label as string}>
              <label style={labelStyle}>{label as string}</label>
              <input
                type="datetime-local"
                style={{ padding: '0.4rem 0.6rem', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 14, width: 200 }}
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
                  <tr key={hg.id}>{hg.headers.map(h => <th key={h.id} style={thStyle}>{flexRender(h.column.columnDef.header, h.getContext())}</th>)}</tr>
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

      {showConfirmDelete && (
        <ConfirmDialog
          message={`GDPR delete all data for entity "${entityId}"? This cannot be undone.`}
          onConfirm={() => { deleteMutation.mutate(); setShowConfirmDelete(false) }}
          onCancel={() => setShowConfirmDelete(false)}
        />
      )}
    </Layout>
  )
}

const labelStyle: React.CSSProperties = { display: 'block', marginBottom: 4, fontSize: 13, fontWeight: 600, color: '#4a5568' }
const secondaryBtnStyle: React.CSSProperties = { padding: '0.35rem 0.8rem', background: '#fff', color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 13 }
const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', background: '#fff', fontSize: 13 }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.5rem 0.6rem', background: '#edf2f7', fontWeight: 600, color: '#4a5568', borderBottom: '1px solid #e2e8f0' }
const tdStyle: React.CSSProperties = { padding: '0.45rem 0.6rem', borderBottom: '1px solid #e2e8f0' }
