import { createFileRoute, useNavigate } from '@tanstack/react-router'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
} from '@tanstack/react-table'
import { useState } from 'react'
import { topicsApi, type TopicConfig } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'
import { ConfirmDialog } from '../../components/ConfirmDialog'
import { useToast } from '../../components/Toast'
import { AddTopicModal } from '../../components/AddTopicModal'

export const Route = createFileRoute('/topics/')({
  component: TopicsPage,
})

const colHelper = createColumnHelper<TopicConfig>()

function TopicsPage() {
  const navigate = useNavigate()
  const qc = useQueryClient()
  const { addToast } = useToast()
  const [showAdd, setShowAdd] = useState(false)
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null)

  const { data, isLoading, error } = useQuery({
    queryKey: ['topics'],
    queryFn: topicsApi.list,
  })

  const pauseMutation = useMutation({
    mutationFn: (topic: string) => topicsApi.pause(topic),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['topics'] }); addToast('Topic paused', 'success') },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const resumeMutation = useMutation({
    mutationFn: (topic: string) => topicsApi.resume(topic),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['topics'] }); addToast('Topic resumed', 'success') },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const deleteMutation = useMutation({
    mutationFn: (topic: string) => topicsApi.delete(topic),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['topics'] }); addToast('Topic deleted', 'success') },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const columns = [
    colHelper.accessor('topic', { header: 'Topic' }),
    colHelper.accessor('mode', { header: 'Mode' }),
    colHelper.accessor('paused', {
      header: 'Paused',
      cell: info => info.getValue() ? 'Yes' : 'No',
    }),
    colHelper.accessor('active', {
      header: 'Active',
      cell: info => info.getValue() ? 'Yes' : 'No',
    }),
    colHelper.accessor('consumerGroup', {
      header: 'Consumer Group',
      cell: info => info.getValue() ?? '—',
    }),
    colHelper.accessor('brokerId', {
      header: 'Broker',
      cell: info => info.getValue() ?? 'default',
    }),
    colHelper.display({
      id: 'actions',
      header: 'Actions',
      cell: ({ row }) => {
        const t = row.original
        return (
          <div style={{ display: 'flex', gap: 6 }}>
            <button
              style={t.paused ? primaryBtnSmall : warnBtnSmall}
              onClick={e => { e.stopPropagation(); t.paused ? resumeMutation.mutate(t.topic) : pauseMutation.mutate(t.topic) }}
            >
              {t.paused ? 'Resume' : 'Pause'}
            </button>
            <button
              style={dangerBtnSmall}
              onClick={e => { e.stopPropagation(); setConfirmDelete(t.topic) }}
            >
              Delete
            </button>
          </div>
        )
      },
    }),
  ]

  const table = useReactTable({
    data: data ?? [],
    columns,
    getCoreRowModel: getCoreRowModel(),
  })

  return (
    <Layout>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
        <h1 style={pageTitle}>Topics</h1>
        <button style={primaryBtnStyle} onClick={() => setShowAdd(true)}>+ Add Topic</button>
      </div>
      {isLoading && <LoadingSpinner />}
      {error && <ErrorMessage message={(error as Error).message} />}
      {!isLoading && !error && (
        <table style={tableStyle}>
          <thead>
            {table.getHeaderGroups().map(hg => (
              <tr key={hg.id}>
                {hg.headers.map(h => (
                  <th key={h.id} style={thStyle}>{flexRender(h.column.columnDef.header, h.getContext())}</th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.map(row => (
              <tr
                key={row.id}
                style={{ cursor: 'pointer' }}
                onClick={() => void navigate({ to: '/topics/$topic', params: { topic: row.original.topic } })}
                onMouseEnter={e => (e.currentTarget.style.background = '#ebf8ff')}
                onMouseLeave={e => (e.currentTarget.style.background = '')}
              >
                {row.getVisibleCells().map(cell => (
                  <td key={cell.id} style={tdStyle}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {showAdd && <AddTopicModal onClose={() => setShowAdd(false)} />}
      {confirmDelete && (
        <ConfirmDialog
          message={`Delete topic "${confirmDelete}"? This cannot be undone.`}
          onConfirm={() => { deleteMutation.mutate(confirmDelete); setConfirmDelete(null) }}
          onCancel={() => setConfirmDelete(null)}
        />
      )}
    </Layout>
  )
}

// Shared styles
const primaryBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const primaryBtnSmall: React.CSSProperties = { padding: '0.2rem 0.6rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 3, cursor: 'pointer', fontSize: 12 }
const warnBtnSmall: React.CSSProperties = { padding: '0.2rem 0.6rem', background: '#dd6b20', color: '#fff', border: 'none', borderRadius: 3, cursor: 'pointer', fontSize: 12 }
const dangerBtnSmall: React.CSSProperties = { padding: '0.2rem 0.6rem', background: '#e53e3e', color: '#fff', border: 'none', borderRadius: 3, cursor: 'pointer', fontSize: 12 }
const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', background: '#fff', borderRadius: 8, overflow: 'hidden', boxShadow: '0 1px 4px rgba(0,0,0,0.1)' }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.6rem 0.75rem', background: '#edf2f7', fontSize: 13, fontWeight: 600, color: '#4a5568', borderBottom: '1px solid #e2e8f0' }
const tdStyle: React.CSSProperties = { padding: '0.55rem 0.75rem', borderBottom: '1px solid #e2e8f0', fontSize: 14 }
const pageTitle: React.CSSProperties = { margin: 0, fontSize: 22, fontWeight: 700 }
