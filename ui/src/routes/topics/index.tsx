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
import {
  pageTitle, primaryBtnStyle, primaryBtnSmall, warnBtnSmall, dangerBtnSmall,
  tableStyle, thStyle, tdStyle,
} from '../../styles/shared'

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
    colHelper.accessor('mode', {
      header: 'Mode',
      cell: info => <span className={`jx-badge jx-badge-brand`}>{info.getValue()}</span>,
    }),
    colHelper.accessor('paused', {
      header: 'Status',
      cell: info => info.getValue()
        ? <span className="jx-badge jx-badge-warn">Paused</span>
        : <span className="jx-badge jx-badge-success">Recording</span>,
    }),
    colHelper.accessor('active', {
      header: 'Active',
      cell: info => info.getValue()
        ? <span className="jx-badge jx-badge-success">Yes</span>
        : <span className="jx-badge jx-badge-default">No</span>,
    }),
    colHelper.accessor('consumerGroup', {
      header: 'Consumer Group',
      cell: info => <span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-mono-size)' }}>{info.getValue() ?? '—'}</span>,
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

  const table = useReactTable({ data: data ?? [], columns, getCoreRowModel: getCoreRowModel() })

  return (
    <Layout>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 24 }}>
        <h1 style={pageTitle}>Topics</h1>
        <button style={primaryBtnStyle} onClick={() => setShowAdd(true)}>+ Add Topic</button>
      </div>

      {isLoading && <LoadingSpinner />}
      {error && <ErrorMessage message={(error as Error).message} />}

      {!isLoading && !error && (
        <div style={tableStyle}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
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
                  className="jx-clickable"
                  onClick={() => void navigate({ to: '/topics/$topic', params: { topic: row.original.topic } })}
                  onMouseEnter={e => (e.currentTarget.style.background = 'var(--surface-raised)')}
                  onMouseLeave={e => (e.currentTarget.style.background = '')}
                  style={{ cursor: 'pointer' }}
                >
                  {row.getVisibleCells().map(cell => (
                    <td key={cell.id} style={tdStyle}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
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
