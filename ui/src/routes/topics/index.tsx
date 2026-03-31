import { createFileRoute, useNavigate } from '@tanstack/react-router'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
} from '@tanstack/react-table'
import { useForm } from '@tanstack/react-form'
import { useState } from 'react'
import { topicsApi, type TopicConfig, type CreateTopicRequest } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'
import { ConfirmDialog } from '../../components/ConfirmDialog'
import { useToast } from '../../components/Toast'

export const Route = createFileRoute('/topics/')({
  component: TopicsPage,
})

const colHelper = createColumnHelper<TopicConfig>()

function AddTopicModal({ onClose }: { onClose: () => void }) {
  const qc = useQueryClient()
  const { addToast } = useToast()
  const mutation = useMutation({
    mutationFn: (data: CreateTopicRequest) => topicsApi.create(data),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['topics'] })
      addToast('Topic created', 'success')
      onClose()
    },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const form = useForm({
    defaultValues: {
      topic: '',
      mode: 'general',
      consumerGroup: '',
      retentionDays: '',
      startFrom: '',
    },
    onSubmit: async ({ value }) => {
      const req: CreateTopicRequest = {
        topic: value.topic,
        mode: value.mode || undefined,
        consumerGroup: value.consumerGroup || undefined,
        retentionDays: value.retentionDays ? Number(value.retentionDays) : undefined,
        startFrom: value.startFrom || undefined,
      }
      mutation.mutate(req)
    },
  })

  return (
    <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }} onClick={onClose}>
      <div style={{ background: '#fff', borderRadius: 8, padding: '1.5rem 2rem', minWidth: 400, maxWidth: 520, boxShadow: '0 10px 30px rgba(0,0,0,0.2)' }} onClick={e => e.stopPropagation()}>
        <h2 style={{ margin: '0 0 1.25rem', fontSize: 18 }}>Add Topic</h2>
        <form
          onSubmit={(e) => {
            e.preventDefault()
            void form.handleSubmit()
          }}
        >
          <form.Field name="topic">
            {(field) => (
              <div style={{ marginBottom: '0.75rem' }}>
                <label style={labelStyle}>Topic *</label>
                <input style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)} required />
              </div>
            )}
          </form.Field>
          <form.Field name="mode">
            {(field) => (
              <div style={{ marginBottom: '0.75rem' }}>
                <label style={labelStyle}>Mode</label>
                <select style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)}>
                  <option value="general">general</option>
                  <option value="entity_only">entity_only</option>
                  <option value="both">both</option>
                </select>
              </div>
            )}
          </form.Field>
          <form.Field name="consumerGroup">
            {(field) => (
              <div style={{ marginBottom: '0.75rem' }}>
                <label style={labelStyle}>Consumer Group</label>
                <input style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)} />
              </div>
            )}
          </form.Field>
          <form.Field name="retentionDays">
            {(field) => (
              <div style={{ marginBottom: '0.75rem' }}>
                <label style={labelStyle}>Retention Days</label>
                <input type="number" style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)} />
              </div>
            )}
          </form.Field>
          <form.Field name="startFrom">
            {(field) => (
              <div style={{ marginBottom: '1rem' }}>
                <label style={labelStyle}>Start From</label>
                <input style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)} placeholder="earliest / latest / ISO date" />
              </div>
            )}
          </form.Field>
          <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
            <button type="button" onClick={onClose} style={cancelBtnStyle}>Cancel</button>
            <button type="submit" disabled={mutation.isPending} style={primaryBtnStyle}>
              {mutation.isPending ? 'Creating…' : 'Create'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

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
const labelStyle: React.CSSProperties = { display: 'block', marginBottom: 4, fontSize: 13, fontWeight: 600, color: '#4a5568' }
const inputStyle: React.CSSProperties = { width: '100%', padding: '0.4rem 0.6rem', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 14, boxSizing: 'border-box' }
const primaryBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const cancelBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#fff', color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const primaryBtnSmall: React.CSSProperties = { padding: '0.2rem 0.6rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 3, cursor: 'pointer', fontSize: 12 }
const warnBtnSmall: React.CSSProperties = { padding: '0.2rem 0.6rem', background: '#dd6b20', color: '#fff', border: 'none', borderRadius: 3, cursor: 'pointer', fontSize: 12 }
const dangerBtnSmall: React.CSSProperties = { padding: '0.2rem 0.6rem', background: '#e53e3e', color: '#fff', border: 'none', borderRadius: 3, cursor: 'pointer', fontSize: 12 }
const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', background: '#fff', borderRadius: 8, overflow: 'hidden', boxShadow: '0 1px 4px rgba(0,0,0,0.1)' }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.6rem 0.75rem', background: '#edf2f7', fontSize: 13, fontWeight: 600, color: '#4a5568', borderBottom: '1px solid #e2e8f0' }
const tdStyle: React.CSSProperties = { padding: '0.55rem 0.75rem', borderBottom: '1px solid #e2e8f0', fontSize: 14 }
const pageTitle: React.CSSProperties = { margin: 0, fontSize: 22, fontWeight: 700 }
