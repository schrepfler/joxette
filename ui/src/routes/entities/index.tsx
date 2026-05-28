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
import { entitiesApi, cassettesApi, type EntityTypeConfig, type CreateEntityRequest } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'
import { ConfirmDialog } from '../../components/ConfirmDialog'
import { ModalDialog } from '../../components/ModalDialog'
import { useToast } from '../../components/Toast'
import {
  pageTitle, primaryBtnStyle, dangerBtnSmall, cancelBtnStyle, labelStyle,
  tableStyle, thStyle, tdStyle,
} from '../../styles/shared'

export const Route = createFileRoute('/entities/')({
  component: EntitiesPage,
})

const colHelper = createColumnHelper<EntityTypeConfig>()

function AddEntityModal({ onClose }: { onClose: () => void }) {
  const qc = useQueryClient()
  const { addToast } = useToast()
  const mutation = useMutation({
    mutationFn: (d: CreateEntityRequest) => entitiesApi.create(d),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['entities'] }); addToast('Entity type created', 'success'); onClose() },
    onError: (e: Error) => addToast(e.message, 'error'),
  })
  const form = useForm({
    defaultValues: { type: '', buckets: '', retentionDays: '' },
    onSubmit: async ({ value }) => {
      mutation.mutate({
        type: value.type,
        buckets: value.buckets ? Number(value.buckets) : undefined,
        retentionDays: value.retentionDays ? Number(value.retentionDays) : undefined,
      })
    },
  })

  return (
    <ModalDialog title="Add Entity Type" onClose={onClose} style={{ minWidth: 380 }}>
      <form onSubmit={(e) => { e.preventDefault(); void form.handleSubmit() }}>
        <form.Field name="type">
          {(f) => (
            <div style={fieldWrap}>
              <label style={labelStyle}>Type *</label>
              <input className="jx-input-box" value={f.state.value} onChange={e => f.handleChange(e.target.value)} required />
            </div>
          )}
        </form.Field>
        <form.Field name="buckets">
          {(f) => (
            <div style={fieldWrap}>
              <label style={labelStyle}>Buckets</label>
              <input type="number" className="jx-input-box" value={f.state.value} onChange={e => f.handleChange(e.target.value)} />
            </div>
          )}
        </form.Field>
        <form.Field name="retentionDays">
          {(f) => (
            <div style={{ ...fieldWrap, marginBottom: 20 }}>
              <label style={labelStyle}>Retention Days</label>
              <input type="number" className="jx-input-box" value={f.state.value} onChange={e => f.handleChange(e.target.value)} />
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
    </ModalDialog>
  )
}

function EntitiesPage() {
  const navigate = useNavigate()
  const qc = useQueryClient()
  const { addToast } = useToast()
  const [showAdd, setShowAdd] = useState(false)
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null)
  const [confirmRebuild, setConfirmRebuild] = useState(false)

  const { data, isLoading, error } = useQuery({ queryKey: ['entities'], queryFn: entitiesApi.list })

  const deleteMutation = useMutation({
    mutationFn: (type: string) => entitiesApi.delete(type),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['entities'] }); addToast('Entity type deleted', 'success') },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const rebuildMutation = useMutation({
    mutationFn: () => cassettesApi.rebuildKnownEntities(),
    onSuccess: (d) => addToast(`Rebuilt ${d.rebuilt.toLocaleString()} entity rows`, 'success'),
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const columns = [
    colHelper.accessor('entityType', { header: 'Entity Type' }),
    colHelper.accessor('buckets', { header: 'Buckets' }),
    colHelper.accessor('retentionDays', { header: 'Retention Days', cell: i => i.getValue() ?? '—' }),
    colHelper.display({
      id: 'actions',
      header: 'Actions',
      cell: ({ row }) => (
        <button
          style={dangerBtnSmall}
          onClick={e => { e.stopPropagation(); setConfirmDelete(row.original.entityType) }}
        >
          Delete
        </button>
      ),
    }),
  ]

  const table = useReactTable({ data: data ?? [], columns, getCoreRowModel: getCoreRowModel() })

  return (
    <Layout>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 24 }}>
        <h1 style={pageTitle}>Entities</h1>
        <div style={{ display: 'flex', gap: 8 }}>
          <button
            style={{ ...primaryBtnStyle, background: 'var(--signal-warn-ink)', borderColor: 'var(--signal-warn-ink)' }}
            onClick={() => setConfirmRebuild(true)}
            disabled={rebuildMutation.isPending}
            title="Rebuild known_entities registry from cassette data on object storage"
          >
            {rebuildMutation.isPending ? 'Rebuilding…' : 'Rebuild Known Entities'}
          </button>
          <button style={primaryBtnStyle} onClick={() => setShowAdd(true)}>+ Add Entity Type</button>
        </div>
      </div>

      {rebuildMutation.isPending && (
        <div style={{
          display: 'flex', alignItems: 'center', gap: 10,
          background: '#fef9c3', border: '1px solid var(--signal-warn)',
          borderRadius: 'var(--radius-sm)', padding: '10px 14px', marginBottom: 16,
        }}>
          <span className="jx-spin" style={{ display: 'inline-block', width: 16, height: 16, border: '2px solid var(--signal-warn)', borderTopColor: 'var(--signal-warn-ink)', borderRadius: '50%', flexShrink: 0 }} />
          <span style={{ fontSize: 'var(--type-body-sm-size)', color: 'var(--signal-warn-ink)', fontWeight: 500 }}>
            Rebuilding known entities registry — scanning all cassette tables, this may take a while…
          </span>
        </div>
      )}

      {isLoading && <LoadingSpinner />}
      {error && <ErrorMessage message={(error as Error).message} />}

      {!isLoading && !error && (
        <div style={tableStyle}>
          <table aria-label="Entity types" style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              {table.getHeaderGroups().map(hg => (
                <tr key={hg.id}>
                  {hg.headers.map(h => <th key={h.id} style={thStyle}>{flexRender(h.column.columnDef.header, h.getContext())}</th>)}
                </tr>
              ))}
            </thead>
            <tbody>
              {table.getRowModel().rows.map(row => (
                <tr
                  key={row.id}
                  style={{ cursor: 'pointer' }}
                  onClick={() => void navigate({ to: '/entities/$entityType', params: { entityType: row.original.entityType } })}
                  onMouseEnter={e => (e.currentTarget.style.background = 'var(--surface-raised)')}
                  onMouseLeave={e => (e.currentTarget.style.background = '')}
                >
                  {row.getVisibleCells().map(cell => <td key={cell.id} style={tdStyle}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {showAdd && <AddEntityModal onClose={() => setShowAdd(false)} />}
      {confirmRebuild && (
        <ConfirmDialog
          message="Rebuild the known_entities registry from all entity cassette tables? This will delete all existing registry rows and re-scan the cassette data. Use this to recover after losing the catalog file."
          onConfirm={() => { rebuildMutation.mutate(); setConfirmRebuild(false) }}
          onCancel={() => setConfirmRebuild(false)}
        />
      )}
      {confirmDelete && (
        <ConfirmDialog
          message={`Delete entity type "${confirmDelete}"?`}
          onConfirm={() => { deleteMutation.mutate(confirmDelete); setConfirmDelete(null) }}
          onCancel={() => setConfirmDelete(null)}
        />
      )}
    </Layout>
  )
}

const fieldWrap: React.CSSProperties = { marginBottom: 12 }
