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
import { brokersApi, type BrokerConfig, type CreateBrokerRequest, type UpdateBrokerRequest } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'
import { ConfirmDialog } from '../../components/ConfirmDialog'
import { ModalDialog } from '../../components/ModalDialog'
import { useToast } from '../../components/Toast'
import {
  pageTitle, primaryBtnStyle, primaryBtnSmall, cancelBtnStyle, dangerBtnSmall,
  labelStyle, tableStyle, thStyle, tdStyle,
} from '../../styles/shared'

export const Route = createFileRoute('/brokers/')({
  component: BrokersPage,
})

const colHelper = createColumnHelper<BrokerConfig>()

const PROTOCOL_TONE: Record<string, string> = {
  PLAINTEXT:      'jx-badge-default',
  SASL_PLAINTEXT: 'jx-badge-accent',
  SASL_SSL:       'jx-badge-success',
  SSL:            'jx-badge-brand',
}

function ProtocolBadge({ protocol }: { protocol: string }) {
  const cls = PROTOCOL_TONE[protocol] ?? 'jx-badge-default'
  return <span className={`jx-badge ${cls}`}>{protocol}</span>
}

const SASL_PROTOCOLS = new Set(['SASL_PLAINTEXT', 'SASL_SSL'])
const SSL_PROTOCOLS = new Set(['SASL_SSL', 'SSL'])

function BrokerForm({
  defaultValues,
  onSubmit,
  isPending,
  onClose,
  isEdit,
}: {
  defaultValues: Record<string, string>
  onSubmit: (values: Record<string, string>) => void
  isPending: boolean
  onClose: () => void
  isEdit?: boolean
}) {
  const form = useForm({ defaultValues, onSubmit: async ({ value }) => onSubmit(value) })
  const [protocol, setProtocol] = useState(defaultValues.securityProtocol ?? 'PLAINTEXT')
  const showSasl = SASL_PROTOCOLS.has(protocol)
  const showSsl = SSL_PROTOCOLS.has(protocol)

  return (
    <form onSubmit={(e) => { e.preventDefault(); void form.handleSubmit() }}>
      {!isEdit && (
        <form.Field name="brokerId">
          {(field) => (
            <div style={fieldWrap}>
              <label style={labelStyle}>Broker ID *</label>
              <input className="jx-input-box" value={field.state.value} onChange={e => field.handleChange(e.target.value)} placeholder="e.g. production, staging" required />
            </div>
          )}
        </form.Field>
      )}
      <form.Field name="bootstrapServers">
        {(field) => (
          <div style={fieldWrap}>
            <label style={labelStyle}>Bootstrap Servers *</label>
            <input className="jx-input-box" value={field.state.value} onChange={e => field.handleChange(e.target.value)} placeholder="broker1:9092,broker2:9092" required />
          </div>
        )}
      </form.Field>
      <form.Field name="securityProtocol">
        {(field) => (
          <div style={fieldWrap}>
            <label style={labelStyle}>Security Protocol</label>
            <select className="jx-input-box" value={field.state.value} onChange={e => { field.handleChange(e.target.value); setProtocol(e.target.value) }}>
              <option value="PLAINTEXT">PLAINTEXT</option>
              <option value="SASL_PLAINTEXT">SASL_PLAINTEXT</option>
              <option value="SASL_SSL">SASL_SSL</option>
              <option value="SSL">SSL</option>
            </select>
          </div>
        )}
      </form.Field>
      {showSasl && (
        <>
          <form.Field name="saslMechanism">
            {(field) => (
              <div style={fieldWrap}>
                <label style={labelStyle}>SASL Mechanism</label>
                <select className="jx-input-box" value={field.state.value} onChange={e => field.handleChange(e.target.value)}>
                  <option value="PLAIN">PLAIN</option>
                  <option value="SCRAM-SHA-256">SCRAM-SHA-256</option>
                  <option value="SCRAM-SHA-512">SCRAM-SHA-512</option>
                </select>
              </div>
            )}
          </form.Field>
          <form.Field name="saslUsername">
            {(field) => (
              <div style={fieldWrap}>
                <label style={labelStyle}>SASL Username</label>
                <input className="jx-input-box" value={field.state.value} onChange={e => field.handleChange(e.target.value)} />
              </div>
            )}
          </form.Field>
          <form.Field name="saslPassword">
            {(field) => (
              <div style={fieldWrap}>
                <label style={labelStyle}>SASL Password</label>
                <input type="password" className="jx-input-box" value={field.state.value} onChange={e => field.handleChange(e.target.value)} />
              </div>
            )}
          </form.Field>
        </>
      )}
      {showSsl && (
        <>
          <form.Field name="sslTruststorePath">
            {(field) => (
              <div style={fieldWrap}>
                <label style={labelStyle}>SSL Truststore Path</label>
                <input className="jx-input-box" value={field.state.value} onChange={e => field.handleChange(e.target.value)} />
              </div>
            )}
          </form.Field>
          <form.Field name="sslTruststorePassword">
            {(field) => (
              <div style={fieldWrap}>
                <label style={labelStyle}>SSL Truststore Password</label>
                <input type="password" className="jx-input-box" value={field.state.value} onChange={e => field.handleChange(e.target.value)} />
              </div>
            )}
          </form.Field>
          <form.Field name="sslKeystorePath">
            {(field) => (
              <div style={fieldWrap}>
                <label style={labelStyle}>SSL Keystore Path</label>
                <input className="jx-input-box" value={field.state.value} onChange={e => field.handleChange(e.target.value)} />
              </div>
            )}
          </form.Field>
          <form.Field name="sslKeystorePassword">
            {(field) => (
              <div style={{ ...fieldWrap, marginBottom: 20 }}>
                <label style={labelStyle}>SSL Keystore Password</label>
                <input type="password" className="jx-input-box" value={field.state.value} onChange={e => field.handleChange(e.target.value)} />
              </div>
            )}
          </form.Field>
        </>
      )}
      <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
        <button type="button" onClick={onClose} style={cancelBtnStyle}>Cancel</button>
        <button type="submit" disabled={isPending} style={primaryBtnStyle}>
          {isPending ? (isEdit ? 'Saving…' : 'Creating…') : (isEdit ? 'Save' : 'Create')}
        </button>
      </div>
    </form>
  )
}

function AddBrokerModal({ onClose }: { onClose: () => void }) {
  const qc = useQueryClient()
  const { addToast } = useToast()
  const mutation = useMutation({
    mutationFn: (data: CreateBrokerRequest) => brokersApi.create(data),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['brokers'] }); addToast('Broker created', 'success'); onClose() },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  return (
    <ModalDialog title="Add Broker" onClose={onClose} style={{ minWidth: 400, maxWidth: 520, maxHeight: '90vh', overflowY: 'auto' }}>
      <BrokerForm
        defaultValues={{ brokerId: '', bootstrapServers: '', securityProtocol: 'PLAINTEXT', saslMechanism: 'PLAIN', saslUsername: '', saslPassword: '', sslTruststorePath: '', sslTruststorePassword: '', sslKeystorePath: '', sslKeystorePassword: '' }}
        onSubmit={(v) => mutation.mutate(v as unknown as CreateBrokerRequest)}
        isPending={mutation.isPending}
        onClose={onClose}
      />
    </ModalDialog>
  )
}

function EditBrokerModal({ broker, onClose }: { broker: BrokerConfig; onClose: () => void }) {
  const qc = useQueryClient()
  const { addToast } = useToast()
  const mutation = useMutation({
    mutationFn: (data: UpdateBrokerRequest) => brokersApi.update(broker.brokerId, data),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['brokers'] }); addToast('Broker updated', 'success'); onClose() },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  return (
    <ModalDialog title="Edit Broker" onClose={onClose} style={{ minWidth: 400, maxWidth: 520, maxHeight: '90vh', overflowY: 'auto' }}>
      <div style={{ marginBottom: 12 }}>
        <div style={{ fontSize: 'var(--type-micro-size)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.08em', color: 'var(--ink-tertiary)', marginBottom: 2 }}>Broker ID</div>
        <div style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-mono-size)', color: 'var(--ink-primary)' }}>{broker.brokerId}</div>
      </div>
      <BrokerForm
        defaultValues={{ bootstrapServers: broker.bootstrapServers, securityProtocol: broker.securityProtocol ?? 'PLAINTEXT', saslMechanism: broker.saslMechanism ?? 'PLAIN', saslUsername: broker.saslUsername ?? '', saslPassword: '', sslTruststorePath: broker.sslTruststorePath ?? '', sslTruststorePassword: '', sslKeystorePath: broker.sslKeystorePath ?? '', sslKeystorePassword: '' }}
        onSubmit={(v) => mutation.mutate(v as unknown as UpdateBrokerRequest)}
        isPending={mutation.isPending}
        onClose={onClose}
        isEdit
      />
    </ModalDialog>
  )
}

function BrokersPage() {
  const qc = useQueryClient()
  const navigate = useNavigate()
  const { addToast } = useToast()
  const [showAdd, setShowAdd] = useState(false)
  const [editBroker, setEditBroker] = useState<BrokerConfig | null>(null)
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null)

  const { data, isLoading, error } = useQuery({ queryKey: ['brokers'], queryFn: brokersApi.list })

  const deleteMutation = useMutation({
    mutationFn: (brokerId: string) => brokersApi.delete(brokerId),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['brokers'] }); addToast('Broker deleted', 'success') },
    onError: (e: Error) => {
      const msg = e.message
      if (msg.includes('409')) {
        const detail = msg.replace(/^HTTP 409[^:]*:\s*/, '')
        addToast(detail || 'Broker is referenced by topics and cannot be deleted', 'error')
      } else {
        addToast(msg, 'error')
      }
    },
  })

  const columns = [
    colHelper.accessor('brokerId', { header: 'Broker ID', cell: info => <span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-mono-size)', fontWeight: 600 }}>{info.getValue()}</span> }),
    colHelper.accessor('bootstrapServers', { header: 'Bootstrap Servers' }),
    colHelper.accessor('securityProtocol', { header: 'Protocol', cell: info => <ProtocolBadge protocol={info.getValue()} /> }),
    colHelper.accessor('saslMechanism', { header: 'SASL Mechanism', cell: info => info.getValue() ?? '—' }),
    colHelper.display({
      id: 'actions',
      header: 'Actions',
      cell: ({ row }) => {
        const b = row.original
        return (
          <div style={{ display: 'flex', gap: 6 }}>
            <button style={primaryBtnSmall} onClick={e => { e.stopPropagation(); setEditBroker(b) }}>Edit</button>
            <button style={dangerBtnSmall} onClick={e => { e.stopPropagation(); setConfirmDelete(b.brokerId) }}>Delete</button>
          </div>
        )
      },
    }),
  ]

  const table = useReactTable({ data: data ?? [], columns, getCoreRowModel: getCoreRowModel() })

  return (
    <Layout>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 24 }}>
        <h1 style={pageTitle}>Brokers</h1>
        <button style={primaryBtnStyle} onClick={() => setShowAdd(true)}>+ Add Broker</button>
      </div>

      {isLoading && <LoadingSpinner />}
      {error && <ErrorMessage message={(error as Error).message} />}

      {!isLoading && !error && (
        <div style={tableStyle}>
          <table aria-label="Kafka brokers" style={{ width: '100%', borderCollapse: 'collapse' }}>
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
                  onClick={() => void navigate({ to: '/brokers/$brokerId', params: { brokerId: row.original.brokerId } })}
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

      {showAdd && <AddBrokerModal onClose={() => setShowAdd(false)} />}
      {editBroker && <EditBrokerModal broker={editBroker} onClose={() => setEditBroker(null)} />}
      {confirmDelete && (
        <ConfirmDialog
          message={`Delete broker "${confirmDelete}"? Topics using this broker will need to be reassigned.`}
          onConfirm={() => { deleteMutation.mutate(confirmDelete); setConfirmDelete(null) }}
          onCancel={() => setConfirmDelete(null)}
        />
      )}
    </Layout>
  )
}

const fieldWrap: React.CSSProperties = { marginBottom: 12 }
