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
import { brokersApi, type BrokerConfig, type CreateBrokerRequest, type UpdateBrokerRequest } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'
import { ConfirmDialog } from '../../components/ConfirmDialog'
import { useToast } from '../../components/Toast'

export const Route = createFileRoute('/brokers/')({
  component: BrokersPage,
})

const colHelper = createColumnHelper<BrokerConfig>()

function ProtocolBadge({ protocol }: { protocol: string }) {
  const colors: Record<string, { bg: string; color: string }> = {
    PLAINTEXT:      { bg: '#edf2f7', color: '#4a5568' },
    SASL_PLAINTEXT: { bg: '#ebf8ff', color: '#2b6cb0' },
    SASL_SSL:       { bg: '#f0fff4', color: '#276749' },
    SSL:            { bg: '#fffff0', color: '#744210' },
  }
  const style = colors[protocol] ?? colors.PLAINTEXT
  return (
    <span style={{ ...style, padding: '2px 8px', borderRadius: 10, fontSize: 12, fontWeight: 600 }}>
      {protocol}
    </span>
  )
}

const SASL_PROTOCOLS = new Set(['SASL_PLAINTEXT', 'SASL_SSL'])
const SSL_PROTOCOLS = new Set(['SASL_SSL', 'SSL'])

function AddBrokerModal({ onClose }: { onClose: () => void }) {
  const qc = useQueryClient()
  const { addToast } = useToast()
  const mutation = useMutation({
    mutationFn: (data: CreateBrokerRequest) => brokersApi.create(data),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['brokers'] })
      addToast('Broker created', 'success')
      onClose()
    },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const form = useForm({
    defaultValues: {
      brokerId: '',
      bootstrapServers: '',
      securityProtocol: 'PLAINTEXT',
      saslMechanism: 'PLAIN',
      saslUsername: '',
      saslPassword: '',
      sslTruststorePath: '',
      sslTruststorePassword: '',
      sslKeystorePath: '',
      sslKeystorePassword: '',
    },
    onSubmit: async ({ value }) => {
      const req: CreateBrokerRequest = {
        brokerId: value.brokerId,
        bootstrapServers: value.bootstrapServers,
        securityProtocol: value.securityProtocol || undefined,
        saslMechanism: value.saslMechanism || undefined,
        saslUsername: value.saslUsername || undefined,
        saslPassword: value.saslPassword || undefined,
        sslTruststorePath: value.sslTruststorePath || undefined,
        sslTruststorePassword: value.sslTruststorePassword || undefined,
        sslKeystorePath: value.sslKeystorePath || undefined,
        sslKeystorePassword: value.sslKeystorePassword || undefined,
      }
      mutation.mutate(req)
    },
  })

  const [protocol, setProtocol] = useState('PLAINTEXT')
  const showSasl = SASL_PROTOCOLS.has(protocol)
  const showSsl = SSL_PROTOCOLS.has(protocol)

  return (
    <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }} onClick={onClose}>
      <div style={{ background: '#fff', borderRadius: 8, padding: '1.5rem 2rem', minWidth: 400, maxWidth: 520, maxHeight: '90vh', overflowY: 'auto', boxShadow: '0 10px 30px rgba(0,0,0,0.2)' }} onClick={e => e.stopPropagation()}>
        <h2 style={{ margin: '0 0 1.25rem', fontSize: 18 }}>Add Broker</h2>
        <form onSubmit={(e) => { e.preventDefault(); void form.handleSubmit() }}>
          <form.Field name="brokerId">
            {(field) => (
              <div style={{ marginBottom: '0.75rem' }}>
                <label style={labelStyle}>Broker ID *</label>
                <input style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)} placeholder="e.g. production, staging" required />
              </div>
            )}
          </form.Field>
          <form.Field name="bootstrapServers">
            {(field) => (
              <div style={{ marginBottom: '0.75rem' }}>
                <label style={labelStyle}>Bootstrap Servers *</label>
                <input style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)} placeholder="broker1:9092,broker2:9092" required />
              </div>
            )}
          </form.Field>
          <form.Field name="securityProtocol">
            {(field) => (
              <div style={{ marginBottom: '0.75rem' }}>
                <label style={labelStyle}>Security Protocol</label>
                <select style={inputStyle} value={field.state.value} onChange={e => { field.handleChange(e.target.value); setProtocol(e.target.value) }}>
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
                  <div style={{ marginBottom: '0.75rem' }}>
                    <label style={labelStyle}>SASL Mechanism</label>
                    <select style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)}>
                      <option value="PLAIN">PLAIN</option>
                      <option value="SCRAM-SHA-256">SCRAM-SHA-256</option>
                      <option value="SCRAM-SHA-512">SCRAM-SHA-512</option>
                    </select>
                  </div>
                )}
              </form.Field>
              <form.Field name="saslUsername">
                {(field) => (
                  <div style={{ marginBottom: '0.75rem' }}>
                    <label style={labelStyle}>SASL Username</label>
                    <input style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)} />
                  </div>
                )}
              </form.Field>
              <form.Field name="saslPassword">
                {(field) => (
                  <div style={{ marginBottom: '0.75rem' }}>
                    <label style={labelStyle}>SASL Password</label>
                    <input type="password" style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)} />
                  </div>
                )}
              </form.Field>
            </>
          )}
          {showSsl && (
            <>
              <form.Field name="sslTruststorePath">
                {(field) => (
                  <div style={{ marginBottom: '0.75rem' }}>
                    <label style={labelStyle}>SSL Truststore Path</label>
                    <input style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)} />
                  </div>
                )}
              </form.Field>
              <form.Field name="sslTruststorePassword">
                {(field) => (
                  <div style={{ marginBottom: '0.75rem' }}>
                    <label style={labelStyle}>SSL Truststore Password</label>
                    <input type="password" style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)} />
                  </div>
                )}
              </form.Field>
              <form.Field name="sslKeystorePath">
                {(field) => (
                  <div style={{ marginBottom: '0.75rem' }}>
                    <label style={labelStyle}>SSL Keystore Path</label>
                    <input style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)} />
                  </div>
                )}
              </form.Field>
              <form.Field name="sslKeystorePassword">
                {(field) => (
                  <div style={{ marginBottom: '1rem' }}>
                    <label style={labelStyle}>SSL Keystore Password</label>
                    <input type="password" style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)} />
                  </div>
                )}
              </form.Field>
            </>
          )}
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

function EditBrokerModal({ broker, onClose }: { broker: BrokerConfig; onClose: () => void }) {
  const qc = useQueryClient()
  const { addToast } = useToast()
  const mutation = useMutation({
    mutationFn: (data: UpdateBrokerRequest) => brokersApi.update(broker.brokerId, data),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['brokers'] })
      addToast('Broker updated', 'success')
      onClose()
    },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const form = useForm({
    defaultValues: {
      brokerId: broker.brokerId,
      bootstrapServers: broker.bootstrapServers,
      securityProtocol: broker.securityProtocol ?? 'PLAINTEXT',
      saslMechanism: broker.saslMechanism ?? 'PLAIN',
      saslUsername: broker.saslUsername ?? '',
      saslPassword: '',
      sslTruststorePath: broker.sslTruststorePath ?? '',
      sslTruststorePassword: '',
      sslKeystorePath: broker.sslKeystorePath ?? '',
      sslKeystorePassword: '',
    },
    onSubmit: async ({ value }) => {
      const req: UpdateBrokerRequest = {
        bootstrapServers: value.bootstrapServers,
        securityProtocol: value.securityProtocol || undefined,
        saslMechanism: value.saslMechanism || undefined,
        saslUsername: value.saslUsername || undefined,
        saslPassword: value.saslPassword || undefined,
        sslTruststorePath: value.sslTruststorePath || undefined,
        sslTruststorePassword: value.sslTruststorePassword || undefined,
        sslKeystorePath: value.sslKeystorePath || undefined,
        sslKeystorePassword: value.sslKeystorePassword || undefined,
      }
      mutation.mutate(req)
    },
  })

  const [protocol, setProtocol] = useState(broker.securityProtocol ?? 'PLAINTEXT')
  const showSasl = SASL_PROTOCOLS.has(protocol)
  const showSsl = SSL_PROTOCOLS.has(protocol)

  return (
    <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }} onClick={onClose}>
      <div style={{ background: '#fff', borderRadius: 8, padding: '1.5rem 2rem', minWidth: 400, maxWidth: 520, maxHeight: '90vh', overflowY: 'auto', boxShadow: '0 10px 30px rgba(0,0,0,0.2)' }} onClick={e => e.stopPropagation()}>
        <h2 style={{ margin: '0 0 1.25rem', fontSize: 18 }}>Edit Broker</h2>
        <form onSubmit={(e) => { e.preventDefault(); void form.handleSubmit() }}>
          <form.Field name="brokerId">
            {(field) => (
              <div style={{ marginBottom: '0.75rem' }}>
                <label style={labelStyle}>Broker ID</label>
                <input style={{ ...inputStyle, background: '#f7fafc' }} value={field.state.value} disabled />
              </div>
            )}
          </form.Field>
          <form.Field name="bootstrapServers">
            {(field) => (
              <div style={{ marginBottom: '0.75rem' }}>
                <label style={labelStyle}>Bootstrap Servers *</label>
                <input style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)} placeholder="broker1:9092,broker2:9092" required />
              </div>
            )}
          </form.Field>
          <form.Field name="securityProtocol">
            {(field) => (
              <div style={{ marginBottom: '0.75rem' }}>
                <label style={labelStyle}>Security Protocol</label>
                <select style={inputStyle} value={field.state.value} onChange={e => { field.handleChange(e.target.value); setProtocol(e.target.value) }}>
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
                  <div style={{ marginBottom: '0.75rem' }}>
                    <label style={labelStyle}>SASL Mechanism</label>
                    <select style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)}>
                      <option value="PLAIN">PLAIN</option>
                      <option value="SCRAM-SHA-256">SCRAM-SHA-256</option>
                      <option value="SCRAM-SHA-512">SCRAM-SHA-512</option>
                    </select>
                  </div>
                )}
              </form.Field>
              <form.Field name="saslUsername">
                {(field) => (
                  <div style={{ marginBottom: '0.75rem' }}>
                    <label style={labelStyle}>SASL Username</label>
                    <input style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)} />
                  </div>
                )}
              </form.Field>
              <form.Field name="saslPassword">
                {(field) => (
                  <div style={{ marginBottom: '0.75rem' }}>
                    <label style={labelStyle}>SASL Password</label>
                    <input type="password" style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)} placeholder={broker.saslPassword ? '••••••••' : ''} />
                  </div>
                )}
              </form.Field>
            </>
          )}
          {showSsl && (
            <>
              <form.Field name="sslTruststorePath">
                {(field) => (
                  <div style={{ marginBottom: '0.75rem' }}>
                    <label style={labelStyle}>SSL Truststore Path</label>
                    <input style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)} />
                  </div>
                )}
              </form.Field>
              <form.Field name="sslTruststorePassword">
                {(field) => (
                  <div style={{ marginBottom: '0.75rem' }}>
                    <label style={labelStyle}>SSL Truststore Password</label>
                    <input type="password" style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)} placeholder={broker.sslTruststorePassword ? '••••••••' : ''} />
                  </div>
                )}
              </form.Field>
              <form.Field name="sslKeystorePath">
                {(field) => (
                  <div style={{ marginBottom: '0.75rem' }}>
                    <label style={labelStyle}>SSL Keystore Path</label>
                    <input style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)} />
                  </div>
                )}
              </form.Field>
              <form.Field name="sslKeystorePassword">
                {(field) => (
                  <div style={{ marginBottom: '1rem' }}>
                    <label style={labelStyle}>SSL Keystore Password</label>
                    <input type="password" style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)} placeholder={broker.sslKeystorePassword ? '••••••••' : ''} />
                  </div>
                )}
              </form.Field>
            </>
          )}
          <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
            <button type="button" onClick={onClose} style={cancelBtnStyle}>Cancel</button>
            <button type="submit" disabled={mutation.isPending} style={primaryBtnStyle}>
              {mutation.isPending ? 'Saving…' : 'Save'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

function BrokersPage() {
  const qc = useQueryClient()
  const { addToast } = useToast()
  const [showAdd, setShowAdd] = useState(false)
  const [editBroker, setEditBroker] = useState<BrokerConfig | null>(null)
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null)

  const { data, isLoading, error } = useQuery({
    queryKey: ['brokers'],
    queryFn: brokersApi.list,
  })

  const deleteMutation = useMutation({
    mutationFn: (brokerId: string) => brokersApi.delete(brokerId),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['brokers'] })
      addToast('Broker deleted', 'success')
    },
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
    colHelper.accessor('brokerId', {
      header: 'Broker ID',
      cell: info => <strong>{info.getValue()}</strong>,
    }),
    colHelper.accessor('bootstrapServers', { header: 'Bootstrap Servers' }),
    colHelper.accessor('securityProtocol', {
      header: 'Security Protocol',
      cell: info => <ProtocolBadge protocol={info.getValue()} />,
    }),
    colHelper.accessor('saslMechanism', {
      header: 'SASL Mechanism',
      cell: info => info.getValue() ?? '—',
    }),
    colHelper.display({
      id: 'actions',
      header: 'Actions',
      cell: ({ row }) => {
        const b = row.original
        return (
          <div style={{ display: 'flex', gap: 6 }}>
            <button
              style={primaryBtnSmall}
              onClick={e => { e.stopPropagation(); setEditBroker(b) }}
            >
              Edit
            </button>
            <button
              style={dangerBtnSmall}
              onClick={e => { e.stopPropagation(); setConfirmDelete(b.brokerId) }}
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
        <h1 style={pageTitle}>Brokers</h1>
        <button style={primaryBtnStyle} onClick={() => setShowAdd(true)}>+ Add Broker</button>
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
                style={{ cursor: 'default' }}
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

// Shared styles
const labelStyle: React.CSSProperties = { display: 'block', marginBottom: 4, fontSize: 13, fontWeight: 600, color: '#4a5568' }
const inputStyle: React.CSSProperties = { width: '100%', padding: '0.4rem 0.6rem', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 14, boxSizing: 'border-box' }
const primaryBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const cancelBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#fff', color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const primaryBtnSmall: React.CSSProperties = { padding: '0.2rem 0.6rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 3, cursor: 'pointer', fontSize: 12 }
const dangerBtnSmall: React.CSSProperties = { padding: '0.2rem 0.6rem', background: '#e53e3e', color: '#fff', border: 'none', borderRadius: 3, cursor: 'pointer', fontSize: 12 }
const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', background: '#fff', borderRadius: 8, overflow: 'hidden', boxShadow: '0 1px 4px rgba(0,0,0,0.1)' }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.6rem 0.75rem', background: '#edf2f7', fontSize: 13, fontWeight: 600, color: '#4a5568', borderBottom: '1px solid #e2e8f0' }
const tdStyle: React.CSSProperties = { padding: '0.55rem 0.75rem', borderBottom: '1px solid #e2e8f0', fontSize: 14 }
const pageTitle: React.CSSProperties = { margin: 0, fontSize: 22, fontWeight: 700 }
