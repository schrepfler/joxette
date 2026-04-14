import { createFileRoute, Link, useNavigate } from '@tanstack/react-router'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
} from '@tanstack/react-table'
import { useForm } from '@tanstack/react-form'
import { useState } from 'react'
import { brokersApi, topicsApi, type BrokerConfig, type UpdateBrokerRequest, type TopicConfig } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'
import { ConfirmDialog } from '../../components/ConfirmDialog'
import { useToast } from '../../components/Toast'

export const Route = createFileRoute('/brokers/$brokerId')({
  component: BrokerDetailPage,
})

const topicColHelper = createColumnHelper<TopicConfig>()

const SASL_PROTOCOLS = new Set(['SASL_PLAINTEXT', 'SASL_SSL'])
const SSL_PROTOCOLS = new Set(['SASL_SSL', 'SSL'])

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
    <div style={overlayStyle} onClick={onClose}>
      <div style={modalStyle} onClick={e => e.stopPropagation()}>
        <h2 style={{ margin: '0 0 1.25rem', fontSize: 18 }}>Edit Broker</h2>
        <form onSubmit={(e) => { e.preventDefault(); void form.handleSubmit() }}>
          <div style={{ marginBottom: '0.75rem' }}>
            <label style={labelStyle}>Broker ID</label>
            <input style={{ ...inputStyle, background: '#f7fafc' }} value={broker.brokerId} disabled />
          </div>
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

function DetailRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div style={{ display: 'flex', padding: '0.55rem 0', borderBottom: '1px solid #e2e8f0' }}>
      <div style={{ width: 200, flexShrink: 0, fontSize: 13, fontWeight: 600, color: '#718096' }}>{label}</div>
      <div style={{ fontSize: 14, color: '#1a202c' }}>{value}</div>
    </div>
  )
}

function BrokerDetailPage() {
  const { brokerId } = Route.useParams()
  const qc = useQueryClient()
  const navigate = useNavigate()
  const { addToast } = useToast()

  const [showEdit, setShowEdit] = useState(false)
  const [showConfirmDelete, setShowConfirmDelete] = useState(false)

  const { data: broker, isLoading, error } = useQuery({
    queryKey: ['brokers', brokerId],
    queryFn: () => brokersApi.get(brokerId),
  })

  const { data: allTopics } = useQuery({
    queryKey: ['topics'],
    queryFn: topicsApi.list,
  })

  const topicsForBroker = (allTopics ?? []).filter(t => t.brokerId === brokerId)

  const deleteMutation = useMutation({
    mutationFn: () => brokersApi.delete(brokerId),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['brokers'] })
      addToast('Broker deleted', 'success')
      void navigate({ to: '/brokers' })
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

  const topicColumns = [
    topicColHelper.accessor('topic', {
      header: 'Topic',
      cell: info => <strong>{info.getValue()}</strong>,
    }),
    topicColHelper.accessor('mode', { header: 'Mode' }),
    topicColHelper.accessor('paused', {
      header: 'Paused',
      cell: info => (
        <span style={{
          background: info.getValue() ? '#fed7d7' : '#edf2f7',
          color: info.getValue() ? '#9b2c2c' : '#4a5568',
          padding: '2px 8px', borderRadius: 10, fontSize: 12,
        }}>
          {info.getValue() ? 'Yes' : 'No'}
        </span>
      ),
    }),
    topicColHelper.accessor('active', {
      header: 'Active',
      cell: info => (
        <span style={{
          background: info.getValue() ? '#c6f6d5' : '#edf2f7',
          color: info.getValue() ? '#276749' : '#4a5568',
          padding: '2px 8px', borderRadius: 10, fontSize: 12,
        }}>
          {info.getValue() ? 'Yes' : 'No'}
        </span>
      ),
    }),
  ]

  const topicTable = useReactTable({
    data: topicsForBroker,
    columns: topicColumns,
    getCoreRowModel: getCoreRowModel(),
  })

  return (
    <Layout>
      {isLoading && <LoadingSpinner />}
      {error && <ErrorMessage message={(error as Error).message} />}
      {broker && (
        <>
          {/* Header */}
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700 }}>{broker.brokerId}</h1>
              <ProtocolBadge protocol={broker.securityProtocol} />
            </div>
            <div style={{ display: 'flex', gap: 8 }}>
              <button style={primaryBtnStyle} onClick={() => setShowEdit(true)}>Edit</button>
              <button style={dangerBtnStyle} onClick={() => setShowConfirmDelete(true)}>Delete</button>
            </div>
          </div>

          {/* Configuration panel */}
          <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem', marginBottom: '1.5rem' }}>
            <h3 style={{ margin: '0 0 0.75rem', fontSize: 15 }}>Configuration</h3>
            <DetailRow label="Broker ID" value={broker.brokerId} />
            <DetailRow label="Bootstrap Servers" value={<span style={{ fontFamily: 'monospace', fontSize: 13 }}>{broker.bootstrapServers}</span>} />
            <DetailRow label="Security Protocol" value={<ProtocolBadge protocol={broker.securityProtocol} />} />
            <DetailRow label="SASL Mechanism" value={broker.saslMechanism ?? '—'} />
            <DetailRow label="SASL Username" value={broker.saslUsername ?? '—'} />
            <DetailRow
              label="SASL Password"
              value={broker.saslPassword != null ? <span style={{ color: '#718096', fontFamily: 'monospace' }}>••••••••</span> : <span style={{ color: '#a0aec0' }}>Not configured</span>}
            />
            <DetailRow label="SSL Truststore Path" value={broker.sslTruststorePath ?? '—'} />
            <DetailRow
              label="SSL Truststore Password"
              value={broker.sslTruststorePassword != null ? <span style={{ color: '#718096', fontFamily: 'monospace' }}>••••••••</span> : <span style={{ color: '#a0aec0' }}>Not configured</span>}
            />
            <DetailRow label="SSL Keystore Path" value={broker.sslKeystorePath ?? '—'} />
            <DetailRow
              label="SSL Keystore Password"
              value={broker.sslKeystorePassword != null ? <span style={{ color: '#718096', fontFamily: 'monospace' }}>••••••••</span> : <span style={{ color: '#a0aec0' }}>Not configured</span>}
            />
          </div>

          {/* Topics using this broker */}
          <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem' }}>
            <h3 style={{ margin: '0 0 0.75rem', fontSize: 15 }}>Topics Using This Broker</h3>
            {topicsForBroker.length === 0 ? (
              <p style={{ margin: 0, fontSize: 14, color: '#a0aec0', fontStyle: 'italic' }}>No topics are using this broker yet.</p>
            ) : (
              <table style={tableStyle}>
                <thead>
                  {topicTable.getHeaderGroups().map(hg => (
                    <tr key={hg.id}>
                      {hg.headers.map(h => <th key={h.id} style={thStyle}>{flexRender(h.column.columnDef.header, h.getContext())}</th>)}
                    </tr>
                  ))}
                </thead>
                <tbody>
                  {topicTable.getRowModel().rows.map(row => (
                    <Link
                      key={row.id}
                      to="/topics/$topic"
                      params={{ topic: row.original.topic }}
                      style={{ display: 'contents', textDecoration: 'none', color: 'inherit' }}
                    >
                      <tr
                        style={{ cursor: 'pointer' }}
                        onMouseEnter={e => (e.currentTarget.style.background = '#ebf8ff')}
                        onMouseLeave={e => (e.currentTarget.style.background = '')}
                      >
                        {row.getVisibleCells().map(cell => (
                          <td key={cell.id} style={tdStyle}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>
                        ))}
                      </tr>
                    </Link>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </>
      )}

      {showEdit && broker && <EditBrokerModal broker={broker} onClose={() => setShowEdit(false)} />}
      {showConfirmDelete && (
        <ConfirmDialog
          message={`Delete broker "${brokerId}"? Topics using this broker will need to be reassigned.`}
          onConfirm={() => { deleteMutation.mutate(); setShowConfirmDelete(false) }}
          onCancel={() => setShowConfirmDelete(false)}
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
const dangerBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#e53e3e', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', background: '#fff', fontSize: 13 }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.5rem 0.6rem', background: '#edf2f7', fontWeight: 600, color: '#4a5568', borderBottom: '1px solid #e2e8f0' }
const tdStyle: React.CSSProperties = { padding: '0.45rem 0.6rem', borderBottom: '1px solid #e2e8f0' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const modalStyle: React.CSSProperties = { background: '#fff', borderRadius: 8, padding: '1.5rem 2rem', minWidth: 400, maxWidth: 520, maxHeight: '90vh', overflowY: 'auto', boxShadow: '0 10px 30px rgba(0,0,0,0.2)' }
