import { createFileRoute, Link } from '@tanstack/react-router'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { streamsApi, type StreamDefinition, type CreateStreamRequest } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'
import { useToast } from '../../components/Toast'

export const Route = createFileRoute('/streams/')({
  component: StreamsPage,
})

function StreamsPage() {
  const qc = useQueryClient()
  const { addToast } = useToast()
  const [showCreate, setShowCreate] = useState(false)

  const streamsQuery = useQuery({
    queryKey: ['streams'],
    queryFn: () => streamsApi.list(),
  })

  const deleteMutation = useMutation({
    mutationFn: (id: string) => streamsApi.delete(id),
    onSuccess: (_, id) => {
      void qc.invalidateQueries({ queryKey: ['streams'] })
      addToast(`Stream "${id}" deleted`, 'success')
    },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  return (
    <Layout>
      <div
        style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}
        data-testid="streams-page-header"
      >
        <div>
          <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700 }}>Named Derived Streams</h1>
          <p style={{ margin: '4px 0 0', fontSize: 13, color: '#718096' }}>
            Stored query definitions over entity event histories — pull or push.
          </p>
        </div>
        <button
          data-testid="btn-create-stream"
          aria-label="Create a new stream definition"
          style={{ padding: '0.45rem 1rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14 }}
          onClick={() => setShowCreate(true)}
        >
          + New Stream
        </button>
      </div>

      {streamsQuery.isLoading && <LoadingSpinner />}
      {streamsQuery.error && <ErrorMessage message={(streamsQuery.error as Error).message} />}

      {streamsQuery.data && streamsQuery.data.length === 0 && (
        <div
          data-testid="streams-empty"
          style={{ textAlign: 'center', padding: '3rem', color: '#a0aec0', fontSize: 14, border: '1px dashed #e2e8f0', borderRadius: 8 }}
        >
          No stream definitions yet. Create one to save a reusable entity query.
        </div>
      )}

      {streamsQuery.data && streamsQuery.data.length > 0 && (
        <div data-testid="streams-list" style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          {streamsQuery.data.map(s => (
            <StreamCard
              key={s.id}
              stream={s}
              onDelete={() => deleteMutation.mutate(s.id)}
            />
          ))}
        </div>
      )}

      {showCreate && (
        <CreateStreamModal
          onClose={() => setShowCreate(false)}
          onCreated={() => {
            void qc.invalidateQueries({ queryKey: ['streams'] })
            setShowCreate(false)
          }}
        />
      )}
    </Layout>
  )
}

// ── Stream card ────────────────────────────────────────────────────────────────

function StreamCard({ stream, onDelete }: { stream: StreamDefinition; onDelete: () => void }) {
  const [confirmDelete, setConfirmDelete] = useState(false)

  return (
    <div
      data-testid={`stream-card-${stream.id}`}
      role="article"
      aria-label={`Stream ${stream.name}`}
      style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem' }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 4 }}>
            <Link
              to="/streams/$streamId"
              params={{ streamId: stream.id }}
              data-testid={`stream-link-${stream.id}`}
              style={{ fontSize: 16, fontWeight: 700, color: '#2b6cb0', textDecoration: 'none' }}
            >
              {stream.name}
            </Link>
            <code
              data-testid={`stream-id-${stream.id}`}
              style={{ fontSize: 11, background: '#f7fafc', border: '1px solid #e2e8f0', borderRadius: 4, padding: '1px 6px', color: '#718096' }}
            >
              {stream.id}
            </code>
          </div>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', fontSize: 12, color: '#718096' }}>
            <span data-testid={`stream-entity-type-${stream.id}`}>
              <strong>Entity type:</strong> {stream.entityType}
            </span>
            {stream.entityId && (
              <span data-testid={`stream-entity-id-${stream.id}`}>
                · <strong>Entity ID:</strong> {stream.entityId}
              </span>
            )}
            {stream.output && stream.output !== 'events' && (
              <span data-testid={`stream-output-${stream.id}`}>
                · <strong>Output:</strong> {stream.output}
              </span>
            )}
            {stream.sol && (
              <span data-testid={`stream-sol-${stream.id}`}>
                · <strong>SOL:</strong> <code style={{ fontFamily: 'monospace' }}>{stream.sol.slice(0, 60)}{stream.sol.length > 60 ? '…' : ''}</code>
              </span>
            )}
          </div>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <Link
            to="/streams/$streamId"
            params={{ streamId: stream.id }}
            data-testid={`btn-view-stream-${stream.id}`}
            aria-label={`View stream ${stream.name}`}
            style={{ padding: '0.3rem 0.75rem', background: '#ebf8ff', color: '#2b6cb0', border: '1px solid #bee3f8', borderRadius: 4, fontSize: 13, textDecoration: 'none' }}
          >
            View
          </Link>
          {!confirmDelete ? (
            <button
              data-testid={`btn-delete-stream-${stream.id}`}
              aria-label={`Delete stream ${stream.name}`}
              style={{ padding: '0.3rem 0.75rem', background: '#fff', color: '#e53e3e', border: '1px solid #fed7d7', borderRadius: 4, cursor: 'pointer', fontSize: 13 }}
              onClick={() => setConfirmDelete(true)}
            >
              Delete
            </button>
          ) : (
            <div style={{ display: 'flex', gap: 6 }}>
              <button
                data-testid={`btn-confirm-delete-stream-${stream.id}`}
                aria-label={`Confirm delete stream ${stream.name}`}
                style={{ padding: '0.3rem 0.75rem', background: '#e53e3e', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 13 }}
                onClick={onDelete}
              >
                Confirm Delete
              </button>
              <button
                data-testid={`btn-cancel-delete-stream-${stream.id}`}
                aria-label="Cancel delete"
                style={{ padding: '0.3rem 0.75rem', background: '#fff', color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 13 }}
                onClick={() => setConfirmDelete(false)}
              >
                Cancel
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

// ── Create stream modal ────────────────────────────────────────────────────────

function CreateStreamModal({ onClose, onCreated }: { onClose: () => void; onCreated: () => void }) {
  const { addToast } = useToast()
  const [id, setId] = useState('')
  const [name, setName] = useState('')
  const [entityType, setEntityType] = useState('')
  const [entityId, setEntityId] = useState('')
  const [output, setOutput] = useState<string>('events')
  const [sol, setSol] = useState('')
  const [stateFold, setStateFold] = useState<string>('merge_patch')

  const createMutation = useMutation({
    mutationFn: (req: CreateStreamRequest) => streamsApi.create(req),
    onSuccess: () => {
      addToast('Stream created', 'success')
      onCreated()
    },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    createMutation.mutate({
      id: id.trim(),
      name: name.trim(),
      entityType: entityType.trim(),
      entityId: entityId.trim() || null,
      sol: sol.trim() || null,
      output: output as CreateStreamRequest['output'],
      stateFold: output === 'state' || output === 'diff' ? stateFold : null,
    })
  }

  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-labelledby="create-stream-dialog-title"
      data-testid="create-stream-dialog"
      style={{
        position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)',
        display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000,
      }}
      onClick={e => { if (e.target === e.currentTarget) onClose() }}
    >
      <div style={{ background: '#fff', borderRadius: 8, padding: '1.5rem', width: 480, maxWidth: '90vw', maxHeight: '90vh', overflowY: 'auto' }}>
        <h2 id="create-stream-dialog-title" style={{ margin: '0 0 1rem', fontSize: 18 }}>New Stream Definition</h2>
        <form data-testid="create-stream-form" onSubmit={handleSubmit}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
            <FormField label="ID" htmlFor="stream-id" hint="Lowercase letters, digits, hyphens, underscores">
              <input
                id="stream-id"
                data-testid="input-stream-id"
                value={id}
                onChange={e => setId(e.target.value)}
                required
                pattern="^[a-z][a-z0-9_-]*$"
                placeholder="order-lifecycle"
                style={inputStyle}
              />
            </FormField>
            <FormField label="Name" htmlFor="stream-name">
              <input
                id="stream-name"
                data-testid="input-stream-name"
                value={name}
                onChange={e => setName(e.target.value)}
                required
                placeholder="Order Lifecycle"
                style={inputStyle}
              />
            </FormField>
            <FormField label="Entity Type" htmlFor="stream-entity-type">
              <input
                id="stream-entity-type"
                data-testid="input-stream-entity-type"
                value={entityType}
                onChange={e => setEntityType(e.target.value)}
                required
                placeholder="order"
                style={inputStyle}
              />
            </FormField>
            <FormField label="Entity ID" htmlFor="stream-entity-id" hint="Leave blank for entity-type stream (bind at query time)">
              <input
                id="stream-entity-id"
                data-testid="input-stream-entity-id"
                value={entityId}
                onChange={e => setEntityId(e.target.value)}
                placeholder="(optional — binds to all entities of this type)"
                style={inputStyle}
              />
            </FormField>
            <FormField label="Output" htmlFor="stream-output">
              <select
                id="stream-output"
                data-testid="select-stream-output"
                value={output}
                onChange={e => setOutput(e.target.value)}
                style={{ ...inputStyle, cursor: 'pointer' }}
              >
                <option value="events">events — raw event stream</option>
                <option value="state">state — folded current state</option>
                <option value="diff">diff — event changelog with before/after</option>
                <option value="snapshot">snapshot — state at a point in time</option>
              </select>
            </FormField>
            {(output === 'state' || output === 'diff') && (
              <FormField label="State Fold Strategy" htmlFor="stream-state-fold">
                <select
                  id="stream-state-fold"
                  data-testid="select-stream-state-fold"
                  value={stateFold}
                  onChange={e => setStateFold(e.target.value)}
                  style={{ ...inputStyle, cursor: 'pointer' }}
                >
                  <option value="merge_patch">merge_patch — RFC 7396 JSON merge patch</option>
                  <option value="last_value">last_value — last event wins</option>
                  <option value="last_per_topic">last_per_topic — last per source topic merged</option>
                </select>
              </FormField>
            )}
            <FormField label="SOL Expression" htmlFor="stream-sol" hint="Optional sequence pattern">
              <input
                id="stream-sol"
                data-testid="input-stream-sol"
                value={sol}
                onChange={e => setSol(e.target.value)}
                placeholder="(optional)"
                style={inputStyle}
              />
            </FormField>
          </div>
          <div style={{ display: 'flex', gap: 10, marginTop: '1.25rem', justifyContent: 'flex-end' }}>
            <button
              type="button"
              data-testid="btn-cancel-create-stream"
              aria-label="Cancel"
              onClick={onClose}
              style={{ padding: '0.45rem 1rem', background: '#fff', color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 14 }}
            >
              Cancel
            </button>
            <button
              type="submit"
              data-testid="btn-submit-create-stream"
              aria-label="Create stream definition"
              disabled={createMutation.isPending}
              style={{ padding: '0.45rem 1rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14, opacity: createMutation.isPending ? 0.6 : 1 }}
            >
              {createMutation.isPending ? 'Creating…' : 'Create Stream'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

function FormField({ label, htmlFor, hint, children }: {
  label: string
  htmlFor: string
  hint?: string
  children: React.ReactNode
}) {
  return (
    <div>
      <label htmlFor={htmlFor} style={{ display: 'block', fontSize: 13, fontWeight: 600, color: '#4a5568', marginBottom: 4 }}>
        {label}
      </label>
      {hint && <div style={{ fontSize: 11, color: '#718096', marginBottom: 4 }}>{hint}</div>}
      {children}
    </div>
  )
}

const inputStyle: React.CSSProperties = {
  width: '100%', padding: '0.4rem 0.6rem', border: '1px solid #cbd5e0',
  borderRadius: 4, fontSize: 14, boxSizing: 'border-box',
}
