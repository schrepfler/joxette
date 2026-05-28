import { createFileRoute, useNavigate, Link } from '@tanstack/react-router'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { streamsApi, type UpdateStreamRequest } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'
import { useToast } from '../../components/Toast'
import { JsonView } from '../../components/JsonView'

export const Route = createFileRoute('/streams/$streamId')({
  component: StreamDetailPage,
})

function StreamDetailPage() {
  const { streamId } = Route.useParams()
  const navigate = useNavigate()
  const qc = useQueryClient()
  const { addToast } = useToast()
  const [editing, setEditing] = useState(false)

  const streamQuery = useQuery({
    queryKey: ['streams', streamId],
    queryFn: () => streamsApi.get(streamId),
  })

  const deleteMutation = useMutation({
    mutationFn: () => streamsApi.delete(streamId),
    onSuccess: () => {
      addToast(`Stream "${streamId}" deleted`, 'success')
      void navigate({ to: '/streams' })
    },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const updateMutation = useMutation({
    mutationFn: (req: UpdateStreamRequest) => streamsApi.update(streamId, req),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['streams', streamId] })
      addToast('Stream updated', 'success')
      setEditing(false)
    },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const stream = streamQuery.data

  return (
    <Layout>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
        <Link to="/streams" data-testid="breadcrumb-streams" style={{ fontSize: 13, color: '#718096', textDecoration: 'none' }}>
          ← Streams
        </Link>
      </div>

      {streamQuery.isLoading && <LoadingSpinner />}
      {streamQuery.error && <ErrorMessage message={(streamQuery.error as Error).message} />}

      {stream && (
        <div data-testid={`stream-detail-${streamId}`}>
          <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
            <div>
              <h1 data-testid="stream-name" style={{ margin: 0, fontSize: 22, fontWeight: 700 }}>{stream.name}</h1>
              <code data-testid="stream-id" style={{ fontSize: 12, color: '#718096' }}>{stream.id}</code>
            </div>
            <div style={{ display: 'flex', gap: 8 }}>
              <button
                data-testid="btn-edit-stream"
                aria-label={`Edit stream ${stream.name}`}
                aria-expanded={editing}
                style={{ padding: '0.35rem 0.8rem', background: '#ebf8ff', color: '#2b6cb0', border: '1px solid #bee3f8', borderRadius: 4, cursor: 'pointer', fontSize: 13 }}
                onClick={() => setEditing(e => !e)}
              >
                {editing ? 'Cancel Edit' : 'Edit'}
              </button>
              <button
                data-testid="btn-delete-stream"
                aria-label={`Delete stream ${stream.name}`}
                style={{ padding: '0.35rem 0.8rem', background: '#fff', color: '#e53e3e', border: '1px solid #fed7d7', borderRadius: 4, cursor: 'pointer', fontSize: 13 }}
                onClick={() => { if (confirm(`Delete stream "${stream.id}"?`)) deleteMutation.mutate() }}
              >
                Delete
              </button>
            </div>
          </div>

          {/* Definition summary */}
          {!editing && (
            <div
              data-testid="stream-definition-summary"
              style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem', marginBottom: '1.5rem' }}
            >
              <h3 style={{ margin: '0 0 0.75rem', fontSize: 15 }}>Definition</h3>
              <dl style={{ display: 'grid', gridTemplateColumns: 'max-content 1fr', gap: '6px 16px', fontSize: 13 }}>
                <DefinitionRow label="Entity Type" value={stream.entityType} testId="def-entity-type" />
                <DefinitionRow label="Entity ID"   value={stream.entityId ?? '(any — bind at query time)'} testId="def-entity-id" />
                <DefinitionRow label="Output"      value={stream.output ?? 'events'} testId="def-output" />
                {(stream.output === 'state' || stream.output === 'diff') && (
                  <DefinitionRow label="State Fold" value={stream.stateFold ?? 'merge_patch'} testId="def-state-fold" />
                )}
                {stream.sol && <DefinitionRow label="SOL" value={stream.sol} testId="def-sol" mono />}
                {stream.source?.messageTypes && stream.source.messageTypes.length > 0 && (
                  <DefinitionRow label="Message Types" value={stream.source.messageTypes.join(', ')} testId="def-message-types" />
                )}
              </dl>
              <div style={{ marginTop: '0.75rem', fontSize: 11, color: '#a0aec0' }}>
                Created: {stream.createdAt.slice(0, 19).replace('T', ' ')} ·
                Updated: {stream.updatedAt.slice(0, 19).replace('T', ' ')}
              </div>
            </div>
          )}

          {/* Inline edit form */}
          {editing && (
            <EditStreamForm
              stream={stream}
              onSave={req => updateMutation.mutate(req)}
              onCancel={() => setEditing(false)}
              saving={updateMutation.isPending}
            />
          )}

          {/* Pull query panel */}
          {!editing && <PullQueryPanel streamId={streamId} entityId={stream.entityId ?? undefined} />}
        </div>
      )}
    </Layout>
  )
}

// ── Definition row ─────────────────────────────────────────────────────────────

function DefinitionRow({ label, value, testId, mono }: { label: string; value: string; testId?: string; mono?: boolean }) {
  return (
    <>
      <dt style={{ fontWeight: 600, color: '#4a5568' }}>{label}</dt>
      <dd data-testid={testId} style={{ margin: 0, fontFamily: mono ? 'monospace' : undefined, color: '#1a202c' }}>{value}</dd>
    </>
  )
}

// ── Edit form ──────────────────────────────────────────────────────────────────

function EditStreamForm({
  stream,
  onSave,
  onCancel,
  saving,
}: {
  stream: import('../../api/client').StreamDefinition
  onSave: (req: UpdateStreamRequest) => void
  onCancel: () => void
  saving: boolean
}) {
  const [name, setName] = useState(stream.name)
  const [entityType, setEntityType] = useState(stream.entityType)
  const [entityId, setEntityId] = useState(stream.entityId ?? '')
  const [output, setOutput] = useState<'events' | 'state' | 'diff' | 'snapshot'>(stream.output ?? 'events')
  const [stateFold, setStateFold] = useState<'merge_patch' | 'last_value' | 'last_per_topic'>(stream.stateFold ?? 'merge_patch')
  const [sol, setSol] = useState(stream.sol ?? '')

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    onSave({
      name,
      entityType,
      entityId: entityId.trim() || null,
      output: output as UpdateStreamRequest['output'],
      stateFold: output === 'state' || output === 'diff' ? stateFold : null,
      sol: sol.trim() || null,
      source: stream.source ?? null,
    })
  }

  return (
    <form
      data-testid="edit-stream-form"
      aria-label="Edit stream definition"
      onSubmit={handleSubmit}
      style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem', marginBottom: '1.5rem' }}
    >
      <h3 style={{ margin: '0 0 1rem', fontSize: 15 }}>Edit Definition</h3>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
        <LabeledInput label="Name" htmlFor="edit-stream-name" testId="input-edit-stream-name"
          value={name} onChange={setName} required />
        <LabeledInput label="Entity Type" htmlFor="edit-stream-entity-type" testId="input-edit-stream-entity-type"
          value={entityType} onChange={setEntityType} required />
        <LabeledInput label="Entity ID" htmlFor="edit-stream-entity-id" testId="input-edit-stream-entity-id"
          value={entityId} onChange={setEntityId}
          hint="Leave blank for entity-type stream" />
        <div>
          <label htmlFor="edit-stream-output" style={labelStyle}>Output</label>
          <select
            id="edit-stream-output"
            data-testid="select-edit-stream-output"
            value={output}
            onChange={e => setOutput(e.target.value as 'events' | 'state' | 'diff' | 'snapshot')}
            style={{ ...inputStyle, cursor: 'pointer' }}
          >
            <option value="events">events</option>
            <option value="state">state</option>
            <option value="diff">diff</option>
            <option value="snapshot">snapshot</option>
          </select>
        </div>
        {(output === 'state' || output === 'diff') && (
          <div>
            <label htmlFor="edit-stream-state-fold" style={labelStyle}>State Fold Strategy</label>
            <select
              id="edit-stream-state-fold"
              data-testid="select-edit-stream-state-fold"
              value={stateFold}
              onChange={e => setStateFold(e.target.value as 'merge_patch' | 'last_value' | 'last_per_topic')}
              style={{ ...inputStyle, cursor: 'pointer' }}
            >
              <option value="merge_patch">merge_patch</option>
              <option value="last_value">last_value</option>
              <option value="last_per_topic">last_per_topic</option>
            </select>
          </div>
        )}
        <LabeledInput label="SOL Expression" htmlFor="edit-stream-sol" testId="input-edit-stream-sol"
          value={sol} onChange={setSol} hint="Optional" />
      </div>
      <div style={{ display: 'flex', gap: 10, marginTop: '1.25rem', justifyContent: 'flex-end' }}>
        <button type="button" data-testid="btn-cancel-edit-stream" aria-label="Cancel edit"
          onClick={onCancel}
          style={{ padding: '0.45rem 1rem', background: '#fff', color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 14 }}
        >
          Cancel
        </button>
        <button type="submit" data-testid="btn-save-edit-stream" aria-label="Save stream changes"
          disabled={saving}
          style={{ padding: '0.45rem 1rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14, opacity: saving ? 0.6 : 1 }}
        >
          {saving ? 'Saving…' : 'Save Changes'}
        </button>
      </div>
    </form>
  )
}

function LabeledInput({ label, htmlFor, testId, value, onChange, required, hint }: {
  label: string; htmlFor: string; testId: string
  value: string; onChange: (v: string) => void; required?: boolean; hint?: string
}) {
  return (
    <div>
      <label htmlFor={htmlFor} style={labelStyle}>{label}</label>
      {hint && <div style={{ fontSize: 11, color: '#718096', marginBottom: 4 }}>{hint}</div>}
      <input
        id={htmlFor}
        data-testid={testId}
        value={value}
        onChange={e => onChange(e.target.value)}
        required={required}
        style={inputStyle}
      />
    </div>
  )
}

// ── Pull query panel ───────────────────────────────────────────────────────────

function PullQueryPanel({ streamId, entityId }: { streamId: string; entityId?: string }) {
  const [queryEntityId, setQueryEntityId] = useState(entityId ?? '')
  const [triggered, setTriggered] = useState(false)

  const pullQuery = useQuery({
    queryKey: ['streams', streamId, 'pull', queryEntityId],
    queryFn: () => streamsApi.pull(streamId, queryEntityId.trim() || undefined),
    enabled: triggered,
  })

  return (
    <div
      data-testid="pull-query-panel"
      style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem' }}
    >
      <h3 style={{ margin: '0 0 0.75rem', fontSize: 15 }}>Pull Query</h3>
      <div style={{ display: 'flex', gap: 10, alignItems: 'flex-end', marginBottom: '1rem', flexWrap: 'wrap' }}>
        {!entityId && (
          <div>
            <label htmlFor="pull-entity-id" style={{ display: 'block', fontSize: 13, fontWeight: 600, color: '#4a5568', marginBottom: 4 }}>
              Entity ID
            </label>
            <input
              id="pull-entity-id"
              data-testid="input-pull-entity-id"
              value={queryEntityId}
              onChange={e => { setQueryEntityId(e.target.value); setTriggered(false) }}
              placeholder="e.g. order-789"
              style={{ padding: '0.4rem 0.6rem', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 14, width: 220 }}
            />
          </div>
        )}
        <button
          data-testid="btn-run-pull-query"
          aria-label="Run pull query against this stream"
          style={{ padding: '0.45rem 1rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14 }}
          onClick={() => { setTriggered(true); }}
        >
          Run
        </button>
        {triggered && pullQuery.data && (
          <span data-testid="pull-query-count" style={{ fontSize: 13, color: '#718096' }}>
            {pullQuery.data.data.length} result{pullQuery.data.data.length !== 1 ? 's' : ''}
            {pullQuery.data.hasMore ? ' (more available)' : ''}
          </span>
        )}
      </div>
      {triggered && pullQuery.isLoading && <LoadingSpinner />}
      {triggered && pullQuery.error && <ErrorMessage message={(pullQuery.error as Error).message} />}
      {triggered && pullQuery.data && pullQuery.data.data.length > 0 && (
        <div data-testid="pull-query-results" style={{ maxHeight: 400, overflowY: 'auto' }}>
          {pullQuery.data.data.map((record, i) => (
            <div key={i} data-testid={`pull-result-${i}`} style={{ marginBottom: 8, fontSize: 12 }}>
              <JsonView src={record as object} />
            </div>
          ))}
        </div>
      )}
      {triggered && pullQuery.data && pullQuery.data.data.length === 0 && (
        <p data-testid="pull-query-empty" style={{ color: '#a0aec0', fontSize: 13, margin: 0 }}>No results.</p>
      )}
    </div>
  )
}

const labelStyle: React.CSSProperties = { display: 'block', fontSize: 13, fontWeight: 600, color: '#4a5568', marginBottom: 4 }
const inputStyle: React.CSSProperties = {
  width: '100%', padding: '0.4rem 0.6rem', border: '1px solid #cbd5e0',
  borderRadius: 4, fontSize: 14, boxSizing: 'border-box',
}
