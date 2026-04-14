import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useForm } from '@tanstack/react-form'
import { topicsApi, brokersApi, type CreateTopicRequest } from '../api/client'
import { useToast } from './Toast'

interface AddTopicModalProps {
  onClose: () => void
  defaultTopic?: string
  defaultBrokerId?: string
}

export function AddTopicModal({ onClose, defaultTopic = '', defaultBrokerId = '' }: AddTopicModalProps) {
  const qc = useQueryClient()
  const { addToast } = useToast()
  const { data: brokers = [] } = useQuery({ queryKey: ['brokers'], queryFn: brokersApi.list })
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
      topic: defaultTopic,
      mode: 'general',
      brokerId: defaultBrokerId,
      consumerGroup: '',
      retentionDays: '',
      startFrom: '',
    },
    onSubmit: async ({ value }) => {
      const req: CreateTopicRequest = {
        topic: value.topic,
        mode: value.mode || undefined,
        brokerId: value.brokerId || undefined,
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
          <form.Field name="brokerId">
            {(field) => (
              <div style={{ marginBottom: '0.75rem' }}>
                <label style={labelStyle}>Broker</label>
                <select style={inputStyle} value={field.state.value} onChange={e => field.handleChange(e.target.value)}>
                  <option value="">(default)</option>
                  {brokers.map(b => (
                    <option key={b.brokerId} value={b.brokerId}>{b.brokerId} — {b.bootstrapServers}</option>
                  ))}
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

const labelStyle: React.CSSProperties = { display: 'block', marginBottom: 4, fontSize: 13, fontWeight: 600, color: '#4a5568' }
const inputStyle: React.CSSProperties = { width: '100%', padding: '0.4rem 0.6rem', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 14, boxSizing: 'border-box' }
const primaryBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const cancelBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#fff', color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
