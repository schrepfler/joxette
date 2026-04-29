import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useForm } from '@tanstack/react-form'
import { topicsApi, brokersApi, type CreateTopicRequest } from '../api/client'
import { useToast } from './Toast'
import { modalH2, labelStyle, primaryBtnStyle, cancelBtnStyle } from '../styles/shared'

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
      mutation.mutate({
        topic: value.topic,
        mode: value.mode || undefined,
        brokerId: value.brokerId || undefined,
        consumerGroup: value.consumerGroup || undefined,
        retentionDays: value.retentionDays ? Number(value.retentionDays) : undefined,
        startFrom: value.startFrom || undefined,
      })
    },
  })

  return (
    <div className="jx-overlay" onClick={onClose}>
      <div
        className="jx-modal"
        style={{ minWidth: 400, maxWidth: 520 }}
        onClick={(e) => e.stopPropagation()}
      >
        <h2 style={modalH2}>Add Topic</h2>
        <form onSubmit={(e) => { e.preventDefault(); void form.handleSubmit() }}>
          <form.Field name="topic">
            {(field) => (
              <div style={fieldWrap}>
                <label style={labelStyle}>Topic *</label>
                <input className="jx-input-box" value={field.state.value} onChange={e => field.handleChange(e.target.value)} required />
              </div>
            )}
          </form.Field>
          <form.Field name="mode">
            {(field) => (
              <div style={fieldWrap}>
                <label style={labelStyle}>Mode</label>
                <select className="jx-input-box" value={field.state.value} onChange={e => field.handleChange(e.target.value)}>
                  <option value="general">general</option>
                  <option value="entity_only">entity_only</option>
                  <option value="both">both</option>
                </select>
              </div>
            )}
          </form.Field>
          <form.Field name="brokerId">
            {(field) => (
              <div style={fieldWrap}>
                <label style={labelStyle}>Broker</label>
                <select className="jx-input-box" value={field.state.value} onChange={e => field.handleChange(e.target.value)}>
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
              <div style={fieldWrap}>
                <label style={labelStyle}>Consumer Group</label>
                <input className="jx-input-box" value={field.state.value} onChange={e => field.handleChange(e.target.value)} />
              </div>
            )}
          </form.Field>
          <form.Field name="retentionDays">
            {(field) => (
              <div style={fieldWrap}>
                <label style={labelStyle}>Retention Days</label>
                <input type="number" className="jx-input-box" value={field.state.value} onChange={e => field.handleChange(e.target.value)} />
              </div>
            )}
          </form.Field>
          <form.Field name="startFrom">
            {(field) => (
              <div style={{ ...fieldWrap, marginBottom: 20 }}>
                <label style={labelStyle}>Start From</label>
                <input className="jx-input-box" value={field.state.value} onChange={e => field.handleChange(e.target.value)} placeholder="earliest / latest / ISO date" />
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

const fieldWrap: React.CSSProperties = { marginBottom: 12 }
