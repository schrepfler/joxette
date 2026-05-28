import { useEffect, useId, useRef } from 'react'
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
  const titleId = useId()
  const firstInputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    firstInputRef.current?.focus()
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose() }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [onClose])
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
      createKafkaTopic: false,
      numPartitions: '1',
      replicationFactor: '1',
    },
    onSubmit: async ({ value }) => {
      mutation.mutate({
        topic: value.topic,
        mode: value.mode || undefined,
        brokerId: value.brokerId || undefined,
        consumerGroup: value.consumerGroup || undefined,
        retentionDays: value.retentionDays ? Number(value.retentionDays) : undefined,
        startFrom: value.startFrom || undefined,
        createKafkaTopicIfAbsent: value.createKafkaTopic || undefined,
        numPartitions: value.createKafkaTopic && value.numPartitions ? Number(value.numPartitions) : undefined,
        replicationFactor: value.createKafkaTopic && value.replicationFactor ? Number(value.replicationFactor) : undefined,
      })
    },
  })

  return (
    <div className="jx-overlay" onClick={onClose} aria-hidden="true">
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        className="jx-modal"
        style={{ minWidth: 400, maxWidth: 520 }}
        onClick={(e) => e.stopPropagation()}
      >
        <h2 id={titleId} style={modalH2}>Add Topic</h2>
        <form onSubmit={(e) => { e.preventDefault(); void form.handleSubmit() }}>
          <form.Field name="topic">
            {(field) => (
              <div style={fieldWrap}>
                <label style={labelStyle}>Topic *</label>
                <input ref={firstInputRef} className="jx-input-box" value={field.state.value} onChange={e => field.handleChange(e.target.value)} required />
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
              <div style={fieldWrap}>
                <label style={labelStyle}>Start From</label>
                <input className="jx-input-box" value={field.state.value} onChange={e => field.handleChange(e.target.value)} placeholder="earliest / latest / ISO date" />
              </div>
            )}
          </form.Field>

          <form.Field name="createKafkaTopic">
            {(field) => (
              <div style={{ ...fieldWrap, borderTop: '1px solid var(--rule)', paddingTop: 12, marginTop: 4 }}>
                <label style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer' }}>
                  <input
                    type="checkbox"
                    checked={field.state.value}
                    onChange={e => field.handleChange(e.target.checked)}
                    style={{ width: 14, height: 14, accentColor: 'var(--accent)', cursor: 'pointer' }}
                  />
                  <span style={{ ...labelStyle, marginBottom: 0 }}>Create Kafka topic if it doesn't exist</span>
                </label>
                <p style={{ margin: '4px 0 0 22px', fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>
                  Requires CREATE permission on the broker credentials.
                </p>
              </div>
            )}
          </form.Field>

          <form.Subscribe selector={s => s.values.createKafkaTopic}>
            {(createKafkaTopic) => createKafkaTopic && (
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 12, paddingLeft: 22 }}>
                <form.Field name="numPartitions">
                  {(field) => (
                    <div>
                      <label style={labelStyle}>Partitions</label>
                      <input
                        type="number"
                        min={1}
                        className="jx-input-box"
                        value={field.state.value}
                        onChange={e => field.handleChange(e.target.value)}
                      />
                    </div>
                  )}
                </form.Field>
                <form.Field name="replicationFactor">
                  {(field) => (
                    <div>
                      <label style={labelStyle}>Replication Factor</label>
                      <input
                        type="number"
                        min={1}
                        className="jx-input-box"
                        value={field.state.value}
                        onChange={e => field.handleChange(e.target.value)}
                      />
                    </div>
                  )}
                </form.Field>
              </div>
            )}
          </form.Subscribe>

          <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 8 }}>
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
