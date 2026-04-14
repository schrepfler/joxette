import { createFileRoute, useRouterState } from '@tanstack/react-router'
import { useState, useEffect } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  brokersApi,
  topicsApi,
  entitiesApi,
  matchersApi,
  type Header,
  type MatcherPreviewResponse,
} from '../../api/client'
import { Layout } from '../../components/Layout'
import { useToast } from '../../components/Toast'

export const Route = createFileRoute('/brokers/$brokerId_/topics/$topic_/playground')({
  component: MatcherPlaygroundPage,
})

// ---- Styles ----

const labelStyle: React.CSSProperties = { display: 'block', marginBottom: 4, fontSize: 13, fontWeight: 600, color: '#4a5568' }
const inputStyle: React.CSSProperties = { width: '100%', padding: '0.4rem 0.6rem', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 14, boxSizing: 'border-box' }
const primaryBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const cancelBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#fff', color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const secondaryBtnSmall: React.CSSProperties = { padding: '0.35rem 0.8rem', background: '#fff', color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 13 }
const dangerBtnSmall: React.CSSProperties = { padding: '0.2rem 0.6rem', background: '#e53e3e', color: '#fff', border: 'none', borderRadius: 3, cursor: 'pointer', fontSize: 12 }
const sectionTitle: React.CSSProperties = { margin: '0 0 1rem', fontSize: 17, fontWeight: 700 }
const hintText: React.CSSProperties = { fontSize: 12, color: '#718096', marginTop: 4 }

const previewBox = (color: string): React.CSSProperties => ({
  padding: '0.6rem 0.75rem',
  borderRadius: 4,
  marginTop: '0.75rem',
  background: color,
  fontSize: 13,
  fontWeight: 600,
})

// ---- Component ----

function MatcherPlaygroundPage() {
  const { brokerId, topic } = Route.useParams()
  const routerState = useRouterState({ select: s => s.location.state }) as {
    key?: string | null
    value?: string
    valueEncoding?: string
    headers?: Header[]
  } | null

  const { addToast } = useToast()
  const qc = useQueryClient()

  // Left panel state
  const [msgKey, setMsgKey] = useState(routerState?.key ?? '')
  const [msgValue, setMsgValue] = useState(routerState?.value ?? '')
  const [msgHeaders, setMsgHeaders] = useState<Header[]>(routerState?.headers ?? [])
  const [valueError, setValueError] = useState(false)
  const isBinary = routerState?.valueEncoding === 'base64'

  // Right panel state
  const [idSource, setIdSource] = useState<'key' | 'value' | 'header'>('value')
  const [idExpression, setIdExpression] = useState('')
  const [messageType, setMessageType] = useState('')
  const [debouncedExpression, setDebouncedExpression] = useState('')
  const [previewResult, setPreviewResult] = useState<MatcherPreviewResponse | null>(null)
  const [saveMode, setSaveMode] = useState<'topic' | 'entity' | null>(null)
  const [entityType, setEntityType] = useState('')
  const [messageTypeError, setMessageTypeError] = useState('')

  // Initialise from routerState on mount
  useEffect(() => {
    if (routerState?.key != null) setMsgKey(routerState.key ?? '')
    if (routerState?.value != null) setMsgValue(routerState.value)
    if (routerState?.headers != null) setMsgHeaders(routerState.headers)
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // Debounce expression changes
  useEffect(() => {
    const id = setTimeout(() => setDebouncedExpression(idExpression), 400)
    return () => clearTimeout(id)
  }, [idExpression, idSource, msgKey, msgValue, msgHeaders])

  // Live preview
  useEffect(() => {
    if (!debouncedExpression && idSource !== 'key') {
      setPreviewResult(null)
      return
    }
    matchersApi
      .preview({
        key: msgKey || null,
        value: msgValue || null,
        headers: msgHeaders,
        idSource,
        idExpression: debouncedExpression,
      })
      .then(setPreviewResult)
      .catch(e => setPreviewResult({ matched: false, entityId: null, error: (e as Error).message }))
  }, [debouncedExpression, idSource, msgKey, msgValue, msgHeaders])

  // Load fresh from broker
  async function loadFresh() {
    try {
      const msgs = await qc.fetchQuery({
        queryKey: ['brokers', brokerId, 'topics', topic, 'peek', 'one'],
        queryFn: () => brokersApi.peekMessages(brokerId, topic, 1),
        staleTime: 0,
      })
      if (msgs[0]) {
        setMsgKey(msgs[0].key ?? '')
        setMsgValue(msgs[0].value ?? '')
        setMsgHeaders(msgs[0].headers)
      }
    } catch (e) {
      addToast((e as Error).message, 'error')
    }
  }

  // Save as topic matcher
  const saveTopicMutation = useMutation({
    mutationFn: () => {
      if (!messageType) throw new Error('Message type is required')
      return topicsApi.addMatcher(topic, { messageType, idSource, idExpression })
    },
    onSuccess: () => addToast('Topic matcher saved', 'success'),
    onError: (e: Error) => {
      if (e.message.includes('404')) {
        addToast('Topic is not yet being recorded. Start recording first.', 'error')
      } else {
        addToast(e.message, 'error')
      }
    },
  })

  // Save as entity matcher
  const saveEntityMutation = useMutation({
    mutationFn: () =>
      entitiesApi.addSource(entityType, {
        topic,
        mode: 'entity_only',
        matchers: [{ messageType, idSource, idExpression }],
      }),
    onSuccess: () => addToast('Entity matcher saved', 'success'),
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  function handleSaveTopic() {
    if (!messageType) {
      setMessageTypeError('Message type is required')
      return
    }
    setMessageTypeError('')
    saveTopicMutation.mutate()
  }

  function handleSaveEntity() {
    saveEntityMutation.mutate()
  }

  // Header helpers
  function addHeader() {
    setMsgHeaders(h => [...h, { key: '', value: '' }])
  }

  function updateHeader(idx: number, field: 'key' | 'value', val: string) {
    setMsgHeaders(h => h.map((hdr, i) => i === idx ? { ...hdr, [field]: val } : hdr))
  }

  function removeHeader(idx: number) {
    setMsgHeaders(h => h.filter((_, i) => i !== idx))
  }

  function prettyPrint() {
    try {
      setMsgValue(JSON.stringify(JSON.parse(msgValue), null, 2))
      setValueError(false)
    } catch {
      // ignore if not valid JSON
    }
  }

  const idSourceHint: Record<string, string> = {
    key: 'The full message key will be used as entity ID',
    value: 'JSONPath expression, e.g. $.order_id',
    header: 'Header name, e.g. entity-id',
  }

  return (
    <Layout>
      <div style={{ marginBottom: '1.5rem' }}>
        <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700 }}>Matcher Playground</h1>
        <p style={{ margin: '0.4rem 0 0', fontSize: 14, color: '#718096' }}>
          Test JSONPath / key / header expressions against a real message, then save as a matcher.
        </p>
        <p style={{ margin: '0.25rem 0 0', fontSize: 13, color: '#a0aec0' }}>
          Topic: <strong>{topic}</strong> &nbsp;·&nbsp; Broker: <strong>{brokerId}</strong>
        </p>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1.5rem', alignItems: 'start' }}>
        {/* Left panel — Message Input */}
        <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1.25rem' }}>
          <h2 style={sectionTitle}>Message</h2>

          {/* Key */}
          <div style={{ marginBottom: '0.75rem' }}>
            <label style={labelStyle}>Key</label>
            <input
              style={inputStyle}
              value={msgKey}
              onChange={e => setMsgKey(e.target.value)}
              placeholder="message key"
            />
          </div>

          {/* Value */}
          <div style={{ marginBottom: '0.75rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
              <label style={{ ...labelStyle, marginBottom: 0 }}>Value</label>
              <button style={secondaryBtnSmall} onClick={prettyPrint} type="button">Pretty Print</button>
            </div>
            <textarea
              rows={6}
              style={{
                ...inputStyle,
                resize: 'vertical',
                fontFamily: 'monospace',
                fontSize: 13,
                border: valueError ? '1px solid #e53e3e' : '1px solid #cbd5e0',
              }}
              value={msgValue}
              onChange={e => {
                const v = e.target.value
                setMsgValue(v)
                if (v.length > 0) {
                  try { JSON.parse(v); setValueError(false) }
                  catch { setValueError(true) }
                } else {
                  setValueError(false)
                }
              }}
              placeholder='{"key": "value"}'
            />
            {valueError && (
              <p style={{ margin: '4px 0 0', fontSize: 12, color: '#e53e3e' }}>Invalid JSON</p>
            )}
          </div>

          {/* Binary banner */}
          {isBinary && (
            <div style={{ ...previewBox('#feebc8'), marginTop: 0, marginBottom: '0.75rem', fontWeight: 400, fontSize: 13 }}>
              This message value is binary (base64-encoded). JSONPath expressions will not match — use key or header as idSource instead.
            </div>
          )}

          {/* Headers */}
          <div style={{ marginBottom: '0.75rem' }}>
            <label style={labelStyle}>Headers</label>
            {msgHeaders.map((hdr, idx) => (
              <div key={idx} style={{ display: 'flex', gap: 6, marginBottom: 6 }}>
                <input
                  style={{ ...inputStyle, flex: 1 }}
                  value={hdr.key}
                  onChange={e => updateHeader(idx, 'key', e.target.value)}
                  placeholder="key"
                />
                <input
                  style={{ ...inputStyle, flex: 2 }}
                  value={hdr.value}
                  onChange={e => updateHeader(idx, 'value', e.target.value)}
                  placeholder="value"
                />
                <button style={dangerBtnSmall} onClick={() => removeHeader(idx)} type="button">×</button>
              </div>
            ))}
            <button
              style={{ ...cancelBtnStyle, fontSize: 13, padding: '0.3rem 0.7rem', marginTop: 4 }}
              onClick={addHeader}
              type="button"
            >
              + Add Header
            </button>
          </div>

          {/* Load fresh */}
          <button style={cancelBtnStyle} onClick={() => void loadFresh()} type="button">
            Load fresh from broker
          </button>
        </div>

        {/* Right panel — Matcher Builder + Save */}
        <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1.25rem' }}>
          <h2 style={sectionTitle}>Matcher</h2>

          {/* idSource */}
          <div style={{ marginBottom: '0.75rem' }}>
            <label style={labelStyle}>ID Source</label>
            <select
              style={inputStyle}
              value={idSource}
              onChange={e => setIdSource(e.target.value as 'key' | 'value' | 'header')}
            >
              <option value="key">key</option>
              <option value="value">value</option>
              <option value="header">header</option>
            </select>
            <p style={hintText}>{idSourceHint[idSource]}</p>
          </div>

          {/* idExpression (hidden for "key") */}
          {idSource !== 'key' && (
            <div style={{ marginBottom: '0.75rem' }}>
              <label style={labelStyle}>ID Expression</label>
              <input
                style={inputStyle}
                value={idExpression}
                onChange={e => setIdExpression(e.target.value)}
                placeholder={idSource === 'value' ? '$.order_id' : 'entity-id'}
              />
            </div>
          )}

          {/* messageType */}
          <div style={{ marginBottom: '0.75rem' }}>
            <label style={labelStyle}>Message type label <span style={{ fontWeight: 400, color: '#a0aec0' }}>(optional)</span></label>
            <input
              style={{ ...inputStyle, borderColor: messageTypeError ? '#e53e3e' : '#cbd5e0' }}
              value={messageType}
              onChange={e => { setMessageType(e.target.value); setMessageTypeError('') }}
              placeholder="e.g. OrderCreated"
            />
            {messageTypeError && (
              <p style={{ margin: '4px 0 0', fontSize: 12, color: '#e53e3e' }}>{messageTypeError}</p>
            )}
          </div>

          {/* Preview result */}
          {previewResult !== null && (
            <div>
              {previewResult.matched && previewResult.entityId && (
                <div style={previewBox('#c6f6d5')}>Matched: {previewResult.entityId}</div>
              )}
              {previewResult.matched && !previewResult.entityId && (
                <div style={previewBox('#fefcbf')}>Matched (empty ID)</div>
              )}
              {!previewResult.matched && !previewResult.error && (
                <div style={previewBox('#fed7d7')}>No match</div>
              )}
              {previewResult.error && (
                <div style={previewBox('#feebc8')}>Error: {previewResult.error}</div>
              )}
            </div>
          )}

          {/* Save section */}
          <div style={{ borderTop: '1px solid #e2e8f0', marginTop: '1.25rem', paddingTop: '1.25rem', display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
            {/* Save as Topic Matcher */}
            <button
              style={primaryBtnStyle}
              onClick={handleSaveTopic}
              disabled={saveTopicMutation.isPending}
              type="button"
            >
              {saveTopicMutation.isPending ? 'Saving…' : 'Save as Topic Matcher'}
            </button>

            {/* Save as Entity Matcher */}
            {saveMode !== 'entity' ? (
              <button
                style={cancelBtnStyle}
                onClick={() => setSaveMode('entity')}
                type="button"
              >
                Save as Entity Matcher
              </button>
            ) : (
              <div style={{ border: '1px solid #e2e8f0', borderRadius: 6, padding: '0.75rem' }}>
                <label style={labelStyle}>Entity type</label>
                <input
                  style={{ ...inputStyle, marginBottom: '0.6rem' }}
                  value={entityType}
                  onChange={e => setEntityType(e.target.value)}
                  placeholder="e.g. order"
                />
                <div style={{ display: 'flex', gap: 8 }}>
                  <button
                    style={primaryBtnStyle}
                    onClick={handleSaveEntity}
                    disabled={saveEntityMutation.isPending || !entityType}
                    type="button"
                  >
                    {saveEntityMutation.isPending ? 'Saving…' : 'Confirm'}
                  </button>
                  <button
                    style={cancelBtnStyle}
                    onClick={() => { setSaveMode(null); setEntityType('') }}
                    type="button"
                  >
                    Cancel
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </Layout>
  )
}
