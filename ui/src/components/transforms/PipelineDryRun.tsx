import { useState } from 'react'
import { cassettesApi } from '#/api/client'
import type { TransformPipeline, GapTransformStep, PipelineStep } from '#/transforms/types'
import { serializeSteps } from '#/transforms/types'
import type { CassetteRecord, EntityRecord } from '#/api/client'
import { GapTimeline, resolveFragmentsClientSide } from './GapTimeline'

interface Props {
  pipeline: TransformPipeline
  mode: 'topic' | 'entity'
  topic?: string
  entityType?: string
  entityId?: string
  onClose: () => void
  /** Called when user adds a GapTransformStep from the timeline panel */
  onAddStep?: (step: PipelineStep) => void
}

type PreviewResult = {
  original: CassetteRecord | EntityRecord
  transformed: CassetteRecord | EntityRecord
}[]

export function PipelineDryRun({ pipeline, mode, topic, entityType, entityId, onClose, onAddStep }: Props) {
  const [limit, setLimit] = useState(5)
  const [results, setResults] = useState<PreviewResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function runPreview() {
    setLoading(true)
    setError(null)
    setResults(null)
    try {
      const steps = serializeSteps(pipeline.steps)
      const res = await cassettesApi.previewTransforms({
        mode,
        topic,
        entityType,
        entityId,
        steps,
        limit,
      })
      setResults(res)
    } catch (e: unknown) {
      setError((e as Error).message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={overlayStyle} onClick={e => { if (e.target === e.currentTarget) onClose() }}>
      <div style={modalStyle}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.75rem' }}>
          <h2 style={{ margin: 0, fontSize: 16, fontWeight: 700 }}>Pipeline Dry Run</h2>
          <button style={closeBtnStyle} onClick={onClose}>✕</button>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: '1rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <label style={{ fontSize: 13, fontWeight: 600, color: '#4a5568' }}>Preview first</label>
            <input
              type="number"
              min={1}
              max={20}
              value={limit}
              onChange={e => setLimit(Math.max(1, Math.min(20, Number(e.target.value))))}
              style={{ width: 56, padding: '0.3rem 0.4rem', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 13 }}
            />
            <span style={{ fontSize: 13, color: '#4a5568' }}>messages</span>
          </div>
          <button
            style={{
              padding: '0.4rem 1rem', background: '#3182ce', color: '#fff', border: 'none',
              borderRadius: 4, fontSize: 13, cursor: loading ? 'not-allowed' : 'pointer',
              opacity: loading ? 0.7 : 1,
            }}
            onClick={runPreview}
            disabled={loading || pipeline.steps.length === 0}
          >
            {loading ? 'Running…' : 'Run Preview'}
          </button>
          {pipeline.steps.length === 0 && (
            <span style={{ fontSize: 12, color: '#e53e3e' }}>Add at least one step first</span>
          )}
        </div>

        {error && (
          <div style={{ padding: '0.5rem 0.75rem', background: '#fff5f5', border: '1px solid #fed7d7', borderRadius: 4, marginBottom: '0.75rem' }}>
            <p style={{ margin: 0, fontSize: 13, color: '#c53030' }}>{error}</p>
          </div>
        )}

        {results && results.length === 0 && (
          <p style={{ fontSize: 13, color: '#718096' }}>No records matched the query.</p>
        )}

        {results && results.length > 0 && (() => {
          const previewMessages = results.map(r => r.original as CassetteRecord)
          const fragments = pipeline.fragments ?? []
          const resolved = resolveFragmentsClientSide(previewMessages, fragments)
          const existingGapSteps = pipeline.steps
            .filter(s => s.type === 'gap_transform') as GapTransformStep[]

          return (
            <div style={{ overflowY: 'auto', flex: 1 }}>
              <p style={{ fontSize: 12, color: '#718096', margin: '0 0 0.75rem' }}>
                Showing {results.length} message{results.length !== 1 ? 's' : ''} — before and after transforms applied.
              </p>

              {/* Timeline — only show when onAddStep is wired and there are gaps */}
              {onAddStep && previewMessages.length > 1 && (
                <div style={{ border: '1px solid #e2e8f0', borderRadius: 6, padding: '0.65rem', marginBottom: '1rem', background: '#f7fafc' }}>
                  <div style={{ fontSize: 12, fontWeight: 700, color: '#4a5568', marginBottom: '0.5rem' }}>
                    Timeline — click a gap to add a transform step
                  </div>
                  <GapTimeline
                    messages={previewMessages}
                    fragments={fragments}
                    resolvedFragments={resolved}
                    onAddStep={step => {
                      onAddStep({ ...step, _id: crypto.randomUUID() } as PipelineStep)
                    }}
                    existingGapSteps={existingGapSteps}
                  />
                </div>
              )}

              <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                {results.map((r, i) => (
                  <MessageDiff key={i} index={i} original={r.original} transformed={r.transformed} />
                ))}
              </div>
            </div>
          )
        })()}
      </div>
    </div>
  )
}

function MessageDiff({
  index,
  original,
  transformed,
}: {
  index: number
  original: CassetteRecord | EntityRecord
  transformed: CassetteRecord | EntityRecord
}) {
  return (
    <div style={{ border: '1px solid #e2e8f0', borderRadius: 6, overflow: 'hidden' }}>
      <div style={{ background: '#f7fafc', padding: '0.35rem 0.75rem', fontSize: 12, fontWeight: 600, color: '#4a5568' }}>
        Message #{index + 1}
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr' }}>
        <div style={{ borderRight: '1px solid #e2e8f0' }}>
          <div style={{ padding: '0.35rem 0.75rem', background: '#fffbeb', fontSize: 11, fontWeight: 700, color: '#744210' }}>
            BEFORE
          </div>
          <pre style={preStyle}>{JSON.stringify(original, null, 2)}</pre>
        </div>
        <div>
          <div style={{ padding: '0.35rem 0.75rem', background: '#f0fff4', fontSize: 11, fontWeight: 700, color: '#22543d' }}>
            AFTER
          </div>
          <pre style={preStyle}>{JSON.stringify(transformed, null, 2)}</pre>
        </div>
      </div>
    </div>
  )
}

const overlayStyle: React.CSSProperties = {
  position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', zIndex: 1000,
  display: 'flex', alignItems: 'center', justifyContent: 'center',
}
const modalStyle: React.CSSProperties = {
  background: '#fff', borderRadius: 8, padding: '1.25rem',
  width: 900, maxWidth: '97vw', maxHeight: '87vh',
  display: 'flex', flexDirection: 'column',
  boxShadow: '0 8px 32px rgba(0,0,0,0.18)',
}
const closeBtnStyle: React.CSSProperties = {
  background: 'none', border: 'none', fontSize: 18, cursor: 'pointer', color: '#718096', padding: '0 4px',
}
const preStyle: React.CSSProperties = {
  margin: 0, padding: '0.5rem 0.75rem', fontSize: 11,
  fontFamily: 'monospace', overflowX: 'auto',
  maxHeight: 300, overflowY: 'auto',
  background: 'transparent',
  whiteSpace: 'pre-wrap', wordBreak: 'break-all',
}
