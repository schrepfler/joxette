import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { transformsApi } from '#/api/client'
import type { PipelineStep, TransformStep } from '#/transforms/types'
import { serializeSteps } from '#/transforms/types'

interface Props {
  steps: PipelineStep[]
  onLoad: (steps: PipelineStep[]) => void
  onClose: () => void
}

export function PresetManager({ steps, onLoad, onClose }: Props) {
  const [tab, setTab] = useState<'load' | 'save'>('load')
  const [saveForm, setSaveForm] = useState({ name: '', description: '' })
  const [saveError, setSaveError] = useState<string | null>(null)
  const qc = useQueryClient()

  const { data: presets, isLoading } = useQuery({
    queryKey: ['transform-presets'],
    queryFn: () => transformsApi.list(),
  })

  const createMutation = useMutation({
    mutationFn: () => transformsApi.create({
      name: saveForm.name.trim(),
      description: saveForm.description.trim() || undefined,
      steps: serializeSteps(steps),
    }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['transform-presets'] })
      onClose()
    },
    onError: (e: Error) => setSaveError(e.message),
  })

  const deleteMutation = useMutation({
    mutationFn: (name: string) => transformsApi.delete(name),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['transform-presets'] }),
  })

  function loadPreset(presetSteps: TransformStep[]) {
    const withIds: PipelineStep[] = presetSteps.map(s => ({
      ...s,
      _id: crypto.randomUUID(),
    } as PipelineStep))
    onLoad(withIds)
    onClose()
  }

  return (
    <div style={overlayStyle} onClick={e => { if (e.target === e.currentTarget) onClose() }}>
      <div style={modalStyle}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.75rem' }}>
          <h2 style={{ margin: 0, fontSize: 16, fontWeight: 700 }}>Transform Presets</h2>
          <button style={closeBtnStyle} onClick={onClose}>✕</button>
        </div>

        {/* Tabs */}
        <div style={{ display: 'flex', borderBottom: '1px solid #e2e8f0', marginBottom: '1rem' }}>
          {(['load', 'save'] as const).map(t => (
            <button
              key={t}
              style={{
                ...tabStyle,
                borderBottom: tab === t ? '2px solid #3182ce' : '2px solid transparent',
                color: tab === t ? '#3182ce' : '#4a5568',
                fontWeight: tab === t ? 700 : 400,
              }}
              onClick={() => setTab(t)}
            >
              {t === 'load' ? 'Load Preset' : `Save Current Pipeline`}
            </button>
          ))}
        </div>

        {tab === 'load' && (
          <div>
            {isLoading && <p style={mutedStyle}>Loading presets…</p>}
            {!isLoading && (!presets || presets.length === 0) && (
              <p style={mutedStyle}>No saved presets yet. Create one from the "Save" tab.</p>
            )}
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              {presets?.map(p => (
                <div key={p.name} style={presetRowStyle}>
                  <div style={{ flex: 1 }}>
                    <span style={{ fontWeight: 600, fontSize: 13 }}>{p.name}</span>
                    {p.description && (
                      <span style={{ fontSize: 12, color: '#718096', marginLeft: 8 }}>{p.description}</span>
                    )}
                    <span style={{ fontSize: 11, color: '#a0aec0', marginLeft: 8 }}>
                      {p.steps.length} step{p.steps.length !== 1 ? 's' : ''}
                    </span>
                  </div>
                  <button
                    style={loadBtnStyle}
                    onClick={() => loadPreset(p.steps)}
                  >
                    Load
                  </button>
                  <button
                    style={deleteBtnStyle}
                    onClick={() => {
                      if (window.confirm(`Delete preset "${p.name}"?`)) {
                        deleteMutation.mutate(p.name)
                      }
                    }}
                    disabled={deleteMutation.isPending}
                  >
                    Delete
                  </button>
                </div>
              ))}
            </div>
          </div>
        )}

        {tab === 'save' && (
          <div>
            <div style={{ marginBottom: '0.75rem' }}>
              <label style={labelStyle}>Preset Name *</label>
              <input
                autoFocus
                type="text"
                placeholder="e.g. staging-sanitize"
                value={saveForm.name}
                onChange={e => setSaveForm(f => ({ ...f, name: e.target.value }))}
                style={inputStyle}
              />
            </div>
            <div style={{ marginBottom: '0.75rem' }}>
              <label style={labelStyle}>Description</label>
              <input
                type="text"
                placeholder="Optional"
                value={saveForm.description}
                onChange={e => setSaveForm(f => ({ ...f, description: e.target.value }))}
                style={inputStyle}
              />
            </div>
            <p style={{ fontSize: 12, color: '#718096', margin: '0 0 0.75rem' }}>
              Saving {steps.length} step{steps.length !== 1 ? 's' : ''} from the current pipeline.
            </p>
            {saveError && <p style={{ fontSize: 12, color: '#e53e3e', margin: '0 0 0.5rem' }}>{saveError}</p>}
            <button
              style={{
                ...saveBtnStyle,
                opacity: !saveForm.name.trim() || createMutation.isPending ? 0.6 : 1,
                cursor: !saveForm.name.trim() || createMutation.isPending ? 'not-allowed' : 'pointer',
              }}
              onClick={() => createMutation.mutate()}
              disabled={!saveForm.name.trim() || createMutation.isPending}
            >
              {createMutation.isPending ? 'Saving…' : 'Save Preset'}
            </button>
          </div>
        )}
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
  width: 520, maxWidth: '95vw', maxHeight: '75vh', overflow: 'auto',
  boxShadow: '0 8px 32px rgba(0,0,0,0.18)',
}
const closeBtnStyle: React.CSSProperties = {
  background: 'none', border: 'none', fontSize: 18, cursor: 'pointer', color: '#718096', padding: '0 4px',
}
const tabStyle: React.CSSProperties = {
  background: 'none', border: 'none', cursor: 'pointer', padding: '0.4rem 0.75rem',
  fontSize: 14, marginBottom: -1,
}
const presetRowStyle: React.CSSProperties = {
  display: 'flex', alignItems: 'center', gap: 8, padding: '0.5rem 0.75rem',
  border: '1px solid #e2e8f0', borderRadius: 6, background: '#f7fafc',
}
const loadBtnStyle: React.CSSProperties = {
  padding: '0.25rem 0.6rem', fontSize: 12, background: '#3182ce', color: '#fff',
  border: 'none', borderRadius: 4, cursor: 'pointer',
}
const deleteBtnStyle: React.CSSProperties = {
  padding: '0.25rem 0.6rem', fontSize: 12, background: '#fff', color: '#e53e3e',
  border: '1px solid #e53e3e', borderRadius: 4, cursor: 'pointer',
}
const mutedStyle: React.CSSProperties = { fontSize: 13, color: '#718096' }
const labelStyle: React.CSSProperties = { display: 'block', marginBottom: 4, fontSize: 12, fontWeight: 600, color: '#4a5568' }
const inputStyle: React.CSSProperties = {
  padding: '0.4rem 0.6rem', border: '1px solid #cbd5e0', borderRadius: 4,
  fontSize: 14, width: '100%', boxSizing: 'border-box',
}
const saveBtnStyle: React.CSSProperties = {
  padding: '0.45rem 1.2rem', background: '#3182ce', color: '#fff', border: 'none',
  borderRadius: 4, fontSize: 14, cursor: 'pointer',
}
