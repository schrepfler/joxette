import { useState, useCallback } from 'react'
import { STEP_DEF_MAP } from '#/transforms/definitions'
import {
  emptyPipeline,
  serializeSteps,
  type TransformPipeline,
  type PipelineStep,
  type TransformStep,
  type FragmentDefinition,
} from '#/transforms/types'
import { StepCard } from './StepCard'
import { StepPicker } from './StepPicker'
import { PresetManager } from './PresetManager'
import { PipelineDryRun } from './PipelineDryRun'
import { FragmentManager } from './FragmentManager'

interface Props {
  pipeline: TransformPipeline
  onChange: (pipeline: TransformPipeline) => void
  mode: 'topic' | 'entity'
  topic?: string
  entityType?: string
  entityId?: string
  disabled?: boolean
}

export function TransformPipelineBuilder({
  pipeline,
  onChange,
  mode,
  topic,
  entityType,
  entityId,
  disabled,
}: Props) {
  const [open, setOpen] = useState(false)
  const [showPicker, setShowPicker] = useState(false)
  const [showPresets, setShowPresets] = useState(false)
  const [showDryRun, setShowDryRun] = useState(false)
  const [expandedIndex, setExpandedIndex] = useState<number | null>(null)

  const steps = pipeline.steps
  const hasSteps = steps.length > 0

  function addStep(type: TransformStep['type']) {
    const def = STEP_DEF_MAP.get(type)
    if (!def) return
    const newStep: PipelineStep = {
      type,
      ...def.defaults(),
      _id: crypto.randomUUID(),
    } as PipelineStep
    const newSteps = [...steps, newStep]
    onChange({ steps: newSteps })
    setExpandedIndex(newSteps.length - 1)
    setShowPicker(false)
  }

  function updateStep(index: number, updated: PipelineStep) {
    const newSteps = steps.map((s, i) => (i === index ? updated : s))
    onChange({ steps: newSteps })
  }

  function deleteStep(index: number) {
    const newSteps = steps.filter((_, i) => i !== index)
    onChange({ steps: newSteps })
    if (expandedIndex === index) setExpandedIndex(null)
    else if (expandedIndex !== null && expandedIndex > index) setExpandedIndex(expandedIndex - 1)
  }

  const moveStep = useCallback(
    (fromIndex: number, toIndex: number) => {
      if (fromIndex === toIndex) return
      const arr = [...steps]
      const [removed] = arr.splice(fromIndex, 1)
      arr.splice(toIndex, 0, removed)
      onChange({ steps: arr })
      if (expandedIndex === fromIndex) setExpandedIndex(toIndex)
      else if (expandedIndex !== null) {
        if (fromIndex < expandedIndex && toIndex >= expandedIndex) setExpandedIndex(expandedIndex - 1)
        else if (fromIndex > expandedIndex && toIndex <= expandedIndex) setExpandedIndex(expandedIndex + 1)
      }
    },
    [steps, expandedIndex, onChange],
  )

  function clearPipeline() {
    onChange(emptyPipeline())
    setExpandedIndex(null)
  }

  return (
    <div style={containerStyle}>
      {/* Header */}
      <div
        style={headerStyle}
        onClick={() => setOpen(o => !o)}
        role="button"
        aria-expanded={open}
        tabIndex={0}
        onKeyDown={e => e.key === 'Enter' && setOpen(o => !o)}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: 13, fontWeight: 700, color: '#2d3748' }}>Transform Pipeline</span>
          {hasSteps && (
            <span style={stepsBadgeStyle}>
              {steps.length} step{steps.length !== 1 ? 's' : ''}
            </span>
          )}
          {!hasSteps && !open && (
            <span style={{ fontSize: 12, color: '#a0aec0' }}>— no transforms applied</span>
          )}
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          {hasSteps && (
            <button
              style={clearBtnStyle}
              onClick={e => { e.stopPropagation(); clearPipeline() }}
              disabled={disabled}
              title="Clear pipeline"
            >
              Clear
            </button>
          )}
          <span style={{ color: '#a0aec0', fontSize: 14 }}>{open ? '▲' : '▼'}</span>
        </div>
      </div>

      {/* Body */}
      {open && (
        <div style={{ padding: '0.75rem' }}>
          {/* Fragment / phase manager */}
          <FragmentManager
            fragments={pipeline.fragments ?? []}
            onChange={(frags: FragmentDefinition[]) => onChange({ ...pipeline, fragments: frags })}
          />

          {/* Step list */}
          {steps.map((step, i) => (
            <StepCard
              key={step._id}
              step={step}
              index={i}
              total={steps.length}
              expanded={expandedIndex === i}
              onToggleExpand={() => setExpandedIndex(expandedIndex === i ? null : i)}
              onChange={updated => updateStep(i, updated)}
              onDelete={() => deleteStep(i)}
              onMove={moveStep}
              disabled={disabled}
              fragments={pipeline.fragments}
            />
          ))}

          {steps.length === 0 && (
            <p style={{ fontSize: 13, color: '#a0aec0', textAlign: 'center', margin: '0.5rem 0 0.75rem' }}>
              No steps yet. Add a step below.
            </p>
          )}

          {/* Toolbar */}
          <div style={{ display: 'flex', gap: 6, marginTop: '0.5rem', flexWrap: 'wrap' }}>
            <button
              style={addBtnStyle}
              onClick={() => setShowPicker(true)}
              disabled={disabled}
            >
              + Add Step
            </button>
            <button
              style={secondaryBtnStyle}
              onClick={() => setShowPresets(true)}
              disabled={disabled}
            >
              Presets
            </button>
            <button
              style={{
                ...secondaryBtnStyle,
                opacity: steps.length === 0 ? 0.5 : 1,
                cursor: steps.length === 0 ? 'not-allowed' : 'pointer',
              }}
              onClick={() => setShowDryRun(true)}
              disabled={disabled || steps.length === 0}
            >
              Dry Run
            </button>
          </div>
        </div>
      )}

      {/* Modals */}
      {showPicker && (
        <StepPicker onSelect={addStep} onClose={() => setShowPicker(false)} />
      )}
      {showPresets && (
        <PresetManager
          steps={steps}
          onLoad={loadedSteps => onChange({ steps: loadedSteps })}
          onClose={() => setShowPresets(false)}
        />
      )}
      {showDryRun && (
        <PipelineDryRun
          pipeline={pipeline}
          mode={mode}
          topic={topic}
          entityType={entityType}
          entityId={entityId}
          onClose={() => setShowDryRun(false)}
          onAddStep={newStep => {
            const updated = [...steps, newStep]
            onChange({ ...pipeline, steps: updated })
            setExpandedIndex(updated.length - 1)
          }}
        />
      )}
    </div>
  )
}

// Export helper to get the serialized steps for API calls
export { serializeSteps }
export { emptyPipeline }

const containerStyle: React.CSSProperties = {
  border: '1px solid #e2e8f0', borderRadius: 8, marginBottom: '1rem', overflow: 'hidden',
}
const headerStyle: React.CSSProperties = {
  display: 'flex', alignItems: 'center', justifyContent: 'space-between',
  padding: '0.6rem 0.85rem', background: '#f7fafc', cursor: 'pointer',
  userSelect: 'none', borderBottom: '1px solid #e2e8f0',
}
const stepsBadgeStyle: React.CSSProperties = {
  background: '#3182ce', color: '#fff', fontSize: 11, fontWeight: 700,
  padding: '1px 7px', borderRadius: 10,
}
const clearBtnStyle: React.CSSProperties = {
  padding: '0.2rem 0.5rem', fontSize: 11, background: '#fff', color: '#e53e3e',
  border: '1px solid #e53e3e', borderRadius: 4, cursor: 'pointer',
}
const addBtnStyle: React.CSSProperties = {
  padding: '0.35rem 0.85rem', background: '#3182ce', color: '#fff', border: 'none',
  borderRadius: 4, cursor: 'pointer', fontSize: 13,
}
const secondaryBtnStyle: React.CSSProperties = {
  padding: '0.35rem 0.85rem', background: '#fff', color: '#4a5568',
  border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 13,
}
