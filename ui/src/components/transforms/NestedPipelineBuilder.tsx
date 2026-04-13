import { useState, useCallback } from 'react'
import { STEP_DEF_MAP } from '#/transforms/definitions'
import type { PipelineStep, TransformStep } from '#/transforms/types'
import { StepPicker } from './StepPicker'
// Note: StepConfigForm is imported below — this creates a circular dep with
// StepConfigForm → NestedPipelineBuilder → StepConfigForm, which is safe in
// ESM/React because the reference is only used inside component function bodies
// (never at module initialisation time).
import { StepConfigForm } from './StepConfigForm'

interface Props {
  steps: TransformStep[]
  onChange: (steps: TransformStep[]) => void
  label?: string
  disabled?: boolean
}

const CATEGORY_COLORS: Record<string, string> = {
  Time: '#38a169',
  'Field Values': '#3182ce',
  Structure: '#805ad5',
  Keys: '#d69e2e',
  Headers: '#e53e3e',
  Routing: '#dd6b20',
  Logic: '#718096',
}

export function NestedPipelineBuilder({ steps: stepsProp, onChange, label = 'Steps', disabled }: Props) {
  const [pipelineSteps, setPipelineSteps] = useState<PipelineStep[]>(() =>
    stepsProp.map(s => ({ ...s, _id: crypto.randomUUID() }) as PipelineStep),
  )
  const [showPicker, setShowPicker] = useState(false)
  const [expandedIndex, setExpandedIndex] = useState<number | null>(null)

  function update(newSteps: PipelineStep[]) {
    setPipelineSteps(newSteps)
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    onChange(newSteps.map(({ _id: _, ...rest }) => rest as TransformStep))
  }

  function addStep(type: TransformStep['type']) {
    const def = STEP_DEF_MAP.get(type)
    if (!def) return
    const newStep = { type, ...def.defaults(), _id: crypto.randomUUID() } as PipelineStep
    const newSteps = [...pipelineSteps, newStep]
    update(newSteps)
    setExpandedIndex(newSteps.length - 1)
    setShowPicker(false)
  }

  function deleteStep(index: number) {
    const newSteps = pipelineSteps.filter((_, i) => i !== index)
    update(newSteps)
    if (expandedIndex === index) setExpandedIndex(null)
    else if (expandedIndex !== null && expandedIndex > index) setExpandedIndex(expandedIndex - 1)
  }

  function updateStep(index: number, updated: PipelineStep) {
    const newSteps = pipelineSteps.map((s, i) => (i === index ? updated : s))
    update(newSteps)
  }

  const moveStep = useCallback(
    (fromIndex: number, toIndex: number) => {
      if (fromIndex === toIndex) return
      const arr = [...pipelineSteps]
      const [removed] = arr.splice(fromIndex, 1)
      arr.splice(toIndex, 0, removed)
      update(arr)
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [pipelineSteps],
  )

  return (
    <div style={containerStyle}>
      <div style={headerStyle}>{label}</div>
      <div style={{ padding: '0.4rem 0.6rem' }}>
        {pipelineSteps.length === 0 && (
          <p style={emptyStyle}>No steps. Click "+ Add Step" below.</p>
        )}
        {pipelineSteps.map((step, i) => (
          <NestedStepItem
            key={step._id}
            step={step}
            index={i}
            total={pipelineSteps.length}
            expanded={expandedIndex === i}
            onToggleExpand={() => setExpandedIndex(expandedIndex === i ? null : i)}
            onChange={updated => updateStep(i, updated)}
            onDelete={() => deleteStep(i)}
            onMoveUp={i > 0 ? () => moveStep(i, i - 1) : undefined}
            onMoveDown={i < pipelineSteps.length - 1 ? () => moveStep(i, i + 1) : undefined}
            disabled={disabled}
          />
        ))}
        <button
          style={addBtnStyle}
          onClick={() => setShowPicker(true)}
          disabled={disabled}
        >
          + Add Step
        </button>
      </div>
      {showPicker && <StepPicker onSelect={addStep} onClose={() => setShowPicker(false)} />}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Compact step item for nested use
// ---------------------------------------------------------------------------

interface NestedStepItemProps {
  step: PipelineStep
  index: number
  total: number
  expanded: boolean
  onToggleExpand: () => void
  onChange: (updated: PipelineStep) => void
  onDelete: () => void
  onMoveUp?: () => void
  onMoveDown?: () => void
  disabled?: boolean
}

function NestedStepItem({
  step,
  expanded,
  onToggleExpand,
  onChange,
  onDelete,
  onMoveUp,
  onMoveDown,
  disabled,
}: NestedStepItemProps) {
  const def = STEP_DEF_MAP.get(step.type)
  const label = def?.label ?? step.type
  const category = def?.category ?? ''
  const badgeColor = CATEGORY_COLORS[category] ?? '#e2e8f0'

  return (
    <div style={{ ...itemStyle, background: expanded ? '#f0f7ff' : '#fff', borderColor: expanded ? '#3182ce' : '#e2e8f0' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
        {/* Up/down reorder */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
          <button
            style={arrowBtnStyle}
            onClick={onMoveUp}
            disabled={disabled || !onMoveUp}
            title="Move up"
          >
            ▲
          </button>
          <button
            style={arrowBtnStyle}
            onClick={onMoveDown}
            disabled={disabled || !onMoveDown}
            title="Move down"
          >
            ▼
          </button>
        </div>

        {/* Category badge */}
        <span style={{ ...badgeStyle, background: badgeColor, color: '#fff' }}>{category}</span>

        {/* Label button */}
        <button style={labelBtnStyle} onClick={onToggleExpand}>
          <span style={{ fontWeight: 600, fontSize: 12 }}>{label}</span>
        </button>

        {/* Delete */}
        <button
          style={deleteBtnStyle}
          onClick={onDelete}
          title="Remove step"
          disabled={disabled}
        >
          ✕
        </button>
      </div>

      {expanded && (
        <div style={{ borderTop: '1px solid #e2e8f0', marginTop: '0.4rem' }}>
          <StepConfigForm step={step} onChange={onChange} />
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const containerStyle: React.CSSProperties = {
  border: '1px solid #e2e8f0',
  borderRadius: 6,
  marginBottom: '0.5rem',
  overflow: 'hidden',
}
const headerStyle: React.CSSProperties = {
  padding: '0.3rem 0.6rem',
  background: '#f7fafc',
  borderBottom: '1px solid #e2e8f0',
  fontSize: 11,
  fontWeight: 700,
  color: '#4a5568',
  textTransform: 'uppercase',
  letterSpacing: '0.04em',
}
const emptyStyle: React.CSSProperties = {
  fontSize: 12,
  color: '#a0aec0',
  margin: '0.25rem 0 0.4rem',
}
const addBtnStyle: React.CSSProperties = {
  marginTop: '0.4rem',
  padding: '0.25rem 0.7rem',
  background: '#3182ce',
  color: '#fff',
  border: 'none',
  borderRadius: 4,
  cursor: 'pointer',
  fontSize: 12,
}
const itemStyle: React.CSSProperties = {
  border: '1px solid #e2e8f0',
  borderRadius: 5,
  padding: '0.35rem 0.5rem',
  marginBottom: 3,
}
const arrowBtnStyle: React.CSSProperties = {
  background: 'none',
  border: 'none',
  cursor: 'pointer',
  fontSize: 8,
  color: '#a0aec0',
  padding: '0 1px',
  lineHeight: 1,
}
const badgeStyle: React.CSSProperties = {
  fontSize: 9,
  fontWeight: 700,
  padding: '1px 5px',
  borderRadius: 8,
  flexShrink: 0,
  textTransform: 'uppercase',
  letterSpacing: '0.04em',
}
const labelBtnStyle: React.CSSProperties = {
  background: 'none',
  border: 'none',
  cursor: 'pointer',
  textAlign: 'left',
  flex: 1,
  padding: 0,
}
const deleteBtnStyle: React.CSSProperties = {
  background: 'none',
  border: 'none',
  cursor: 'pointer',
  fontSize: 11,
  color: '#a0aec0',
  padding: '1px 3px',
  flexShrink: 0,
  lineHeight: 1,
}

