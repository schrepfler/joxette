import { useCallback, useEffect, useRef, useState } from 'react'
import { cassettesApi } from '#/api/client'
import type { MatchStep, FragmentDefinition, SequenceConstraints } from '#/transforms/types'
import { isLeafPredicate } from '#/transforms/types'
import {
  useSequenceStore,
  addStep,
  removeStep,
  updateStep,
  reorderSteps,
  setConstraints,
  setResults,
  setLoading,
  setError,
  stepColors,
} from '#/stores/sequenceStore'
import { MatchStepRow } from './sequence/MatchStepRow'
import { CleanDataSection } from './sequence/CleanDataSection'
import type { CleanDataState } from './sequence/CleanDataSection'
import { SequenceExamplesPane } from './sequence/SequenceExamplesPane'
import { SequenceModelPanel } from './sequence/SequenceModelPanel'

interface Props {
  mode: 'topic' | 'entity'
  topic?: string
  entityType?: string
  entityId?: string
  onSaveFragment?: (fragment: FragmentDefinition) => void
}

let _idCounter = 0
function newStepId() { return `seq-step-${++_idCounter}` }

function defaultStep(): MatchStep {
  return {
    _id: newStepId(),
    predicate: { field: '$.value.type', operator: 'EQ', value: '' },
    label: undefined,
    required: true,
    repeated: false,
    gap: 'any',
  }
}

function buildSOL(steps: MatchStep[], constraints?: SequenceConstraints): string {
  if (steps.length === 0) return '# No steps defined'

  const stepLines = steps.map((s, i) => {
    const name = s.label || inferStepLabel(s, i)
    const safeName = name.replace(/\s+/g, '_').toLowerCase()
    let pred = ''
    if (isLeafPredicate(s.predicate) && s.predicate.field) {
      pred = `${s.predicate.field} ${s.predicate.operator === 'EQ' ? '=' : s.predicate.operator} "${s.predicate.value ?? ''}"`
    }
    const quantifier = s.repeated ? '+' : s.required ? '' : '?'
    const gapPrefix = i > 0 && steps[i - 1].gap === 'any' ? '      *\n      ' : '      '
    return `${gapPrefix}${safeName}${quantifier}${pred ? `: ${pred}` : ''}`
  })

  const lines = ['match ' + stepLines.join('\n')]

  if (constraints?.maxDurationMs) {
    const firstName = inferStepLabel(steps[0], 0).replace(/\s+/g, '_').toLowerCase()
    const lastName = inferStepLabel(steps[steps.length - 1], steps.length - 1).replace(/\s+/g, '_').toLowerCase()
    const secs = constraints.maxDurationMs / 1000
    lines.push(`if ${lastName}.timestamp - ${firstName}.timestamp < ${secs}s`)
  }

  if (constraints?.minDurationMs) {
    const firstName = inferStepLabel(steps[0], 0).replace(/\s+/g, '_').toLowerCase()
    const lastName = inferStepLabel(steps[steps.length - 1], steps.length - 1).replace(/\s+/g, '_').toLowerCase()
    const secs = constraints.minDurationMs / 1000
    lines.push(`   AND ${lastName}.timestamp - ${firstName}.timestamp > ${secs}s`)
  }

  return lines.join('\n')
}

function inferStepLabel(step: MatchStep, fallbackIndex: number): string {
  if (step.label) return step.label
  if (isLeafPredicate(step.predicate) && step.predicate.field === '$.value.type' && step.predicate.value) {
    return String(step.predicate.value)
  }
  return `step${fallbackIndex + 1}`
}

export function SequenceQueryPanel({ mode, topic, entityType, onSaveFragment }: Props) {
  const { query, loading, error } = useSequenceStore()
  const { steps, constraints } = query
  const colors = stepColors(steps)

  const [cleanData, setCleanData] = useState<CleanDataState>({})
  const [optionsOpen, setOptionsOpen] = useState(false)
  const [solOpen, setSolOpen] = useState(false)
  const [fragmentDialog, setFragmentDialog] = useState<{
    name: string; label: string; color: string
  } | null>(null)

  // Debounced query execution
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const runQuery = useCallback(() => {
    if (steps.length === 0) { setResults(null); return }
    setLoading(true)
    setError(null)
    cassettesApi.matchSequences(mode, { topic, entityType }, query)
      .then(r => { setResults(r); setLoading(false) })
      .catch(e => { setError(String(e)); setLoading(false) })
  }, [mode, topic, entityType, query])

  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(runQuery, 400)
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current) }
  }, [runQuery])

  function handleMove(fromIndex: number, toIndex: number) {
    reorderSteps(fromIndex, toIndex)
  }

  function handleOpenSaveDialog() {
    if (!onSaveFragment || steps.length < 1) return
    const first = steps[0]
    const last = steps[steps.length - 1]
    setFragmentDialog({
      name: `sequence_${Date.now()}`,
      label: `${inferStepLabel(first, 0)} → ${inferStepLabel(last, steps.length - 1)}`,
      color: colors[first._id] ?? '#4f6ef7',
    })
  }

  function handleConfirmFragment() {
    if (!onSaveFragment || !fragmentDialog || steps.length < 1) return
    const first = steps[0]
    const last = steps[steps.length - 1]
    const fragment: FragmentDefinition = {
      name: fragmentDialog.name,
      label: fragmentDialog.label,
      color: fragmentDialog.color,
      from: { predicate: first.predicate, quantifier: 'first' },
      to: { predicate: last.predicate, quantifier: 'last' },
      ...(constraints?.maxDurationMs ? { if: { max_duration_ms: constraints.maxDurationMs } } : {}),
    }
    onSaveFragment(fragment)
    setFragmentDialog(null)
  }

  const sol = buildSOL(steps, constraints)

  return (
    <div style={outerPanelStyle}>
      {/* Panel header — spans full width */}
      <div style={panelHeaderStyle}>
        <span style={headerLabelStyle}>▸ Sequence Query</span>
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 6 }}>
          {loading && <span style={loadingBadgeStyle}>…</span>}
          <button style={solBtnStyle} onClick={() => setSolOpen(v => !v)} title="View SOL query">
            View SOL
          </button>
        </div>
      </div>

      {/* SOL popover — spans full width */}
      {solOpen && (
        <div style={solPopoverStyle}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
            <span style={{ fontSize: 11, fontWeight: 700, color: '#718096', textTransform: 'uppercase', letterSpacing: '0.04em' }}>SOL Query</span>
            <button style={solCloseBtnStyle} onClick={() => setSolOpen(false)}>✕</button>
          </div>
          <pre style={solPreStyle}>{sol}</pre>
        </div>
      )}

      {/* Error banner — spans full width */}
      {error && (
        <div style={errorBannerStyle}>
          <span style={{ fontSize: 12, color: '#c53030' }}>{error}</span>
          <button style={errorDismissStyle} onClick={() => setError(null)}>✕</button>
        </div>
      )}

      {/* Two-column body */}
      <div style={columnsStyle}>
        {/* LEFT column — fixed w-80 */}
        <div style={leftColumnStyle}>
          {/* Clean data section */}
          <CleanDataSection value={cleanData} onChange={setCleanData} />

          {/* Step list */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 0 }}>
            {steps.map((step, i) => (
              <MatchStepRow
                key={step._id}
                step={step}
                stepIndex={i}
                color={colors[step._id] ?? '#4f6ef7'}
                onUpdate={patch => updateStep(step._id, patch)}
                onRemove={() => removeStep(step._id)}
                onMove={handleMove}
                isFirst={i === 0}
                isLast={i === steps.length - 1}
              />
            ))}
          </div>

          {/* Add step */}
          <button style={addStepBtnStyle} onClick={() => addStep(defaultStep())}>
            + Add step
          </button>

          {/* Sequence options */}
          <div style={{ ...sectionStyle, marginTop: 8 }}>
            <button style={sectionHeaderBtnStyle} onClick={() => setOptionsOpen(v => !v)}>
              <span style={sectionLabelStyle}>▸ Sequence options</span>
              <span style={{ fontSize: 10, color: '#a0aec0', marginLeft: 'auto' }}>
                {optionsOpen ? '▴' : '▾'}
              </span>
            </button>
            {optionsOpen && (
              <div style={{ padding: '8px 10px', display: 'flex', flexDirection: 'column', gap: 8 }}>
                <DurationInput
                  label="Max duration"
                  valueMs={constraints?.maxDurationMs}
                  onChange={v => setConstraints({ ...constraints, maxDurationMs: v })}
                />
                <DurationInput
                  label="Min duration"
                  valueMs={constraints?.minDurationMs}
                  onChange={v => setConstraints({ ...constraints, minDurationMs: v })}
                />
              </div>
            )}
          </div>

          {/* Sequence model */}
          <SequenceModelPanel />

          {/* Save as Fragment */}
          {onSaveFragment && steps.length >= 2 && (
            <button style={saveFragmentBtnStyle} onClick={handleOpenSaveDialog}>
              Save as Fragment
            </button>
          )}
        </div>

        {/* RIGHT column — fills remaining space */}
        <div style={rightColumnStyle}>
          <SequenceExamplesPane />
        </div>
      </div>

      {/* Save as Fragment dialog */}
      {fragmentDialog && (
        <div style={dialogOverlayStyle}>
          <div style={dialogBoxStyle}>
            <div style={{ fontWeight: 700, fontSize: 14, marginBottom: 12, color: '#2d3748' }}>
              Save as Fragment
            </div>
            <label style={dialogLabelStyle}>Name</label>
            <input
              style={dialogInputStyle}
              value={fragmentDialog.name}
              onChange={e => setFragmentDialog(d => d ? { ...d, name: e.target.value } : d)}
            />
            <label style={dialogLabelStyle}>Label</label>
            <input
              style={dialogInputStyle}
              value={fragmentDialog.label}
              onChange={e => setFragmentDialog(d => d ? { ...d, label: e.target.value } : d)}
            />
            <label style={dialogLabelStyle}>Color</label>
            <input
              type="color"
              style={{ ...dialogInputStyle, padding: '2px 4px', height: 32, cursor: 'pointer' }}
              value={fragmentDialog.color}
              onChange={e => setFragmentDialog(d => d ? { ...d, color: e.target.value } : d)}
            />
            <div style={{ display: 'flex', gap: 8, marginTop: 16, justifyContent: 'flex-end' }}>
              <button style={dialogCancelBtnStyle} onClick={() => setFragmentDialog(null)}>
                Cancel
              </button>
              <button style={dialogConfirmBtnStyle} onClick={handleConfirmFragment}>
                Save
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function DurationInput({ label, valueMs, onChange }: { label: string; valueMs?: number; onChange: (v: number | undefined) => void }) {
  const [raw, setRaw] = useState(valueMs != null ? String(valueMs / 1000) : '')

  function commit(v: string) {
    const n = parseFloat(v)
    onChange(isNaN(n) || v === '' ? undefined : Math.round(n * 1000))
  }

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
      <span style={{ fontSize: 12, color: '#4a5568', width: 90, flexShrink: 0 }}>{label}</span>
      <input
        type="number"
        min="0"
        step="0.1"
        value={raw}
        placeholder="—"
        onChange={e => setRaw(e.target.value)}
        onBlur={e => commit(e.target.value)}
        style={durationInputStyle}
      />
      <span style={{ fontSize: 11, color: '#a0aec0' }}>s</span>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const outerPanelStyle: React.CSSProperties = {
  display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden',
}
const panelHeaderStyle: React.CSSProperties = {
  display: 'flex', alignItems: 'center', gap: 8,
  padding: '10px 12px', borderBottom: '1px solid var(--color-border, #e2e8f0)', flexShrink: 0,
}
const columnsStyle: React.CSSProperties = {
  display: 'flex', flex: 1, overflow: 'hidden',
}
const leftColumnStyle: React.CSSProperties = {
  width: 320, flexShrink: 0, display: 'flex', flexDirection: 'column',
  padding: '12px', overflowY: 'auto',
  borderRight: '1px solid var(--color-border, #e2e8f0)',
}
const rightColumnStyle: React.CSSProperties = {
  flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden',
  minWidth: 0,
}
const headerLabelStyle: React.CSSProperties = {
  fontSize: 13, fontWeight: 800, color: '#2d3748', textTransform: 'uppercase', letterSpacing: '0.05em',
}
const loadingBadgeStyle: React.CSSProperties = {
  fontSize: 11, color: '#718096', padding: '2px 6px', background: '#edf2f7', borderRadius: 10,
}
const solBtnStyle: React.CSSProperties = {
  padding: '3px 10px', fontSize: 11, fontWeight: 600, background: '#fff',
  color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 5, cursor: 'pointer',
}
const solPopoverStyle: React.CSSProperties = {
  background: '#1a202c', borderRadius: 8, padding: '12px', marginBottom: 10,
  border: '1px solid #2d3748',
}
const solPreStyle: React.CSSProperties = {
  margin: 0, fontSize: 12, color: '#e2e8f0', fontFamily: 'monospace', whiteSpace: 'pre-wrap', lineHeight: 1.6,
}
const solCloseBtnStyle: React.CSSProperties = {
  background: 'none', border: 'none', cursor: 'pointer', color: '#718096', fontSize: 12, padding: 0,
}
const errorBannerStyle: React.CSSProperties = {
  display: 'flex', alignItems: 'center', justifyContent: 'space-between',
  background: '#fff5f5', border: '1px solid #feb2b2', borderRadius: 6,
  padding: '6px 10px', marginBottom: 8,
}
const errorDismissStyle: React.CSSProperties = {
  background: 'none', border: 'none', cursor: 'pointer', color: '#fc8181', fontSize: 12, padding: 0,
}
const addStepBtnStyle: React.CSSProperties = {
  marginTop: 8, padding: '6px 12px', fontSize: 12, fontWeight: 600,
  background: '#ebf8ff', color: '#2b6cb0', border: '1px dashed #bee3f8',
  borderRadius: 7, cursor: 'pointer', width: '100%',
}
const sectionStyle: React.CSSProperties = {
  border: '1px solid #e2e8f0', borderRadius: 8, background: '#fafafa',
}
const sectionHeaderBtnStyle: React.CSSProperties = {
  display: 'flex', alignItems: 'center', gap: 6, width: '100%',
  background: 'none', border: 'none', cursor: 'pointer', padding: '8px 10px', textAlign: 'left',
}
const sectionLabelStyle: React.CSSProperties = {
  fontSize: 12, fontWeight: 700, color: '#4a5568', textTransform: 'uppercase', letterSpacing: '0.04em',
}
const saveFragmentBtnStyle: React.CSSProperties = {
  marginTop: 12, padding: '7px 14px', fontSize: 12, fontWeight: 700,
  background: '#4f6ef7', color: '#fff', border: 'none',
  borderRadius: 7, cursor: 'pointer', width: '100%',
}
const durationInputStyle: React.CSSProperties = {
  width: 70, padding: '3px 6px', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 12,
}
const dialogOverlayStyle: React.CSSProperties = {
  position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex',
  alignItems: 'center', justifyContent: 'center', zIndex: 1000,
}
const dialogBoxStyle: React.CSSProperties = {
  background: '#fff', borderRadius: 10, padding: '20px 24px', minWidth: 320, maxWidth: 400,
  boxShadow: '0 8px 32px rgba(0,0,0,0.18)',
  display: 'flex', flexDirection: 'column', gap: 4,
}
const dialogLabelStyle: React.CSSProperties = {
  fontSize: 11, fontWeight: 600, color: '#718096', textTransform: 'uppercase',
  letterSpacing: '0.04em', marginTop: 8, marginBottom: 2, display: 'block',
}
const dialogInputStyle: React.CSSProperties = {
  width: '100%', padding: '6px 8px', border: '1px solid #cbd5e0', borderRadius: 5,
  fontSize: 13, boxSizing: 'border-box',
}
const dialogCancelBtnStyle: React.CSSProperties = {
  padding: '7px 16px', fontSize: 13, background: '#edf2f7', color: '#4a5568',
  border: 'none', borderRadius: 6, cursor: 'pointer',
}
const dialogConfirmBtnStyle: React.CSSProperties = {
  padding: '7px 16px', fontSize: 13, fontWeight: 700, background: '#4f6ef7',
  color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer',
}
