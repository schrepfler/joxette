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
  const { query, results, loading, error } = useSequenceStore()
  const { steps, constraints } = query
  const colors = stepColors(steps)

  const [cleanData, setCleanData] = useState<CleanDataState>({})
  const [optionsOpen, setOptionsOpen] = useState(false)
  const [solOpen, setSolOpen] = useState(false)

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

  function handleSaveFragment() {
    if (!onSaveFragment || steps.length < 1) return
    const first = steps[0]
    const last = steps[steps.length - 1]
    const fragment: FragmentDefinition = {
      name: `sequence_${Date.now()}`,
      label: `${inferStepLabel(first, 0)} → ${inferStepLabel(last, steps.length - 1)}`,
      color: colors[first._id] ?? '#4f6ef7',
      from: { predicate: first.predicate, quantifier: 'first' },
      to: { predicate: last.predicate, quantifier: 'last' },
      ...(constraints?.maxDurationMs ? { if: { max_duration_ms: constraints.maxDurationMs } } : {}),
    }
    onSaveFragment(fragment)
  }

  const sol = buildSOL(steps, constraints)

  return (
    <div style={panelStyle}>
      {/* Panel header */}
      <div style={headerRowStyle}>
        <span style={headerLabelStyle}>▸ Match</span>
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 6 }}>
          {loading && <span style={loadingBadgeStyle}>…</span>}
          {results && (
            <span style={resultBadgeStyle}>
              {results.matchedSequences.toLocaleString()} sequences ({Math.round(results.matchRate * 100)}%)
            </span>
          )}
          <button style={solBtnStyle} onClick={() => setSolOpen(v => !v)} title="View SOL query">
            View SOL
          </button>
        </div>
      </div>

      {/* SOL popover */}
      {solOpen && (
        <div style={solPopoverStyle}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
            <span style={{ fontSize: 11, fontWeight: 700, color: '#718096', textTransform: 'uppercase', letterSpacing: '0.04em' }}>SOL Query</span>
            <button style={solCloseBtnStyle} onClick={() => setSolOpen(false)}>✕</button>
          </div>
          <pre style={solPreStyle}>{sol}</pre>
        </div>
      )}

      {/* Error banner */}
      {error && (
        <div style={errorBannerStyle}>
          <span style={{ fontSize: 12, color: '#c53030' }}>{error}</span>
          <button style={errorDismissStyle} onClick={() => setError(null)}>✕</button>
        </div>
      )}

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

      {/* Sequence model — reach rates */}
      {results && results.reachRates.length > 0 && (
        <div style={{ ...sectionStyle, marginTop: 8 }}>
          <div style={sectionLabelRowStyle}>
            <span style={sectionLabelStyle}>▸ Sequence model</span>
          </div>
          <div style={{ padding: '6px 10px', display: 'flex', flexDirection: 'column', gap: 4 }}>
            {steps.map((step, i) => {
              const rate = results.reachRates[i] ?? 0
              const label = inferStepLabel(step, i)
              return (
                <div key={step._id} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <div style={{
                    width: 10, height: 10, borderRadius: 2, background: colors[step._id] ?? '#4f6ef7', flexShrink: 0,
                  }} />
                  <span style={{ fontSize: 12, color: '#4a5568', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {label}
                  </span>
                  <div style={{ width: 80, height: 6, background: '#e2e8f0', borderRadius: 3, overflow: 'hidden' }}>
                    <div style={{
                      width: `${Math.round(rate * 100)}%`, height: '100%',
                      background: colors[step._id] ?? '#4f6ef7',
                    }} />
                  </div>
                  <span style={{ fontSize: 11, color: '#718096', width: 36, textAlign: 'right' }}>
                    {Math.round(rate * 100)}%
                  </span>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* Save as Fragment */}
      {onSaveFragment && steps.length >= 2 && (
        <button style={saveFragmentBtnStyle} onClick={handleSaveFragment}>
          Save as Fragment
        </button>
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

const panelStyle: React.CSSProperties = {
  display: 'flex', flexDirection: 'column', padding: '12px', minWidth: 280,
}
const headerRowStyle: React.CSSProperties = {
  display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10,
}
const headerLabelStyle: React.CSSProperties = {
  fontSize: 13, fontWeight: 800, color: '#2d3748', textTransform: 'uppercase', letterSpacing: '0.05em',
}
const loadingBadgeStyle: React.CSSProperties = {
  fontSize: 11, color: '#718096', padding: '2px 6px', background: '#edf2f7', borderRadius: 10,
}
const resultBadgeStyle: React.CSSProperties = {
  fontSize: 11, color: '#2b6cb0', padding: '2px 8px', background: '#ebf8ff',
  border: '1px solid #bee3f8', borderRadius: 10,
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
const sectionLabelRowStyle: React.CSSProperties = {
  padding: '8px 10px',
}
const saveFragmentBtnStyle: React.CSSProperties = {
  marginTop: 12, padding: '7px 14px', fontSize: 12, fontWeight: 700,
  background: '#4f6ef7', color: '#fff', border: 'none',
  borderRadius: 7, cursor: 'pointer', width: '100%',
}
const durationInputStyle: React.CSSProperties = {
  width: 70, padding: '3px 6px', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 12,
}
