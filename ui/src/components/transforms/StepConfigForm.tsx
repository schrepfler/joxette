import { useState } from 'react'
import type { PipelineStep, FragmentDefinition, GapTransformStep, GapSelector, MessagePattern } from '#/transforms/types'
import { PredicateBuilder } from './PredicateBuilder'
import { NestedPipelineBuilder } from './NestedPipelineBuilder'
import { PatternBuilder } from './PatternBuilder'

interface Props {
  step: PipelineStep
  onChange: (updated: PipelineStep) => void
  fragments?: FragmentDefinition[]
}

export function StepConfigForm({ step, onChange, fragments }: Props) {
  function patch(fields: object) {
    onChange({ ...step, ...fields } as PipelineStep)
  }

  return (
    <>
      {renderStepFields(step, patch, fragments ?? [])}
      <WhenGuardSection step={step} onChange={onChange} />
    </>
  )
}

// ---------------------------------------------------------------------------
// Step-specific field renderers
// ---------------------------------------------------------------------------

function renderStepFields(step: PipelineStep, patch: (fields: object) => void, fragments: FragmentDefinition[]) {
  switch (step.type) {
    case 'wall_time':
      return (
        <Grid>
          <Field label="Target" help="$.timestamp, $.recorded_at, or ALL_TIMESTAMPS">
            <TextInput value={step.target} onChange={v => patch({ target: v })} />
          </Field>
        </Grid>
      )

    case 'time_shift':
      return (
        <Grid>
          <Field label="Target" help="$.timestamp, $.recorded_at, or ALL_TIMESTAMPS">
            <TextInput value={step.target} onChange={v => patch({ target: v })} />
          </Field>
          <Field label="Shift (ms)" help="Positive = forward, negative = backward">
            <NumberInput value={step.shiftMs} onChange={v => patch({ shiftMs: v })} />
          </Field>
        </Grid>
      )

    case 'time_compress':
      return (
        <Grid>
          <Field label="Target" help="$.timestamp or ALL_TIMESTAMPS">
            <TextInput value={step.target} onChange={v => patch({ target: v })} />
          </Field>
          <Field label="Factor" help="2.0 = halve gaps, 0.5 = double gaps">
            <NumberInput value={step.factor} step={0.1} onChange={v => patch({ factor: v })} />
          </Field>
        </Grid>
      )

    case 'time_freeze':
      return (
        <Grid>
          <Field label="Target" help="$.timestamp or ALL_TIMESTAMPS">
            <TextInput value={step.target} onChange={v => patch({ target: v })} />
          </Field>
          <Field label="Frozen At" help="ISO-8601 instant or NOW">
            <TextInput value={step.frozenAt} onChange={v => patch({ frozenAt: v })} />
          </Field>
        </Grid>
      )

    case 'set_constant':
      return (
        <Grid>
          <Field label="Target" help="JSONPath e.g. $.value.env">
            <TextInput value={step.target} onChange={v => patch({ target: v })} />
          </Field>
          <Field label="Value (JSON)" help="Any JSON value: string, number, boolean, object, array">
            <JsonInput value={step.value} onChange={v => patch({ value: v })} />
          </Field>
        </Grid>
      )

    case 'copy_field':
      return (
        <Grid>
          <Field label="From" help="Source JSONPath">
            <TextInput value={step.from} onChange={v => patch({ from: v })} />
          </Field>
          <Field label="To" help="Destination JSONPath">
            <TextInput value={step.to} onChange={v => patch({ to: v })} />
          </Field>
        </Grid>
      )

    case 'template':
      return (
        <Grid>
          <Field label="Target" help="JSONPath where result is written">
            <TextInput value={step.target} onChange={v => patch({ target: v })} />
          </Field>
          <Field label="Template" help="${path} placeholders resolved against message">
            <TextInput value={step.template} onChange={v => patch({ template: v })} />
          </Field>
        </Grid>
      )

    case 'redact':
      return (
        <Grid>
          <Field label="Target" help="JSONPath to the field to redact">
            <TextInput value={step.target} onChange={v => patch({ target: v })} />
          </Field>
        </Grid>
      )

    case 'mask_hash':
      return (
        <Grid>
          <Field label="Target" help="JSONPath to the field to hash">
            <TextInput value={step.target} onChange={v => patch({ target: v })} />
          </Field>
          <Field label="Prefix" help="Prepended to the hex digest">
            <TextInput value={step.prefix ?? ''} onChange={v => patch({ prefix: v || undefined })} />
          </Field>
          <Field label="Salt" help="Prepended to the value before hashing">
            <TextInput value={step.salt ?? ''} onChange={v => patch({ salt: v || undefined })} />
          </Field>
        </Grid>
      )

    case 'coalesce':
      return (
        <Grid>
          <Field label="Sources (one per line)" help="JSONPath list; first non-null wins">
            <MultiLineInput value={step.sources} onChange={v => patch({ sources: v })} />
          </Field>
          <Field label="Target" help="JSONPath to write the result">
            <TextInput value={step.target} onChange={v => patch({ target: v })} />
          </Field>
          <Field label="Fallback (JSON)" help="Written if all sources are null">
            <JsonInput value={step.fallback} onChange={v => patch({ fallback: v })} nullable />
          </Field>
        </Grid>
      )

    case 'rename_field':
      return (
        <Grid>
          <Field label="Source" help="JSONPath to the key to rename">
            <TextInput value={step.source} onChange={v => patch({ source: v })} />
          </Field>
          <Field label="New Name" help="Bare leaf key name (no path)">
            <TextInput value={step.new_name} onChange={v => patch({ new_name: v })} />
          </Field>
        </Grid>
      )

    case 'delete_field':
      return (
        <Grid>
          <Field label="Target" help="JSONPath of the field to remove">
            <TextInput value={step.target} onChange={v => patch({ target: v })} />
          </Field>
        </Grid>
      )

    case 'flatten_field':
      return (
        <Grid>
          <Field label="Source" help="JSONPath to the nested object to flatten">
            <TextInput value={step.source} onChange={v => patch({ source: v })} />
          </Field>
          <Field label="Prefix" help="Prepended to each hoisted key (optional)">
            <TextInput value={step.prefix ?? ''} onChange={v => patch({ prefix: v || undefined })} />
          </Field>
        </Grid>
      )

    case 'add_computed_field':
      return (
        <Grid>
          <Field label="Target" help="JSONPath where result is written">
            <TextInput value={step.target} onChange={v => patch({ target: v })} />
          </Field>
          <Field label="Expression" help="JSONPath expression evaluated against message">
            <TextInput value={step.expression} onChange={v => patch({ expression: v })} />
          </Field>
        </Grid>
      )

    case 'merge_patch':
      return (
        <Grid>
          <Field label="Target" help="JSONPath to the object to patch (e.g. $.value)">
            <TextInput value={step.target} onChange={v => patch({ target: v })} />
          </Field>
          <Field label="Patch (JSON object)" help="RFC 7396 merge patch; null values remove keys">
            <JsonInput value={step.patch} onChange={v => patch({ patch: v as Record<string, unknown> })} />
          </Field>
        </Grid>
      )

    case 'remap_key':
      return (
        <Grid>
          <Field label="Source" help="JSONPath into message value">
            <TextInput value={step.source} onChange={v => patch({ source: v })} />
          </Field>
        </Grid>
      )

    case 'null_key':
      return <p style={noParamsStyle}>No parameters — sets message key to null.</p>

    case 'key_from_value':
      return (
        <Grid>
          <Field label="Expression" help="Template expression e.g. ${value.order_id}">
            <TextInput value={step.expression} onChange={v => patch({ expression: v })} />
          </Field>
        </Grid>
      )

    case 'add_header':
      return (
        <Grid>
          <Field label="Key" help="Header name">
            <TextInput value={step.key} onChange={v => patch({ key: v })} />
          </Field>
          <Field label="Value" help="Literal string or ${path} template">
            <TextInput value={step.value} onChange={v => patch({ value: v })} />
          </Field>
          <Field label="If Absent">
            <CheckboxInput
              id="add-header-if-absent"
              label="Only add when header doesn't exist"
              checked={step.if_absent ?? false}
              onChange={v => patch({ if_absent: v })}
            />
          </Field>
        </Grid>
      )

    case 'remove_header':
      return (
        <Grid>
          <Field label="Key" help="Header name to remove">
            <TextInput value={step.key} onChange={v => patch({ key: v })} />
          </Field>
        </Grid>
      )

    case 'copy_to_header':
      return (
        <Grid>
          <Field label="Source" help="JSONPath in message value">
            <TextInput value={step.source} onChange={v => patch({ source: v })} />
          </Field>
          <Field label="Header Key" help="Kafka header name to write to">
            <TextInput value={step.headerKey} onChange={v => patch({ headerKey: v })} />
          </Field>
        </Grid>
      )

    case 'redirect_topic':
      return (
        <Grid>
          <Field label="Topic" help="Target topic name or ${path} template">
            <TextInput value={step.topic} onChange={v => patch({ topic: v })} />
          </Field>
        </Grid>
      )

    case 'fan_out':
      return (
        <Grid>
          <Field label="Topics (one per line)" help="Messages are duplicated to each topic">
            <MultiLineInput value={step.topics} onChange={v => patch({ topics: v })} />
          </Field>
        </Grid>
      )

    case 'filter_drop':
      return (
        <div style={{ padding: '0.75rem 0' }}>
          <label style={labelStyle}>
            Drop when
            <span style={{ fontWeight: 400, color: '#718096', marginLeft: 4, fontSize: 11 }}>
              (predicate evaluates to true → message is dropped)
            </span>
          </label>
          <div style={{ marginTop: 6 }}>
            <PredicateBuilder
              value={step.predicate}
              onChange={p => patch({ predicate: p ?? { field: '', operator: 'EQ' } })}
            />
          </div>
        </div>
      )

    case 'conditional':
      return (
        <div style={{ padding: '0.75rem 0', display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
          <div>
            <label style={labelStyle}>
              Condition
              <span style={{ fontWeight: 400, color: '#718096', marginLeft: 4, fontSize: 11 }}>
                (true → then steps; false → else steps)
              </span>
            </label>
            <div style={{ marginTop: 6 }}>
              <PredicateBuilder
                value={step.condition}
                onChange={p => patch({ condition: p ?? { field: '', operator: 'EQ' } })}
              />
            </div>
          </div>
          <NestedPipelineBuilder
            steps={step.then_steps}
            onChange={steps => patch({ then_steps: steps })}
            label="Then steps (condition is true)"
          />
          <NestedPipelineBuilder
            steps={step.else_steps}
            onChange={steps => patch({ else_steps: steps })}
            label="Else steps (condition is false)"
          />
        </div>
      )

    case 'gap_transform':
      return (
        <GapTransformFields
          step={step}
          patch={patch}
          fragments={fragments}
        />
      )

    default:
      return <p style={noParamsStyle}>Unknown step type.</p>
  }
}

// ---------------------------------------------------------------------------
// Per-step 'when' guard section
// ---------------------------------------------------------------------------

function WhenGuardSection({ step, onChange }: { step: PipelineStep; onChange: (updated: PipelineStep) => void }) {
  const [open, setOpen] = useState(!!step.when)
  const hasGuard = !!step.when

  function toggle() {
    if (open && hasGuard) {
      // Remove the guard before closing
      onChange({ ...step, when: undefined })
    }
    setOpen(o => !o)
  }

  function handleRemoveGuard() {
    onChange({ ...step, when: undefined })
    setOpen(false)
  }

  return (
    <div style={guardContainerStyle}>
      <div style={guardHeaderStyle}>
        <button
          style={guardToggleBtnStyle}
          onClick={toggle}
          aria-expanded={open}
        >
          <span style={{ fontSize: 10, marginRight: 4 }}>{open ? '▲' : '▼'}</span>
          Only apply when…
        </button>
        {hasGuard && !open && (
          <span style={guardBadgeStyle}>guard active</span>
        )}
        {hasGuard && open && (
          <button style={removeGuardBtnStyle} onClick={handleRemoveGuard} title="Remove guard">
            Remove guard
          </button>
        )}
      </div>

      {open && (
        <div style={{ padding: '0.5rem 0.75rem 0.6rem' }}>
          <p style={{ margin: '0 0 0.4rem', fontSize: 11, color: '#718096' }}>
            Step only runs when this predicate matches the message. Leave empty to always run.
          </p>
          <PredicateBuilder
            value={step.when ?? null}
            onChange={p => onChange({ ...step, when: p ?? undefined })}
            nullable
          />
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Shared sub-components
// ---------------------------------------------------------------------------

function Grid({ children }: { children: React.ReactNode }) {
  return (
    <div style={{
      display: 'grid',
      gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))',
      gap: '0.6rem 1rem',
      padding: '0.75rem 0',
    }}>
      {children}
    </div>
  )
}

function Field({ label, help, children }: { label: string; help?: string; children: React.ReactNode }) {
  return (
    <div>
      <label style={labelStyle}>{label}</label>
      {children}
      {help && <p style={helpStyle}>{help}</p>}
    </div>
  )
}

function TextInput({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  return (
    <input
      type="text"
      value={value}
      onChange={e => onChange(e.target.value)}
      style={inputStyle}
    />
  )
}

function NumberInput({ value, onChange, step }: { value: number; onChange: (v: number) => void; step?: number }) {
  return (
    <input
      type="number"
      value={value}
      step={step ?? 1}
      onChange={e => onChange(Number(e.target.value))}
      style={inputStyle}
    />
  )
}

function MultiLineInput({ value, onChange }: { value: string[]; onChange: (v: string[]) => void }) {
  return (
    <textarea
      value={value.join('\n')}
      onChange={e => onChange(e.target.value.split('\n').filter(Boolean))}
      rows={3}
      style={{ ...inputStyle, resize: 'vertical', fontFamily: 'monospace', fontSize: 12 }}
    />
  )
}

function JsonInput({ value, onChange, nullable }: { value: unknown; onChange: (v: unknown) => void; nullable?: boolean }) {
  const [raw, setRaw] = useState(() => {
    if (nullable && value == null) return ''
    return JSON.stringify(value, null, 2)
  })
  const [err, setErr] = useState(false)

  function handleChange(text: string) {
    setRaw(text)
    if (nullable && text.trim() === '') {
      setErr(false)
      onChange(undefined)
      return
    }
    try {
      onChange(JSON.parse(text))
      setErr(false)
    } catch {
      setErr(true)
    }
  }

  return (
    <>
      <textarea
        value={raw}
        onChange={e => handleChange(e.target.value)}
        rows={3}
        style={{
          ...inputStyle,
          resize: 'vertical',
          fontFamily: 'monospace',
          fontSize: 12,
          borderColor: err ? '#e53e3e' : '#cbd5e0',
        }}
      />
      {err && <p style={{ margin: '2px 0 0', fontSize: 11, color: '#e53e3e' }}>Invalid JSON</p>}
    </>
  )
}

function CheckboxInput({
  id,
  label,
  checked,
  onChange,
}: {
  id: string
  label: string
  checked: boolean
  onChange: (v: boolean) => void
}) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6, paddingTop: 4 }}>
      <input id={id} type="checkbox" checked={checked} onChange={e => onChange(e.target.checked)} />
      <label
        htmlFor={id}
        style={{ fontSize: 13, color: '#4a5568', userSelect: 'none', cursor: 'pointer' }}
      >
        {label}
      </label>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const labelStyle: React.CSSProperties = {
  display: 'block', marginBottom: 3, fontSize: 12, fontWeight: 600, color: '#4a5568',
}
const helpStyle: React.CSSProperties = {
  margin: '2px 0 0', fontSize: 11, color: '#718096', lineHeight: 1.3,
}
const inputStyle: React.CSSProperties = {
  padding: '0.35rem 0.55rem', border: '1px solid #cbd5e0', borderRadius: 4,
  fontSize: 13, width: '100%', boxSizing: 'border-box',
}
const noParamsStyle: React.CSSProperties = {
  margin: '0.5rem 0', fontSize: 13, color: '#718096',
}
const guardContainerStyle: React.CSSProperties = {
  borderTop: '1px dashed #e2e8f0',
  marginTop: '0.5rem',
}
const guardHeaderStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: 8,
  padding: '0.3rem 0.75rem',
  background: '#f7fafc',
}
const guardToggleBtnStyle: React.CSSProperties = {
  background: 'none',
  border: 'none',
  cursor: 'pointer',
  fontSize: 12,
  color: '#4a5568',
  padding: 0,
  fontWeight: 600,
}
const guardBadgeStyle: React.CSSProperties = {
  fontSize: 10,
  fontWeight: 700,
  padding: '1px 6px',
  borderRadius: 8,
  background: '#ebf8ff',
  color: '#2b6cb0',
  border: '1px solid #bee3f8',
  textTransform: 'uppercase',
  letterSpacing: '0.04em',
}
const removeGuardBtnStyle: React.CSSProperties = {
  fontSize: 11,
  color: '#e53e3e',
  background: 'none',
  border: '1px solid #fed7d7',
  borderRadius: 3,
  padding: '1px 6px',
  cursor: 'pointer',
}

// ---------------------------------------------------------------------------
// GapTransformFields — gap_transform step config
// ---------------------------------------------------------------------------

const DEFAULT_PATTERN: MessagePattern = { predicate: { field: '', operator: 'EQ' }, quantifier: 'first' }

const GAP_SELECTOR_MODES = [
  { value: 'after', label: 'After message' },
  { value: 'before', label: 'Before message' },
  { value: 'between', label: 'Between two messages' },
  { value: 'within_fragment', label: 'Within phase' },
  { value: 'threshold', label: 'Duration threshold only' },
] as const
type SelectorMode = typeof GAP_SELECTOR_MODES[number]['value']

function selectorMode(sel: GapSelector): SelectorMode {
  if (sel.within_fragment) return 'within_fragment'
  if (sel.after && sel.before) return 'between'
  if (sel.after) return 'after'
  if (sel.before) return 'before'
  return 'threshold'
}

const GAP_OPS = ['cut', 'hold', 'trim', 'pad', 'scale'] as const

function GapTransformFields({
  step,
  patch,
  fragments,
}: {
  step: GapTransformStep
  patch: (fields: object) => void
  fragments: FragmentDefinition[]
}) {
  const sel = step.select
  const op = step.operation
  const mode = selectorMode(sel)

  function patchSel(fields: Partial<GapSelector>) {
    patch({ select: { ...sel, ...fields } })
  }

  function setMode(m: SelectorMode) {
    switch (m) {
      case 'after': patch({ select: { after: sel.after ?? DEFAULT_PATTERN, min_duration_ms: sel.min_duration_ms } }); break
      case 'before': patch({ select: { before: sel.before ?? DEFAULT_PATTERN, min_duration_ms: sel.min_duration_ms } }); break
      case 'between': patch({ select: { after: sel.after ?? DEFAULT_PATTERN, before: sel.before ?? DEFAULT_PATTERN, min_duration_ms: sel.min_duration_ms } }); break
      case 'within_fragment': patch({ select: { within_fragment: fragments[0]?.name ?? '', min_duration_ms: sel.min_duration_ms } }); break
      case 'threshold': patch({ select: { min_duration_ms: sel.min_duration_ms } }); break
    }
  }

  function setOpType(opType: string) {
    switch (opType) {
      case 'cut': patch({ operation: { op: 'cut' } }); break
      case 'hold': patch({ operation: { op: 'hold', target_ms: (op as { target_ms?: number }).target_ms ?? 500 } }); break
      case 'trim': patch({ operation: { op: 'trim', by_ms: (op as { by_ms?: number }).by_ms ?? 1000 } }); break
      case 'pad': patch({ operation: { op: 'pad', by_ms: (op as { by_ms?: number }).by_ms ?? 500 } }); break
      case 'scale': patch({ operation: { op: 'scale', factor: (op as { factor?: number }).factor ?? 0.5 } }); break
    }
  }

  return (
    <div style={{ padding: '0.75rem 0', display: 'flex', flexDirection: 'column', gap: '0.85rem' }}>
      {/* Selector */}
      <div>
        <label style={labelStyle}>Gap selector</label>
        <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap', marginBottom: '0.5rem' }}>
          {GAP_SELECTOR_MODES.map(m => (
            <button
              key={m.value}
              onClick={() => setMode(m.value)}
              style={{
                padding: '2px 8px', fontSize: 11, fontWeight: 600,
                border: '1px solid #cbd5e0', borderRadius: 3, cursor: 'pointer',
                background: mode === m.value ? '#3182ce' : '#fff',
                color: mode === m.value ? '#fff' : '#4a5568',
              }}
            >
              {m.label}
            </button>
          ))}
        </div>

        {(mode === 'after' || mode === 'between') && (
          <div style={{ marginBottom: '0.5rem' }}>
            <div style={subLabelStyle}>After</div>
            <PatternBuilder
              value={sel.after ?? DEFAULT_PATTERN}
              onChange={p => patchSel({ after: p })}
            />
          </div>
        )}
        {(mode === 'before' || mode === 'between') && (
          <div style={{ marginBottom: '0.5rem' }}>
            <div style={subLabelStyle}>Before</div>
            <PatternBuilder
              value={sel.before ?? DEFAULT_PATTERN}
              onChange={p => patchSel({ before: p })}
            />
          </div>
        )}
        {mode === 'within_fragment' && (
          <div style={{ marginBottom: '0.5rem' }}>
            <div style={subLabelStyle}>Phase</div>
            {fragments.length === 0 ? (
              <p style={{ fontSize: 12, color: '#a0aec0', margin: 0 }}>No phases defined. Add one in the Phases section above.</p>
            ) : (
              <select
                value={sel.within_fragment ?? fragments[0].name}
                onChange={e => patchSel({ within_fragment: e.target.value })}
                style={{ padding: '0.25rem 0.4rem', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 12 }}
              >
                {fragments.map(f => (
                  <option key={f.name} value={f.name}>{f.label || f.name}</option>
                ))}
              </select>
            )}
          </div>
        )}

        {/* Duration threshold filters (always available) */}
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center' }}>
          <label style={{ fontSize: 12, color: '#4a5568', display: 'flex', alignItems: 'center', gap: 4 }}>
            Min duration
            <input
              type="number" min={0}
              value={sel.min_duration_ms ?? ''}
              placeholder="—"
              onChange={e => patchSel({ min_duration_ms: e.target.value === '' ? undefined : parseInt(e.target.value) })}
              style={{ width: 80, padding: '2px 5px', fontSize: 12, border: '1px solid #cbd5e0', borderRadius: 4, boxSizing: 'border-box' }}
            />
            <span style={{ fontSize: 11, color: '#a0aec0' }}>ms</span>
          </label>
          <label style={{ fontSize: 12, color: '#4a5568', display: 'flex', alignItems: 'center', gap: 4 }}>
            Max duration
            <input
              type="number" min={0}
              value={sel.max_duration_ms ?? ''}
              placeholder="—"
              onChange={e => patchSel({ max_duration_ms: e.target.value === '' ? undefined : parseInt(e.target.value) })}
              style={{ width: 80, padding: '2px 5px', fontSize: 12, border: '1px solid #cbd5e0', borderRadius: 4, boxSizing: 'border-box' }}
            />
            <span style={{ fontSize: 11, color: '#a0aec0' }}>ms</span>
          </label>
        </div>
      </div>

      {/* Operation */}
      <div>
        <label style={labelStyle}>Operation</label>
        <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap', marginBottom: '0.5rem' }}>
          {GAP_OPS.map(o => (
            <button
              key={o}
              onClick={() => setOpType(o)}
              style={{
                padding: '2px 10px', fontSize: 11, fontWeight: 600,
                border: '1px solid #cbd5e0', borderRadius: 3, cursor: 'pointer',
                background: op.op === o ? '#805ad5' : '#fff',
                color: op.op === o ? '#fff' : '#4a5568',
                textTransform: 'uppercase',
              }}
            >
              {o}
            </button>
          ))}
        </div>
        {op.op === 'hold' && (
          <label style={{ fontSize: 12, color: '#4a5568', display: 'flex', alignItems: 'center', gap: 4 }}>
            Target duration
            <input
              type="number" min={0}
              value={op.target_ms}
              onChange={e => patch({ operation: { op: 'hold', target_ms: parseInt(e.target.value) || 0 } })}
              style={{ width: 90, padding: '2px 5px', fontSize: 12, border: '1px solid #cbd5e0', borderRadius: 4 }}
            />
            <span style={{ fontSize: 11, color: '#a0aec0' }}>ms</span>
          </label>
        )}
        {op.op === 'trim' && (
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center' }}>
            <label style={{ fontSize: 12, color: '#4a5568', display: 'flex', alignItems: 'center', gap: 4 }}>
              By ms
              <input
                type="number" min={0}
                value={op.by_ms ?? ''}
                placeholder="—"
                onChange={e => patch({ operation: { op: 'trim', by_ms: e.target.value === '' ? undefined : parseInt(e.target.value), by_factor: op.by_factor } })}
                style={{ width: 80, padding: '2px 5px', fontSize: 12, border: '1px solid #cbd5e0', borderRadius: 4 }}
              />
              <span style={{ fontSize: 11, color: '#a0aec0' }}>ms</span>
            </label>
            <label style={{ fontSize: 12, color: '#4a5568', display: 'flex', alignItems: 'center', gap: 4 }}>
              By factor
              <input
                type="number" step={0.01} min={0} max={1}
                value={op.by_factor ?? ''}
                placeholder="—"
                onChange={e => patch({ operation: { op: 'trim', by_ms: op.by_ms, by_factor: e.target.value === '' ? undefined : parseFloat(e.target.value) } })}
                style={{ width: 70, padding: '2px 5px', fontSize: 12, border: '1px solid #cbd5e0', borderRadius: 4 }}
              />
            </label>
          </div>
        )}
        {op.op === 'pad' && (
          <label style={{ fontSize: 12, color: '#4a5568', display: 'flex', alignItems: 'center', gap: 4 }}>
            Extend by
            <input
              type="number" min={0}
              value={op.by_ms}
              onChange={e => patch({ operation: { op: 'pad', by_ms: parseInt(e.target.value) || 0 } })}
              style={{ width: 90, padding: '2px 5px', fontSize: 12, border: '1px solid #cbd5e0', borderRadius: 4 }}
            />
            <span style={{ fontSize: 11, color: '#a0aec0' }}>ms</span>
          </label>
        )}
        {op.op === 'scale' && (
          <label style={{ fontSize: 12, color: '#4a5568', display: 'flex', alignItems: 'center', gap: 4 }}>
            Factor
            <input
              type="number" step={0.01} min={0}
              value={op.factor}
              onChange={e => patch({ operation: { op: 'scale', factor: parseFloat(e.target.value) || 0 } })}
              style={{ width: 80, padding: '2px 5px', fontSize: 12, border: '1px solid #cbd5e0', borderRadius: 4 }}
            />
            <span style={{ fontSize: 11, color: '#a0aec0' }}>×</span>
          </label>
        )}
        {op.op === 'cut' && (
          <p style={{ margin: 0, fontSize: 12, color: '#718096' }}>Eliminates the gap — the next message plays immediately after the previous one.</p>
        )}
      </div>
    </div>
  )
}

const subLabelStyle: React.CSSProperties = {
  fontSize: 11, fontWeight: 700, color: '#718096',
  textTransform: 'uppercase', letterSpacing: '0.04em', marginBottom: '0.3rem',
}
