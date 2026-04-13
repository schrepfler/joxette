import { useState } from 'react'
import type { PipelineStep, FilterOperator } from '#/transforms/types'

interface Props {
  step: PipelineStep
  onChange: (updated: PipelineStep) => void
}

export function StepConfigForm({ step, onChange }: Props) {
  function patch(fields: object) {
    onChange({ ...step, ...fields } as PipelineStep)
  }

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
            <MultiLineInput
              value={step.sources}
              onChange={v => patch({ sources: v })}
            />
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
        <Grid>
          <Field label="Field" help="JSONPath to evaluate">
            <TextInput value={step.field} onChange={v => patch({ field: v })} />
          </Field>
          <Field label="Operator">
            <SelectInput<FilterOperator>
              value={step.operator}
              onChange={v => patch({ operator: v })}
              options={FILTER_OPERATORS}
            />
          </Field>
          {step.operator !== 'IS_NULL' && step.operator !== 'IS_NOT_NULL' && (
            <Field label="Value" help="Value to compare against">
              <TextInput value={String(step.value ?? '')} onChange={v => patch({ value: v })} />
            </Field>
          )}
        </Grid>
      )

    case 'conditional':
      return (
        <Grid>
          <Field label="Condition" help="Expression string e.g. $.amount > 1000">
            <TextInput value={step.condition} onChange={v => patch({ condition: v })} />
          </Field>
          <p style={{ margin: '0.25rem 0', fontSize: 12, color: '#718096', gridColumn: '1/-1' }}>
            Nested then/else step configuration is available in the expanded step view.
          </p>
        </Grid>
      )

    default:
      return <p style={noParamsStyle}>Unknown step type.</p>
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const FILTER_OPERATORS: { value: FilterOperator; label: string }[] = [
  { value: 'EQ', label: 'Equals (EQ)' },
  { value: 'NEQ', label: 'Not Equals (NEQ)' },
  { value: 'GT', label: 'Greater Than (GT)' },
  { value: 'GTE', label: 'Greater or Equal (GTE)' },
  { value: 'LT', label: 'Less Than (LT)' },
  { value: 'LTE', label: 'Less or Equal (LTE)' },
  { value: 'CONTAINS', label: 'Contains' },
  { value: 'MATCHES', label: 'Regex Matches' },
  { value: 'IS_NULL', label: 'Is Null' },
  { value: 'IS_NOT_NULL', label: 'Is Not Null' },
]

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function Grid({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))', gap: '0.6rem 1rem', padding: '0.75rem 0' }}>
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
          resize: 'vertical', fontFamily: 'monospace', fontSize: 12,
          borderColor: err ? '#e53e3e' : '#cbd5e0',
        }}
      />
      {err && <p style={{ margin: '2px 0 0', fontSize: 11, color: '#e53e3e' }}>Invalid JSON</p>}
    </>
  )
}

function CheckboxInput({ id, label, checked, onChange }: { id: string; label: string; checked: boolean; onChange: (v: boolean) => void }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6, paddingTop: 4 }}>
      <input id={id} type="checkbox" checked={checked} onChange={e => onChange(e.target.checked)} />
      <label htmlFor={id} style={{ fontSize: 13, color: '#4a5568', userSelect: 'none', cursor: 'pointer' }}>{label}</label>
    </div>
  )
}

function SelectInput<T extends string>({ value, onChange, options }: {
  value: T
  onChange: (v: T) => void
  options: { value: T; label: string }[]
}) {
  return (
    <select value={value} onChange={e => onChange(e.target.value as T)} style={inputStyle}>
      {options.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
    </select>
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
