// ---------------------------------------------------------------------------
// Transform step TypeScript types matching the backend TransformStep sealed
// interface and all @JsonSubTypes entries in TransformStep.java.
//
// IMPORTANT: field names must match Jackson serialisation exactly.
// Records use camelCase Java names unless @JsonProperty overrides them.
// ---------------------------------------------------------------------------

export type FilterOperator =
  | 'EQ' | 'NEQ' | 'GT' | 'GTE' | 'LT' | 'LTE'
  | 'CONTAINS' | 'MATCHES' | 'IS_NULL' | 'IS_NOT_NULL'

// A condition string for the conditional step (simple free-text expression)
export type ConditionExpr = string

// ---- Time ----

export interface WallTimeStep {
  type: 'wall_time'
  target: string   // JSONPath, default "$.timestamp"
}

export interface TimeShiftStep {
  type: 'time_shift'
  target: string   // JSONPath or "ALL_TIMESTAMPS"
  shiftMs: number  // ms; positive = forward, negative = backward
}

export interface TimeCompressStep {
  type: 'time_compress'
  target: string   // JSONPath or "ALL_TIMESTAMPS"
  factor: number   // >1 compresses, <1 expands
}

export interface TimeFreezeStep {
  type: 'time_freeze'
  target: string   // JSONPath or "ALL_TIMESTAMPS"
  frozenAt: string // ISO-8601 instant or "NOW"
}

// ---- Field value ----

export interface SetConstantStep {
  type: 'set_constant'
  target: string
  value: unknown   // any JSON value
}

export interface CopyFieldStep {
  type: 'copy_field'
  from: string
  to: string
}

export interface TemplateStep {
  type: 'template'
  target: string
  template: string // ${path} placeholders resolved against message
}

export interface RedactStep {
  type: 'redact'
  target: string
}

export interface MaskHashStep {
  type: 'mask_hash'
  target: string
  prefix?: string
  salt?: string
}

export interface CoalesceStep {
  type: 'coalesce'
  sources: string[]  // first non-null wins
  target: string
  fallback?: unknown  // any JSON value; written if all sources are null
}

// ---- Structure ----

export interface RenameFieldStep {
  type: 'rename_field'
  source: string        // JSONPath to existing key
  new_name: string      // bare leaf name (no path), @JsonProperty("new_name")
}

export interface DeleteFieldStep {
  type: 'delete_field'
  target: string
}

export interface FlattenFieldStep {
  type: 'flatten_field'
  source: string        // JSONPath to nested object
  prefix?: string       // prepended to each hoisted key
}

export interface AddComputedFieldStep {
  type: 'add_computed_field'
  target: string
  expression: string    // JSONPath or template expression
}

export interface MergePatchStep {
  type: 'merge_patch'
  target: string
  patch: Record<string, unknown>  // RFC 7396 merge patch object
}

// ---- Keys ----

export interface RemapKeyStep {
  type: 'remap_key'
  source: string  // JSONPath into message value
}

export interface NullKeyStep {
  type: 'null_key'
}

export interface KeyFromValueStep {
  type: 'key_from_value'
  expression: string
}

// ---- Headers ----

export interface AddHeaderStep {
  type: 'add_header'
  key: string
  value: string          // may contain ${path} placeholders
  if_absent?: boolean    // @JsonProperty("if_absent")
}

export interface RemoveHeaderStep {
  type: 'remove_header'
  key: string
}

export interface CopyToHeaderStep {
  type: 'copy_to_header'
  source: string       // JSONPath
  headerKey: string    // camelCase on the wire
}

// ---- Routing ----

export interface RedirectTopicStep {
  type: 'redirect_topic'
  topic: string
}

export interface FanOutStep {
  type: 'fan_out'
  topics: string[]
}

export interface FilterDropStep {
  type: 'filter_drop'
  field: string
  operator: FilterOperator
  value?: string | number
}

// ---- Logic ----

export interface ConditionalStep {
  type: 'conditional'
  condition: ConditionExpr  // free-form expression string
  thenSteps: TransformStep[]  // camelCase on the wire
  elseSteps: TransformStep[]  // camelCase on the wire
}

// ---------------------------------------------------------------------------
// Union type
// ---------------------------------------------------------------------------

export type TransformStep =
  | WallTimeStep
  | TimeShiftStep
  | TimeCompressStep
  | TimeFreezeStep
  | SetConstantStep
  | CopyFieldStep
  | TemplateStep
  | RedactStep
  | MaskHashStep
  | CoalesceStep
  | RenameFieldStep
  | DeleteFieldStep
  | FlattenFieldStep
  | AddComputedFieldStep
  | MergePatchStep
  | RemapKeyStep
  | NullKeyStep
  | KeyFromValueStep
  | AddHeaderStep
  | RemoveHeaderStep
  | CopyToHeaderStep
  | RedirectTopicStep
  | FanOutStep
  | FilterDropStep
  | ConditionalStep

// ---------------------------------------------------------------------------
// Pipeline — ordered list of steps (wraps TransformStep[] for the builder UI)
// When serialising to the POST body, send the steps array directly.
// ---------------------------------------------------------------------------

export interface TransformPipeline {
  /** Steps in execution order. Each step has a UI-only `_id` for React keys. */
  steps: PipelineStep[]
}

/** A TransformStep with a client-side stable identity key (not sent to server). */
export type PipelineStep = TransformStep & { _id: string }

export const emptyPipeline = (): TransformPipeline => ({ steps: [] })

/** Strip the `_id` field before sending to the server. */
export function serializeSteps(steps: PipelineStep[]): TransformStep[] {
  return steps.map(({ _id: _, ...rest }) => rest as TransformStep)
}
