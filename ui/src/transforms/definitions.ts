import type { TransformStep } from './types'

export interface StepDef {
  type: TransformStep['type']
  label: string
  category: string
  description: string
  /** Factory for the default params (minus `type` and `_id`). */
  defaults: () => Omit<TransformStep, 'type'>
}

export const STEP_CATEGORIES = [
  'Time',
  'Field Values',
  'Structure',
  'Keys',
  'Headers',
  'Routing',
  'Logic',
] as const

export type StepCategory = (typeof STEP_CATEGORIES)[number]

export const STEP_DEFINITIONS: StepDef[] = [
  // ---- Time ----
  {
    type: 'wall_time',
    label: 'Wall Time',
    category: 'Time',
    description: 'Replace timestamp with current wall-clock time',
    defaults: () => ({ target: '$.timestamp' }),
  },
  {
    type: 'time_shift',
    label: 'Time Shift',
    category: 'Time',
    description: 'Shift timestamp by a fixed number of milliseconds',
    defaults: () => ({ target: 'ALL_TIMESTAMPS', shiftMs: -3600000 }),
  },
  {
    type: 'time_compress',
    label: 'Time Compress',
    category: 'Time',
    description: 'Compress or expand time gaps between messages',
    defaults: () => ({ target: '$.timestamp', factor: 2.0 }),
  },
  {
    type: 'time_freeze',
    label: 'Time Freeze',
    category: 'Time',
    description: 'Fix all timestamps to a single instant',
    defaults: () => ({ target: 'ALL_TIMESTAMPS', frozenAt: 'NOW' }),
  },
  // ---- Field Values ----
  {
    type: 'set_constant',
    label: 'Set Constant',
    category: 'Field Values',
    description: 'Set a field to a constant value',
    defaults: () => ({ target: '$.value.env', value: 'staging' }),
  },
  {
    type: 'copy_field',
    label: 'Copy Field',
    category: 'Field Values',
    description: 'Copy value from one field path to another',
    defaults: () => ({ from: '$.value.order_id', to: '$.key' }),
  },
  {
    type: 'template',
    label: 'Template',
    category: 'Field Values',
    description: 'Write a template string with ${path} placeholders to a field',
    defaults: () => ({ target: '$.value.routing_key', template: '${value.order_id}' }),
  },
  {
    type: 'redact',
    label: 'Redact',
    category: 'Field Values',
    description: 'Replace field value with REDACTED',
    defaults: () => ({ target: '$.value.email' }),
  },
  {
    type: 'mask_hash',
    label: 'Mask / Hash',
    category: 'Field Values',
    description: 'Replace field with SHA-256 hash (pseudonymise)',
    defaults: () => ({ target: '$.value.email', prefix: 'anon-', salt: '' }),
  },
  {
    type: 'coalesce',
    label: 'Coalesce',
    category: 'Field Values',
    description: 'Write first non-null value from a list of source paths',
    defaults: () => ({ sources: ['$.value.order_id', '$.value.legacy_id'], target: '$.value.id', fallback: 'unknown' }),
  },
  // ---- Structure ----
  {
    type: 'rename_field',
    label: 'Rename Field',
    category: 'Structure',
    description: 'Rename a field key within its parent object',
    defaults: () => ({ source: '$.value.orderId', new_name: 'order_id' }),
  },
  {
    type: 'delete_field',
    label: 'Delete Field',
    category: 'Structure',
    description: 'Remove a field from the message value',
    defaults: () => ({ target: '$.value.internal_debug' }),
  },
  {
    type: 'flatten_field',
    label: 'Flatten Field',
    category: 'Structure',
    description: 'Hoist nested object fields up to the parent level',
    defaults: () => ({ source: '$.value.metadata', prefix: 'meta_' }),
  },
  {
    type: 'add_computed_field',
    label: 'Add Computed Field',
    category: 'Structure',
    description: 'Add a new field computed via a JSONPath expression',
    defaults: () => ({ target: '$.value.computed', expression: '$.value.price' }),
  },
  {
    type: 'merge_patch',
    label: 'Merge Patch',
    category: 'Structure',
    description: 'Apply RFC 7396 JSON Merge Patch to a field',
    defaults: () => ({ target: '$.value', patch: { status: 'REPLAYED' } }),
  },
  // ---- Keys ----
  {
    type: 'remap_key',
    label: 'Remap Key',
    category: 'Keys',
    description: 'Replace message key with value extracted from message body',
    defaults: () => ({ source: '$.value.order_id' }),
  },
  {
    type: 'null_key',
    label: 'Null Key',
    category: 'Keys',
    description: 'Set message key to null',
    defaults: () => ({}),
  },
  {
    type: 'key_from_value',
    label: 'Key From Value',
    category: 'Keys',
    description: 'Set message key from a template expression',
    defaults: () => ({ expression: '${value.order_id}' }),
  },
  // ---- Headers ----
  {
    type: 'add_header',
    label: 'Add Header',
    category: 'Headers',
    description: 'Inject a Kafka header (supports ${path} templates)',
    defaults: () => ({ key: 'x-env', value: 'staging', if_absent: false }),
  },
  {
    type: 'remove_header',
    label: 'Remove Header',
    category: 'Headers',
    description: 'Remove all headers with a given key',
    defaults: () => ({ key: 'x-internal' }),
  },
  {
    type: 'copy_to_header',
    label: 'Copy to Header',
    category: 'Headers',
    description: 'Copy a value field into a Kafka header',
    defaults: () => ({ source: '$.value.correlation_id', headerKey: 'x-correlation-id' }),
  },
  // ---- Routing ----
  {
    type: 'redirect_topic',
    label: 'Redirect Topic',
    category: 'Routing',
    description: 'Override the target topic for this message',
    defaults: () => ({ topic: 'orders-staging' }),
  },
  {
    type: 'fan_out',
    label: 'Fan Out',
    category: 'Routing',
    description: 'Duplicate message to multiple target topics',
    defaults: () => ({ topics: ['orders-a', 'orders-b'] }),
  },
  {
    type: 'filter_drop',
    label: 'Filter Drop',
    category: 'Routing',
    description: 'Drop messages matching a field predicate',
    defaults: () => ({ field: '$.value.status', operator: 'EQ' as const, value: 'cancelled' }),
  },
  // ---- Logic ----
  {
    type: 'conditional',
    label: 'Conditional',
    category: 'Logic',
    description: 'Apply different steps based on a condition',
    defaults: () => ({ condition: '$.amount > 1000', thenSteps: [], elseSteps: [] }),
  },
]

export const STEP_DEF_MAP = new Map(STEP_DEFINITIONS.map(d => [d.type, d]))

export const STEPS_BY_CATEGORY = STEP_CATEGORIES.map(cat => ({
  category: cat,
  steps: STEP_DEFINITIONS.filter(d => d.category === cat),
}))
