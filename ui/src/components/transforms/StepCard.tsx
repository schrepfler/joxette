import { useEffect, useRef, useState } from 'react'
import { draggable, dropTargetForElements } from '@atlaskit/pragmatic-drag-and-drop/element/adapter'
import { attachClosestEdge, extractClosestEdge, type Edge } from '@atlaskit/pragmatic-drag-and-drop-hitbox/closest-edge'
import { STEP_DEF_MAP } from '#/transforms/definitions'
import type { PipelineStep, Predicate, FragmentDefinition } from '#/transforms/types'
import { isLeafPredicate } from '#/transforms/types'
import { StepConfigForm } from './StepConfigForm'

interface Props {
  step: PipelineStep
  index: number
  total: number
  expanded: boolean
  onToggleExpand: () => void
  onChange: (updated: PipelineStep) => void
  onDelete: () => void
  onMove: (fromIndex: number, toIndex: number) => void
  disabled?: boolean
  fragments?: FragmentDefinition[]
}

const STEP_DND_TYPE = 'joxette:transform-step'

export function StepCard({ step, index, total, expanded, onToggleExpand, onChange, onDelete, onMove, disabled, fragments }: Props) {
  const cardRef = useRef<HTMLDivElement>(null)
  const handleRef = useRef<HTMLButtonElement>(null)
  const [closestEdge, setClosestEdge] = useState<Edge | null>(null)
  const [isDragging, setIsDragging] = useState(false)

  const def = STEP_DEF_MAP.get(step.type)
  const label = def?.label ?? step.type
  const category = def?.category ?? ''

  useEffect(() => {
    const el = cardRef.current
    const handle = handleRef.current
    if (!el || !handle || disabled) return

    const cleanupDraggable = draggable({
      element: el,
      dragHandle: handle,
      getInitialData: () => ({ [STEP_DND_TYPE]: true, index }),
      onDragStart: () => setIsDragging(true),
      onDrop: () => setIsDragging(false),
    })

    const cleanupDropTarget = dropTargetForElements({
      element: el,
      getData: ({ input }) =>
        attachClosestEdge(
          { [STEP_DND_TYPE]: true, index } as Record<string, unknown>,
          { element: el, input, allowedEdges: ['top', 'bottom'] },
        ),
      onDrag: ({ self }) => {
        setClosestEdge(extractClosestEdge(self.data))
      },
      onDragLeave: () => setClosestEdge(null),
      onDrop: ({ source, self }) => {
        setClosestEdge(null)
        const srcData = source.data
        if (!srcData[STEP_DND_TYPE]) return
        const fromIdx = srcData.index as number
        const edge = extractClosestEdge(self.data)
        const toIdx = edge === 'bottom' ? index + 1 : index
        if (fromIdx === toIdx || fromIdx + 1 === toIdx) return
        onMove(fromIdx, toIdx > fromIdx ? toIdx - 1 : toIdx)
      },
    })

    return () => {
      cleanupDraggable()
      cleanupDropTarget()
    }
  }, [index, total, onMove, disabled])

  const badgeColor = CATEGORY_COLORS[category] ?? '#e2e8f0'
  const badgeText = CATEGORY_COLORS[category] ? '#fff' : '#4a5568'

  return (
    <div style={{ position: 'relative' }}>
      {/* Drop indicator — top edge */}
      {closestEdge === 'top' && <DropIndicator />}

      <div
        ref={cardRef}
        style={{
          ...cardStyle,
          opacity: isDragging ? 0.4 : 1,
          background: expanded ? '#f0f7ff' : '#fff',
          borderColor: expanded ? '#3182ce' : '#e2e8f0',
        }}
      >
        {/* Card header row */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          {/* Drag handle */}
          <button
            ref={handleRef}
            style={dragHandleStyle}
            title="Drag to reorder"
            aria-label="Drag handle"
            disabled={disabled}
          >
            ⠿
          </button>

          {/* Category badge */}
          <span style={{ ...badgeStyle, background: badgeColor, color: badgeText }}>
            {category}
          </span>

          {/* Step label + params summary */}
          <button style={stepLabelBtnStyle} onClick={onToggleExpand}>
            <span style={{ fontWeight: 600, fontSize: 13 }}>{label}</span>
            <span style={{ fontSize: 12, color: '#718096', marginLeft: 6 }}>
              {buildParamSummary(step)}
            </span>
          </button>

          {/* Delete button */}
          <button
            style={deleteBtnStyle}
            onClick={onDelete}
            title="Remove step"
            aria-label="Remove step"
            disabled={disabled}
          >
            ✕
          </button>
        </div>

        {/* Expanded config form */}
        {expanded && (
          <div style={{ borderTop: '1px solid #e2e8f0', marginTop: '0.5rem' }}>
            <StepConfigForm step={step} onChange={onChange} fragments={fragments} />
          </div>
        )}
      </div>

      {/* Drop indicator — bottom edge */}
      {closestEdge === 'bottom' && <DropIndicator />}
    </div>
  )
}

function DropIndicator() {
  return <div style={{ height: 3, background: '#3182ce', borderRadius: 2, margin: '1px 0' }} />
}

function buildParamSummary(step: PipelineStep): string {
  const s = step as unknown as Record<string, unknown>
  switch (step.type) {
    case 'wall_time': return `→ ${s.target}`
    case 'time_shift': return `${s.shiftMs}ms → ${s.target}`
    case 'time_compress': return `×${s.factor} → ${s.target}`
    case 'time_freeze': return `${s.frozenAt} → ${s.target}`
    case 'set_constant': return `${s.target} = ${JSON.stringify(s.value)}`
    case 'copy_field': return `${s.from} → ${s.to}`
    case 'template': return `${s.target}`
    case 'redact': return `${s.target}`
    case 'mask_hash': return `${s.target}`
    case 'coalesce': return `→ ${s.target}`
    case 'rename_field': return `${s.source} → ${s.new_name}`
    case 'delete_field': return `${s.target}`
    case 'flatten_field': return `${s.source}${s.prefix ? ` prefix:${s.prefix}` : ''}`
    case 'add_computed_field': return `${s.target}`
    case 'merge_patch': return `${s.target}`
    case 'remap_key': return `${s.source}`
    case 'null_key': return ''
    case 'key_from_value': return `${s.expression}`
    case 'add_header': return `${s.key}: ${s.value}`
    case 'remove_header': return `${s.key}`
    case 'copy_to_header': return `${s.source} → ${s.headerKey}`
    case 'redirect_topic': return `→ ${s.topic}`
    case 'fan_out': return `→ ${(s.topics as string[]).join(', ')}`
    case 'filter_drop': return predicateSummary(s.predicate as Predicate)
    case 'conditional': return `if: ${predicateSummary(s.condition as Predicate)}`
    default: return ''
  }
}

function predicateSummary(p: Predicate | undefined | null): string {
  if (!p) return ''
  if (isLeafPredicate(p)) return `${p.field} ${p.operator}${p.value != null ? ` ${p.value}` : ''}`
  if (p.match === 'not') return `NOT (${predicateSummary(p.predicate)})`
  return `[${p.match.toUpperCase()} × ${p.predicates.length}]`
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

const cardStyle: React.CSSProperties = {
  border: '1px solid #e2e8f0', borderRadius: 6, padding: '0.5rem 0.75rem',
  background: '#fff', marginBottom: 4,
}
const dragHandleStyle: React.CSSProperties = {
  background: 'none', border: 'none', cursor: 'grab', fontSize: 14,
  color: '#a0aec0', padding: '0 4px', lineHeight: 1, flexShrink: 0,
}
const badgeStyle: React.CSSProperties = {
  fontSize: 10, fontWeight: 700, padding: '2px 6px', borderRadius: 10,
  flexShrink: 0, textTransform: 'uppercase', letterSpacing: '0.04em',
}
const stepLabelBtnStyle: React.CSSProperties = {
  background: 'none', border: 'none', cursor: 'pointer', textAlign: 'left',
  flex: 1, padding: 0, display: 'flex', alignItems: 'baseline', gap: 4,
}
const deleteBtnStyle: React.CSSProperties = {
  background: 'none', border: 'none', cursor: 'pointer', fontSize: 12,
  color: '#a0aec0', padding: '2px 4px', flexShrink: 0, lineHeight: 1,
}
