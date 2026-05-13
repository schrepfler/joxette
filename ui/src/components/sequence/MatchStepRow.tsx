import { useEffect, useRef, useState } from 'react'
import { draggable, dropTargetForElements } from '@atlaskit/pragmatic-drag-and-drop/element/adapter'
import { attachClosestEdge, extractClosestEdge, type Edge } from '@atlaskit/pragmatic-drag-and-drop-hitbox/closest-edge'
import type { MatchStep, Predicate } from '#/transforms/types'
import { PredicateBuilder } from '../transforms/PredicateBuilder'

interface Props {
  step: MatchStep
  stepIndex: number
  color: string
  onUpdate: (patch: Partial<MatchStep>) => void
  onRemove: () => void
  onMove: (fromIndex: number, toIndex: number) => void
  isFirst: boolean
  isLast: boolean
}

const STEP_DND_TYPE = 'joxette:sequence-step'

export function MatchStepRow({ step, stepIndex: index, color, onUpdate, onRemove, onMove, isLast }: Props) {
  const cardRef = useRef<HTMLDivElement>(null)
  const handleRef = useRef<HTMLButtonElement>(null)
  const [closestEdge, setClosestEdge] = useState<Edge | null>(null)
  const [isDragging, setIsDragging] = useState(false)
  const [expanded, setExpanded] = useState(false)
  const [hovered, setHovered] = useState(false)
  const [labelEditing, setLabelEditing] = useState(false)

  useEffect(() => {
    const el = cardRef.current
    const handle = handleRef.current
    if (!el || !handle) return

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
      onDrag: ({ self }) => setClosestEdge(extractClosestEdge(self.data)),
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

    return () => { cleanupDraggable(); cleanupDropTarget() }
  }, [index, onMove])

  const displayLabel = step.label || inferLabel(step.predicate)

  function inferLabel(pred: Predicate): string {
    if ('field' in pred && pred.field === '$.value.type' && pred.operator === 'EQ' && pred.value != null) {
      return String(pred.value)
    }
    return `Step ${index + 1}`
  }

  const hasWhere = isNonDefaultPredicate(step.predicate)

  return (
    <div style={{ position: 'relative', marginBottom: 2 }}>
      {closestEdge === 'top' && <DropIndicator />}

      <div ref={cardRef} style={{ opacity: isDragging ? 0.4 : 1 }}>
        {/* Main pill row */}
        <div
          style={{
            display: 'flex', alignItems: 'center', gap: 6,
            background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8,
            padding: '6px 8px',
          }}
          onMouseEnter={() => setHovered(true)}
          onMouseLeave={() => setHovered(false)}
        >
          {/* Drag handle */}
          <button ref={handleRef} style={dragHandleStyle} title="Drag to reorder" aria-label="Drag handle">
            ⠿
          </button>

          {/* Colored badge icon */}
          <div style={{
            width: 28, height: 28, borderRadius: 6, flexShrink: 0,
            background: color, display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}>
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
              <rect x="1" y="1" width="4" height="4" rx="1" fill="rgba(255,255,255,0.85)" />
              <rect x="6" y="1" width="7" height="1.5" rx="0.75" fill="rgba(255,255,255,0.7)" />
              <rect x="1" y="6" width="4" height="4" rx="1" fill="rgba(255,255,255,0.85)" />
              <rect x="6" y="6" width="7" height="1.5" rx="0.75" fill="rgba(255,255,255,0.7)" />
              <rect x="6" y="9" width="5" height="1.5" rx="0.75" fill="rgba(255,255,255,0.5)" />
            </svg>
          </div>

          {/* Label input */}
          {labelEditing ? (
            <input
              autoFocus
              value={step.label ?? inferLabel(step.predicate)}
              onChange={e => onUpdate({ label: e.target.value || undefined })}
              onBlur={() => setLabelEditing(false)}
              onKeyDown={e => { if (e.key === 'Enter' || e.key === 'Escape') setLabelEditing(false) }}
              style={labelInputStyle}
              placeholder="Step name"
            />
          ) : (
            <button
              style={labelBtnStyle}
              onClick={() => setLabelEditing(true)}
              title="Click to rename"
            >
              {displayLabel}
            </button>
          )}

          {/* Badges */}
          {!step.required && (
            <span style={{ ...pillBadgeStyle, background: '#fef3c7', color: '#92400e' }}>optional</span>
          )}
          {step.repeated && (
            <span style={{ ...pillBadgeStyle, background: '#dbeafe', color: '#1e40af' }}>+</span>
          )}

          <div style={{ flex: 1 }} />

          {/* Expand chevron */}
          <button
            style={chevronBtnStyle}
            onClick={() => setExpanded(v => !v)}
            title={expanded ? 'Collapse' : 'Expand conditions'}
          >
            {expanded ? '▲' : '▾'}
          </button>

          {/* Delete */}
          {hovered && (
            <button style={deleteBtnStyle} onClick={onRemove} title="Remove step" aria-label="Remove step">
              ✕
            </button>
          )}
        </div>

        {/* Expanded predicate builder */}
        {expanded && (
          <div style={{
            marginLeft: 42, marginTop: 4, marginBottom: 4,
            padding: '0.5rem', background: '#f7fafc',
            border: '1px solid #e2e8f0', borderRadius: 6,
          }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: '#718096', textTransform: 'uppercase', letterSpacing: '0.04em', marginBottom: 6 }}>
              Conditions
            </div>
            <PredicateBuilder
              value={step.predicate}
              onChange={p => onUpdate({ predicate: p ?? { field: '', operator: 'EQ' } })}
              depth={0}
            />
          </div>
        )}

        {/* Shortcut chip buttons (dormant) */}
        <div style={{ display: 'flex', gap: 5, marginLeft: 42, marginTop: 5, flexWrap: 'wrap' }}>
          {!hasWhere && !expanded && (
            <ShortcutChip
              label="+ where"
              onClick={() => setExpanded(true)}
            />
          )}
          {step.required && (
            <ShortcutChip
              label="+ is required"
              onClick={() => onUpdate({ required: true })}
              hidden
            />
          )}
          {!step.required && (
            <ActiveChip label="optional" onRemove={() => onUpdate({ required: true })} />
          )}
          {step.repeated ? (
            <ActiveChip label="repeated" onRemove={() => onUpdate({ repeated: false })} />
          ) : (
            <ShortcutChip label="+ repeated" onClick={() => onUpdate({ repeated: true })} />
          )}
        </div>

        {/* Gap connector (shown between steps, not after last) */}
        {!isLast && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginLeft: 42, marginTop: 6, marginBottom: 2 }}>
            <button
              style={{
                ...gapChipStyle,
                background: step.gap === 'any' ? '#ebf8ff' : '#f0fff4',
                color: step.gap === 'any' ? '#2b6cb0' : '#276749',
                borderColor: step.gap === 'any' ? '#bee3f8' : '#c6f6d5',
              }}
              onClick={() => onUpdate({ gap: step.gap === 'any' ? 'immediate' : 'any' })}
              title="Toggle gap constraint"
            >
              {step.gap === 'any' ? '↕ any events' : '→ immediately'}
            </button>
          </div>
        )}
      </div>

      {closestEdge === 'bottom' && <DropIndicator />}
    </div>
  )
}

function ShortcutChip({ label, onClick, hidden }: { label: string; onClick: () => void; hidden?: boolean }) {
  if (hidden) return null
  return (
    <button style={shortcutChipStyle} onClick={onClick}>
      {label}
    </button>
  )
}

function ActiveChip({ label, onRemove }: { label: string; onRemove: () => void }) {
  return (
    <span style={activeChipStyle}>
      {label}
      <button style={chipRemoveStyle} onClick={onRemove} title={`Remove ${label}`}>✕</button>
    </span>
  )
}

function DropIndicator() {
  return <div style={{ height: 3, background: '#3182ce', borderRadius: 2, margin: '1px 0' }} />
}

function isNonDefaultPredicate(pred: Predicate): boolean {
  if (!('field' in pred)) return true
  return pred.field !== '' || pred.operator !== 'EQ'
}

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const dragHandleStyle: React.CSSProperties = {
  background: 'none', border: 'none', cursor: 'grab', fontSize: 14,
  color: '#a0aec0', padding: '0 2px', lineHeight: 1, flexShrink: 0,
}
const labelInputStyle: React.CSSProperties = {
  flex: 1, padding: '2px 6px', border: '1px solid #90cdf4', borderRadius: 4,
  fontSize: 13, fontWeight: 600, outline: 'none', background: '#ebf8ff',
  minWidth: 0,
}
const labelBtnStyle: React.CSSProperties = {
  flex: 1, background: 'none', border: 'none', cursor: 'text', textAlign: 'left',
  fontSize: 13, fontWeight: 600, color: '#2d3748', padding: 0, minWidth: 0,
  overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
}
const pillBadgeStyle: React.CSSProperties = {
  fontSize: 10, fontWeight: 700, padding: '2px 6px', borderRadius: 10,
  textTransform: 'uppercase', letterSpacing: '0.04em', flexShrink: 0,
}
const chevronBtnStyle: React.CSSProperties = {
  background: 'none', border: 'none', cursor: 'pointer', fontSize: 11,
  color: '#a0aec0', padding: '2px 4px', lineHeight: 1, flexShrink: 0,
}
const deleteBtnStyle: React.CSSProperties = {
  background: 'none', border: 'none', cursor: 'pointer', fontSize: 11,
  color: '#fc8181', padding: '2px 4px', lineHeight: 1, flexShrink: 0,
}
const shortcutChipStyle: React.CSSProperties = {
  padding: '2px 8px', fontSize: 11, background: '#fff', color: '#718096',
  border: '1px dashed #cbd5e0', borderRadius: 12, cursor: 'pointer',
  transition: 'background 0.15s',
}
const activeChipStyle: React.CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 4,
  padding: '2px 8px', fontSize: 11, background: '#ebf8ff', color: '#2b6cb0',
  border: '1px solid #bee3f8', borderRadius: 12,
}
const chipRemoveStyle: React.CSSProperties = {
  background: 'none', border: 'none', cursor: 'pointer', fontSize: 10,
  color: '#4299e1', padding: 0, lineHeight: 1,
}
const gapChipStyle: React.CSSProperties = {
  padding: '3px 10px', fontSize: 11, fontWeight: 600,
  border: '1px solid', borderRadius: 12, cursor: 'pointer',
  transition: 'background 0.15s',
}
