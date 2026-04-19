import { useState } from 'react'
import type { Predicate } from '#/transforms/types'
import { PredicateBuilder } from '../transforms/PredicateBuilder'

interface DateRange {
  from?: string
  to?: string
}

interface CleanDataState {
  includeFilter?: Predicate
  excludeFilter?: Predicate
  dateRange?: DateRange
  splitPredicate?: Predicate
}

interface Props {
  value: CleanDataState
  onChange: (v: CleanDataState) => void
}

export function CleanDataSection({ value, onChange }: Props) {
  const [collapsed, setCollapsed] = useState(false)

  const hasAny = value.includeFilter || value.excludeFilter || value.dateRange || value.splitPredicate
  const activeTags = buildActiveTags(value, onChange)

  return (
    <div style={sectionStyle}>
      {/* Section header */}
      <button style={sectionHeaderStyle} onClick={() => setCollapsed(v => !v)}>
        <span style={sectionLabelStyle}>▸ Clean data</span>
        {hasAny && <span style={activeCountBadge}>{activeTags.length}</span>}
        <span style={{ fontSize: 10, color: '#a0aec0', marginLeft: 'auto' }}>
          {collapsed ? '▾' : '▴'}
        </span>
      </button>

      {!collapsed && (
        <div style={{ padding: '8px 10px', display: 'flex', flexDirection: 'column', gap: 8 }}>
          {/* Active filter tags */}
          {activeTags.length > 0 && (
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
              {activeTags}
            </div>
          )}

          {/* Include only events */}
          {value.includeFilter && (
            <FilterBlock
              label="Include only events where"
              onRemove={() => onChange({ ...value, includeFilter: undefined })}
            >
              <PredicateBuilder
                value={value.includeFilter}
                onChange={p => onChange({ ...value, includeFilter: p ?? undefined })}
                depth={0}
              />
            </FilterBlock>
          )}

          {/* Exclude events */}
          {value.excludeFilter && (
            <FilterBlock
              label="Exclude events where"
              onRemove={() => onChange({ ...value, excludeFilter: undefined })}
            >
              <PredicateBuilder
                value={value.excludeFilter}
                onChange={p => onChange({ ...value, excludeFilter: p ?? undefined })}
                depth={0}
              />
            </FilterBlock>
          )}

          {/* Date range */}
          {value.dateRange && (
            <FilterBlock
              label="Start date range"
              onRemove={() => onChange({ ...value, dateRange: undefined })}
            >
              <div style={{ display: 'flex', gap: 6, alignItems: 'center', flexWrap: 'wrap' }}>
                <input
                  type="datetime-local"
                  value={value.dateRange.from ?? ''}
                  onChange={e => onChange({ ...value, dateRange: { ...value.dateRange, from: e.target.value || undefined } })}
                  style={dateInputStyle}
                  title="From"
                />
                <span style={{ fontSize: 11, color: '#718096' }}>→</span>
                <input
                  type="datetime-local"
                  value={value.dateRange.to ?? ''}
                  onChange={e => onChange({ ...value, dateRange: { ...value.dateRange, to: e.target.value || undefined } })}
                  style={dateInputStyle}
                  title="To"
                />
              </div>
            </FilterBlock>
          )}

          {/* Split sequences */}
          {value.splitPredicate && (
            <FilterBlock
              label="Split sequences when"
              onRemove={() => onChange({ ...value, splitPredicate: undefined })}
            >
              <PredicateBuilder
                value={value.splitPredicate}
                onChange={p => onChange({ ...value, splitPredicate: p ?? undefined })}
                depth={0}
              />
            </FilterBlock>
          )}

          {/* Shortcut chip buttons for inactive filters */}
          <div style={{ display: 'flex', gap: 5, flexWrap: 'wrap' }}>
            {!value.includeFilter && (
              <DormantChip
                label="+ include only events"
                onClick={() => onChange({ ...value, includeFilter: { field: '', operator: 'EQ' } })}
              />
            )}
            {!value.excludeFilter && (
              <DormantChip
                label="+ exclude events"
                onClick={() => onChange({ ...value, excludeFilter: { field: '', operator: 'EQ' } })}
              />
            )}
            {!value.dateRange && (
              <DormantChip
                label="+ filter by start date"
                onClick={() => onChange({ ...value, dateRange: {} })}
              />
            )}
            {!value.splitPredicate && (
              <DormantChip
                label="+ split sequences"
                onClick={() => onChange({ ...value, splitPredicate: { field: '', operator: 'EQ' } })}
              />
            )}
          </div>
        </div>
      )}
    </div>
  )
}

function FilterBlock({ label, onRemove, children }: { label: string; onRemove: () => void; children: React.ReactNode }) {
  return (
    <div style={filterBlockStyle}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 5 }}>
        <span style={{ fontSize: 11, fontWeight: 600, color: '#4a5568' }}>{label}</span>
        <button style={filterRemoveStyle} onClick={onRemove} title="Remove filter">✕</button>
      </div>
      {children}
    </div>
  )
}

function DormantChip({ label, onClick }: { label: string; onClick: () => void }) {
  return (
    <button style={dormantChipStyle} onClick={onClick}>
      {label}
    </button>
  )
}

function buildActiveTags(value: CleanDataState, onChange: (v: CleanDataState) => void): React.ReactElement[] {
  const tags: React.ReactElement[] = []
  if (value.includeFilter) {
    tags.push(
      <ActiveTag key="include" label="include filter" onRemove={() => onChange({ ...value, includeFilter: undefined })} />,
    )
  }
  if (value.excludeFilter) {
    tags.push(
      <ActiveTag key="exclude" label="exclude filter" onRemove={() => onChange({ ...value, excludeFilter: undefined })} />,
    )
  }
  if (value.dateRange) {
    const parts = [value.dateRange.from, value.dateRange.to].filter(Boolean).join(' → ')
    tags.push(
      <ActiveTag key="date" label={parts ? `dates: ${parts}` : 'date range'} onRemove={() => onChange({ ...value, dateRange: undefined })} />,
    )
  }
  if (value.splitPredicate) {
    tags.push(
      <ActiveTag key="split" label="split boundary" onRemove={() => onChange({ ...value, splitPredicate: undefined })} />,
    )
  }
  return tags
}

function ActiveTag({ label, onRemove }: { label: string; onRemove: () => void }) {
  return (
    <span style={activeTagStyle}>
      {label}
      <button style={tagRemoveStyle} onClick={onRemove}>✕</button>
    </span>
  )
}

export type { CleanDataState }

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const sectionStyle: React.CSSProperties = {
  border: '1px solid #e2e8f0', borderRadius: 8, marginBottom: 8, background: '#fafafa',
}
const sectionHeaderStyle: React.CSSProperties = {
  display: 'flex', alignItems: 'center', gap: 6, width: '100%',
  background: 'none', border: 'none', cursor: 'pointer', padding: '8px 10px',
  textAlign: 'left',
}
const sectionLabelStyle: React.CSSProperties = {
  fontSize: 12, fontWeight: 700, color: '#4a5568', textTransform: 'uppercase',
  letterSpacing: '0.04em',
}
const activeCountBadge: React.CSSProperties = {
  background: '#3182ce', color: '#fff', borderRadius: 10, fontSize: 10,
  fontWeight: 700, padding: '1px 5px', minWidth: 16, textAlign: 'center',
}
const filterBlockStyle: React.CSSProperties = {
  background: '#fff', border: '1px solid #e2e8f0', borderRadius: 6, padding: '8px 10px',
}
const filterRemoveStyle: React.CSSProperties = {
  background: 'none', border: 'none', cursor: 'pointer', fontSize: 11,
  color: '#a0aec0', padding: 0, lineHeight: 1,
}
const dormantChipStyle: React.CSSProperties = {
  padding: '3px 9px', fontSize: 11, background: '#fff', color: '#718096',
  border: '1px dashed #cbd5e0', borderRadius: 12, cursor: 'pointer',
}
const activeTagStyle: React.CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 4,
  padding: '2px 8px', fontSize: 11, background: '#ebf8ff', color: '#2b6cb0',
  border: '1px solid #bee3f8', borderRadius: 12,
}
const tagRemoveStyle: React.CSSProperties = {
  background: 'none', border: 'none', cursor: 'pointer', fontSize: 10,
  color: '#4299e1', padding: 0, lineHeight: 1,
}
const dateInputStyle: React.CSSProperties = {
  padding: '3px 6px', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 11,
}
