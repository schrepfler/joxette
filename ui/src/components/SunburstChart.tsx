import { useEffect, useRef, useState } from 'react'
import * as d3Hierarchy from 'd3-hierarchy'
import * as d3Interpolate from 'd3-interpolate'
import * as d3Shape from 'd3-shape'

// ── Types ──────────────────────────────────────────────────────────────────────

export interface SunburstNode {
  name: string
  nodeId: number
  nodeCount: number
  seqIds?: string[]
  children?: SunburstNode[]
}

export interface SunburstData {
  root: SunburstNode
  eventNames: string[]
  totalSequences: number
}

interface ZoomCoords { x0: number; x1: number; y0: number }

interface Props {
  data: SunburstData
  /** Outer diameter of the chart in px. Default 560. */
  diameter?: number
  /** Max rings to display (depth). Default 7. */
  maxSteps?: number
  /** Arcs smaller than this angle (degrees) are hidden. Default 0.5. */
  minAngleDeg?: number
  onNodeClick?: (node: SunburstNode, path: SunburstNode[]) => void
  /** Called on right-click: receives the arc node and all seqIds in its subtree. */
  onArcRightClick?: (node: SunburstNode, seqIds: string[]) => void
}

/** Recursively collects all seqIds from a node and all its descendants. */
export function collectSubtreeSeqIds(node: SunburstNode): string[] {
  const ids: string[] = []
  function walk(n: SunburstNode) {
    if (n.seqIds) ids.push(...n.seqIds)
    if (n.children) n.children.forEach(walk)
  }
  walk(node)
  return [...new Set(ids)]
}

// ── Colour helpers ─────────────────────────────────────────────────────────────

function djb2(s: string): number {
  let h = 5381
  for (let i = 0; i < s.length; i++) h = (h * 33) ^ s.charCodeAt(i)
  return Math.abs(h)
}

function eventColour(name: string): string {
  const hue = (djb2(name) * 137.508) % 360
  return `hsl(${hue.toFixed(0)}, 55%, 68%)`
}

// ── Breadcrumb ─────────────────────────────────────────────────────────────────

const BW = 140  // breadcrumb box width
const BH = 28   // breadcrumb box height
const TIP = 9   // arrow tip height

function breadcrumbPoints(i: number): string {
  const pts: string[] = []
  pts.push('0,0')
  pts.push(`0,${BH}`)
  pts.push(`${BW / 2},${BH + TIP}`)
  pts.push(`${BW},${BH}`)
  pts.push(`${BW},0`)
  if (i > 0) pts.push(`${TIP},${BH / 2}`)  // left notch
  return pts.join(' ')
}

// ── Main component ─────────────────────────────────────────────────────────────

export function SunburstChart({
  data,
  diameter = 560,
  maxSteps = 7,
  minAngleDeg = 0.5,
  onNodeClick,
  onArcRightClick,
}: Props) {
  const svgRef = useRef<SVGSVGElement>(null)
  const animFrameRef = useRef<number | null>(null)
  const [path, setPath] = useState<SunburstNode[]>([])
  const [hovered, setHovered] = useState<d3Hierarchy.HierarchyRectangularNode<SunburstNode> | null>(null)

  const radius = diameter / 2
  const minAngleRad = (minAngleDeg / 360) * 2 * Math.PI

  // D3 hierarchy + partition
  const root = d3Hierarchy.hierarchy(data.root)
    .sum(d => d.children ? 0 : d.nodeCount)
    .sort((a, b) => (b.value ?? 0) - (a.value ?? 0))

  // Re-sum using nodeCount directly (not leaf-only)
  root.each(d => { (d as d3Hierarchy.HierarchyNode<SunburstNode> & { value: number }).value = d.data.nodeCount })

  const partition = d3Hierarchy.partition<SunburstNode>()
    .size([2 * Math.PI, maxSteps + 1])

  const partitioned = partition(root)

  // Arc generator — sqrt radii for equal-area rings
  const arc = d3Shape.arc<d3Hierarchy.HierarchyRectangularNode<SunburstNode>>()
    .startAngle(d => d.x0)
    .endAngle(d => d.x1)
    .padAngle(d => Math.min((d.x1 - d.x0) / 2, 2 / diameter))
    .padRadius(radius)
    .innerRadius(d => radius * Math.sqrt(d.y0 / (maxSteps + 1)))
    .outerRadius(d => radius * Math.sqrt(d.y1 / (maxSteps + 1)) - 1)

  function arcVisible(d: d3Hierarchy.HierarchyRectangularNode<SunburstNode>) {
    return d.y1 >= 1 && d.y1 <= maxSteps + 1 && (d.x1 - d.x0) >= minAngleRad
  }

  const visibleNodes = partitioned.descendants().filter(arcVisible)

  // Zoom state
  const [zoomNode, setZoomNode] = useState<d3Hierarchy.HierarchyRectangularNode<SunburstNode>>(partitioned)
  const [animZoom, setAnimZoom] = useState<ZoomCoords>({ x0: 0, x1: 2 * Math.PI, y0: 0 })

  useEffect(() => () => { if (animFrameRef.current != null) cancelAnimationFrame(animFrameRef.current) }, [])

  function tweenZoom(from: ZoomCoords, to: ZoomCoords) {
    if (animFrameRef.current != null) cancelAnimationFrame(animFrameRef.current)
    const ix0 = d3Interpolate.interpolateNumber(from.x0, to.x0)
    const ix1 = d3Interpolate.interpolateNumber(from.x1, to.x1)
    const iy0 = d3Interpolate.interpolateNumber(from.y0, to.y0)
    const start = performance.now()
    const DURATION = 400
    function tick(now: number) {
      const raw = Math.min((now - start) / DURATION, 1)
      const t = raw < 0.5 ? 2 * raw * raw : -1 + (4 - 2 * raw) * raw  // easeInOutQuad
      setAnimZoom({ x0: ix0(t), x1: ix1(t), y0: iy0(t) })
      if (raw < 1) animFrameRef.current = requestAnimationFrame(tick)
      else animFrameRef.current = null
    }
    animFrameRef.current = requestAnimationFrame(tick)
  }

  function handleDoubleClick(d: d3Hierarchy.HierarchyRectangularNode<SunburstNode>) {
    const target = d.depth > 0 ? d : partitioned  // double-click root = zoom out
    tweenZoom(animZoom, { x0: target.x0, x1: target.x1, y0: target.y0 })
    setZoomNode(target)
    setPath(target.ancestors().reverse().map(n => n.data).slice(1))
    onNodeClick?.(d.data, d.ancestors().reverse().map(n => n.data).slice(1))
  }

  function handleMouseEnter(d: d3Hierarchy.HierarchyRectangularNode<SunburstNode>) {
    setHovered(d)
    setPath(d.ancestors().reverse().map(n => n.data).slice(1))
  }

  function handleMouseLeave() {
    setHovered(null)
    setPath(zoomNode.ancestors().reverse().map(n => n.data).slice(1))
  }

  // Compute zoomed arc coordinates using interpolated zoom window
  function zoomedArc(d: d3Hierarchy.HierarchyRectangularNode<SunburstNode>, zoom: ZoomCoords) {
    const xScale = (x: number) => ((x - zoom.x0) / (zoom.x1 - zoom.x0)) * 2 * Math.PI
    const scaledD = {
      ...d,
      x0: xScale(d.x0),
      x1: xScale(d.x1),
      y0: d.y0 - zoom.y0,
      y1: d.y1 - zoom.y0,
    }
    return arc(scaledD as any) ?? ''
  }

  const svgWidth = diameter + 40 + (BW + 10) * (maxSteps + 2)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8, fontFamily: 'var(--font-body)' }}>

      {/* Legend */}
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '5px 12px' }}>
        {data.eventNames.map(name => (
          <div key={name} style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
            <div style={{ width: 10, height: 10, borderRadius: 2, background: eventColour(name), flexShrink: 0 }} />
            <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-secondary)', fontFamily: 'var(--font-mono)' }}>{name}</span>
          </div>
        ))}
      </div>

      <div style={{ overflowX: 'auto' }}>
        <svg
          ref={svgRef}
          width={svgWidth}
          height={diameter + 20}
          style={{ display: 'block', cursor: 'default' }}
        >
          {/* Sunburst arcs */}
          <g transform={`translate(${radius + 10},${radius + 10})`}>
            {/* Centre circle — double-click to zoom out */}
            <circle
              r={radius * Math.sqrt(1 / (maxSteps + 1)) - 2}
              fill="var(--surface-raised)"
              stroke="var(--rule)"
              strokeWidth={1}
              style={{ cursor: zoomNode.depth > 0 ? 'pointer' : 'default' }}
              onDoubleClick={() => {
                tweenZoom(animZoom, { x0: partitioned.x0, x1: partitioned.x1, y0: partitioned.y0 })
                setZoomNode(partitioned)
                setPath([])
              }}
            />
            <text textAnchor="middle" dominantBaseline="middle"
              style={{ fontSize: 11, fill: 'var(--ink-tertiary)', pointerEvents: 'none', userSelect: 'none' }}>
              {zoomNode.depth > 0 ? '← back' : 'start'}
            </text>

            {visibleNodes.map(d => {
              const fill = d.depth === 0 ? 'none' : eventColour(d.data.name)
              const isHovered = hovered?.data.nodeId === d.data.nodeId
              const pathIds = new Set(path.map(n => n.nodeId))
              const onPath = pathIds.has(d.data.nodeId)
              return (
                <path
                  key={d.data.nodeId}
                  d={zoomedArc(d, animZoom)}
                  fill={fill}
                  fillOpacity={onPath ? 1 : isHovered ? 0.85 : 0.7}
                  stroke="var(--surface-paper)"
                  strokeWidth={0.5}
                  style={{ cursor: 'pointer', transition: 'fill-opacity 0.15s' }}
                  onMouseEnter={() => handleMouseEnter(d)}
                  onMouseLeave={handleMouseLeave}
                  onDoubleClick={() => handleDoubleClick(d)}
                  onClick={() => onNodeClick?.(d.data, d.ancestors().reverse().map(n => n.data).slice(1))}
                  onContextMenu={e => {
                    if (!onArcRightClick) return
                    e.preventDefault()
                    onArcRightClick(d.data, collectSubtreeSeqIds(d.data))
                  }}
                >
                  <title>{`${d.data.name}\n${d.data.nodeCount.toLocaleString()} sequences\n${((d.data.nodeCount / data.totalSequences) * 100).toFixed(1)}%`}</title>
                </path>
              )
            })}

            {/* Arc labels for large arcs */}
            {visibleNodes.filter(d => d.depth > 0 && (d.x1 - d.x0) > 0.15).map(d => {
              const centroid = arc.centroid(d as d3Hierarchy.HierarchyRectangularNode<SunburstNode>)
              if (!centroid || isNaN(centroid[0])) return null
              const xScale = (x: number) => ((x - animZoom.x0) / (animZoom.x1 - animZoom.x0)) * 2 * Math.PI
              const cx = (radius * Math.sqrt((d.y0 + d.y1) / 2 / (maxSteps + 1))) *
                Math.sin((xScale(d.x0) + xScale(d.x1)) / 2)
              const cy = -(radius * Math.sqrt((d.y0 + d.y1) / 2 / (maxSteps + 1))) *
                Math.cos((xScale(d.x0) + xScale(d.x1)) / 2)
              return (
                <text
                  key={`lbl-${d.data.nodeId}`}
                  x={cx} y={cy}
                  textAnchor="middle" dominantBaseline="middle"
                  style={{ fontSize: 9, fill: 'var(--ink-primary)', pointerEvents: 'none', userSelect: 'none' }}
                >
                  {d.data.name.length > 10 ? d.data.name.slice(0, 9) + '…' : d.data.name}
                </text>
              )
            })}
          </g>

          {/* Breadcrumb trail */}
          <g transform={`translate(${diameter + 30}, 10)`}>
            {path.map((node, i) => (
              <g key={node.nodeId} transform={`translate(0, ${i * (BH + TIP + 4)})`}>
                <polygon
                  points={breadcrumbPoints(i)}
                  fill={eventColour(node.name)}
                  opacity={0.85}
                />
                <text
                  x={BW / 2 + (i > 0 ? TIP / 2 : 0)}
                  y={BH / 2 + 1}
                  textAnchor="middle"
                  dominantBaseline="middle"
                  style={{ fontSize: 10, fill: 'var(--ink-primary)', pointerEvents: 'none', userSelect: 'none' }}
                >
                  {node.name.length > 14 ? node.name.slice(0, 13) + '…' : node.name}
                </text>
              </g>
            ))}

            {/* Count / percentage for hovered arc */}
            {hovered && hovered.depth > 0 && (
              <g transform={`translate(0, ${path.length * (BH + TIP + 4) + 8})`}>
                <rect x={0} y={0} width={BW} height={44}
                  rx={4} fill="var(--surface-raised)" stroke="var(--rule)" />
                <text x={BW / 2} y={16} textAnchor="middle"
                  style={{ fontSize: 13, fontWeight: 700, fill: 'var(--ink-primary)', userSelect: 'none' }}>
                  {hovered.data.nodeCount.toLocaleString()}
                </text>
                <text x={BW / 2} y={32} textAnchor="middle"
                  style={{ fontSize: 11, fill: 'var(--ink-tertiary)', userSelect: 'none' }}>
                  {((hovered.data.nodeCount / data.totalSequences) * 100).toFixed(1)}% of all
                </text>
              </g>
            )}
          </g>
        </svg>
      </div>

      {/* Stats */}
      <div style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>
        {data.totalSequences.toLocaleString()} sequences · {data.eventNames.length} event types
        {zoomNode.depth > 0 && (
          <span> · zoomed on <strong>{zoomNode.data.name}</strong> — double-click centre to zoom out</span>
        )}
        {zoomNode.depth === 0 && (
          <span> · double-click any arc to zoom in</span>
        )}
        {onArcRightClick && (
          <span> · right-click arc to inspect field distribution</span>
        )}
      </div>
    </div>
  )
}
