/**
 * Shared colour assignment for SOL tags — used by the editor token decorations,
 * the examples pane span overlays, and the sequence model panel, so a tag is
 * the same colour everywhere (Motif-style: `home_page` blue in the query is
 * blue in the results).
 */

export interface SolTagColor {
  /** Saturated colour: span label background, editor underline. */
  strong: string
  /** Light wash: covered chip background, editor token background. */
  wash: string
}

/** Palette assigned to user tags in pattern order. */
export const SOL_TAG_PALETTE: SolTagColor[] = [
  { strong: '#3f6fa8', wash: '#dde8f4' },   // blue
  { strong: '#c25151', wash: '#f6dede' },   // red
  { strong: '#8a5ba6', wash: '#ebe0f2' },   // purple
  { strong: '#2e8b74', wash: '#d8efe8' },   // teal
  { strong: '#c08a2d', wash: '#f5e9d4' },   // amber
  { strong: '#b5497e', wash: '#f3dcea' },   // pink
  { strong: '#587b2e', wash: '#e4eed6' },   // olive
  { strong: '#3d8ca6', wash: '#dcedf3' },   // cyan
]

/** Neutral colour for implicit regions: Prefix, Suffix, unnamed gaps. */
export const SOL_NEUTRAL: SolTagColor = { strong: '#9a938b', wash: '#ece9e5' }

const IMPLICIT = new Set(['PREFIX', 'SUFFIX', 'SEQ', 'MATCHED'])

/**
 * Assigns palette colours to user tags in the given order; implicit tags get
 * the neutral colour. Order should be pattern order (backend model order, or
 * {@link extractPatternTags} before a result exists).
 */
export function buildTagColors(orderedTags: string[]): Record<string, SolTagColor> {
  const colors: Record<string, SolTagColor> = {}
  let i = 0
  for (const tag of orderedTags) {
    if (colors[tag]) continue
    colors[tag] = IMPLICIT.has(tag) ? SOL_NEUTRAL : SOL_TAG_PALETTE[i++ % SOL_TAG_PALETTE.length]
  }
  colors['PREFIX'] ??= SOL_NEUTRAL
  colors['SUFFIX'] ??= SOL_NEUTRAL
  return colors
}

/**
 * Client-side extraction of the taggable element names from the first MATCH
 * clause, in pattern order — used to colour editor tokens before the first run.
 * `Tag(event)` yields the tag name; a bare event name yields itself (the engine
 * auto-tags bare single-event elements); wildcards and untagged alternations
 * yield nothing.
 */
export function extractPatternTags(query: string): string[] {
  const m = query.match(/\bmatch(?:\s+split)?\s+([^\n]*)/i)
  if (!m) return []
  const tags: string[] = []
  for (const rawEl of m[1].split('>>')) {
    const el = rawEl.trim()
    if (!el || el.startsWith('*') || el.startsWith('(')) continue
    const id = el.match(/^([A-Za-z_][A-Za-z0-9_]*)/)
    if (!id) continue
    const name = id[1]
    if (name === 'start' || name === 'end') continue
    if (!tags.includes(name)) tags.push(name)
  }
  return tags
}
