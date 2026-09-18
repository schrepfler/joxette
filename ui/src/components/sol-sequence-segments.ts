/**
 * Pure sequence-segmentation logic shared by every "sequence as chips" view:
 * the multi-entity examples pane (SolExamplesPane) and the single-sequence
 * result view (SolResultSequence, in SolQueryPanel).
 *
 * A sequence of N events is covered by zero or more named tag spans. This
 * splits it into contiguous runs that share the same covering tag (or none),
 * so a renderer can group chips under one coloured header per run.
 */

import type { SolTagSpan } from '../api/client'

export interface SolSequenceSegment {
  tag: string | null
  from: number
  to: number
}

/**
 * Splits `n` events into contiguous runs sharing the same covering tag.
 *
 * `PREFIX` and `SEQ` are never shown as segment tags: `PREFIX` labels "before
 * the match", visually redundant with the untagged run it always coincides
 * with, and `SEQ` always spans the entire sequence, so — depending on object
 * key iteration order — it could otherwise clobber a real tag (including the
 * other implicit one, `SUFFIX`) that happens to be processed first. `SUFFIX`
 * itself is an implicit catch-all and yields to any more specific tag
 * covering the same events; no non-implicit tag is ever overridden.
 */
export function segmentSequence(n: number, tags: Record<string, SolTagSpan>): SolSequenceSegment[] {
  const cover: (string | null)[] = new Array(n).fill(null)
  for (const [name, span] of Object.entries(tags)) {
    if (name === 'PREFIX' || name === 'SEQ') continue
    for (let i = span.from; i < Math.min(span.to, n); i++) {
      if (cover[i] === null || cover[i] === 'SUFFIX') cover[i] = name
    }
  }
  const segments: SolSequenceSegment[] = []
  let start = 0
  for (let i = 1; i <= n; i++) {
    if (i === n || cover[i] !== cover[start]) {
      segments.push({ tag: cover[start], from: start, to: i })
      start = i
    }
  }
  return segments
}

/** One renderable item: a single event chip, or a collapsed run of untagged events. */
export type FlatSequenceItem =
  | { kind: 'event'; index: number; name: string; segmentTag: string | null; isSegmentStart: boolean }
  | { kind: 'gap'; from: number; to: number }

/**
 * `'tagged-only'` collapses a segment when it's untagged, or tagged `SUFFIX` —
 * the implicit "everything after the interesting match" catch-all, which is
 * exactly the long trailing filler this mode exists to hide. `MATCHED` is kept:
 * for a pattern with no named tag bindings (`match event1 >> * >> event2`,
 * no `Tag(...)`), it's the only label the actual match ever gets, so hiding it
 * would hide the one thing 'tagged-only' is supposed to surface.
 */
function isCollapsibleInTaggedOnlyMode(tag: string | null): boolean {
  return tag === null || tag === 'SUFFIX'
}

/**
 * Flattens events + their segments into a renderable list.
 *
 * `'all'` emits one item per event. `'tagged-only'` drops structural filler
 * (see {@link isCollapsibleInTaggedOnlyMode}), collapsing each such run into a
 * single `gap` marker so a long sequence with a short match and a long tail
 * doesn't render thousands of filler chips.
 */
export function flattenSequence(
  events: string[],
  segments: SolSequenceSegment[],
  mode: 'all' | 'tagged-only',
): FlatSequenceItem[] {
  const items: FlatSequenceItem[] = []
  for (const seg of segments) {
    if (mode === 'tagged-only' && isCollapsibleInTaggedOnlyMode(seg.tag)) {
      items.push({ kind: 'gap', from: seg.from, to: seg.to })
      continue
    }
    for (let i = seg.from; i < seg.to; i++) {
      items.push({ kind: 'event', index: i, name: events[i], segmentTag: seg.tag, isSegmentStart: i === seg.from })
    }
  }
  return items
}
