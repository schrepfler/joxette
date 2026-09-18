import { describe, expect, it } from 'vitest'
import { segmentSequence, flattenSequence } from './sol-sequence-segments'

describe('segmentSequence', () => {
  it('returns a single untagged segment when there are no tags', () => {
    expect(segmentSequence(3, {})).toEqual([{ tag: null, from: 0, to: 3 }])
  })

  it('returns an empty list for an empty sequence', () => {
    expect(segmentSequence(0, {})).toEqual([])
  })

  it('splits into untagged / tagged / untagged runs around a tag in the middle', () => {
    expect(segmentSequence(5, { A: { from: 2, to: 4 } })).toEqual([
      { tag: null, from: 0, to: 2 },
      { tag: 'A', from: 2, to: 4 },
      { tag: null, from: 4, to: 5 },
    ])
  })

  it('ignores PREFIX when computing coverage', () => {
    // PREFIX covers 0-2 but must not appear as a segment tag; MATCHED wins there instead.
    expect(segmentSequence(4, { PREFIX: { from: 0, to: 2 }, MATCHED: { from: 0, to: 4 } })).toEqual([
      { tag: 'MATCHED', from: 0, to: 4 },
    ])
  })

  it('ignores SEQ when computing coverage, regardless of key iteration order', () => {
    // SEQ always spans the entire sequence — it must never win over a more specific
    // tag, including another implicit one like SUFFIX, no matter which key the
    // object happens to iterate first.
    expect(segmentSequence(4, { SEQ: { from: 0, to: 4 }, SUFFIX: { from: 2, to: 4 } })).toEqual([
      { tag: null, from: 0, to: 2 },
      { tag: 'SUFFIX', from: 2, to: 4 },
    ])
    expect(segmentSequence(4, { SUFFIX: { from: 2, to: 4 }, SEQ: { from: 0, to: 4 } })).toEqual([
      { tag: null, from: 0, to: 2 },
      { tag: 'SUFFIX', from: 2, to: 4 },
    ])
  })

  it('lets a real tag override SUFFIX but not the other way around', () => {
    // SUFFIX (a broad implicit tag) covers 0-4; A more specifically covers 1-2.
    expect(segmentSequence(4, { SUFFIX: { from: 0, to: 4 }, A: { from: 1, to: 2 } })).toEqual([
      { tag: 'SUFFIX', from: 0, to: 1 },
      { tag: 'A', from: 1, to: 2 },
      { tag: 'SUFFIX', from: 2, to: 4 },
    ])
  })

  it('clamps a span that extends past the sequence length', () => {
    expect(segmentSequence(3, { A: { from: 1, to: 100 } })).toEqual([
      { tag: null, from: 0, to: 1 },
      { tag: 'A', from: 1, to: 3 },
    ])
  })
})

describe('flattenSequence', () => {
  const events = ['a', 'b', 'c', 'd', 'e']
  const segments = segmentSequence(5, { A: { from: 2, to: 4 } })
  // segments: [null 0-2], [A 2-4], [null 4-5]

  it("mode 'all' emits one event item per index, flagging each segment's first event", () => {
    expect(flattenSequence(events, segments, 'all')).toEqual([
      { kind: 'event', index: 0, name: 'a', segmentTag: null, isSegmentStart: true },
      { kind: 'event', index: 1, name: 'b', segmentTag: null, isSegmentStart: false },
      { kind: 'event', index: 2, name: 'c', segmentTag: 'A', isSegmentStart: true },
      { kind: 'event', index: 3, name: 'd', segmentTag: 'A', isSegmentStart: false },
      { kind: 'event', index: 4, name: 'e', segmentTag: null, isSegmentStart: true },
    ])
  })

  it("mode 'tagged-only' collapses each untagged run into a single gap item", () => {
    expect(flattenSequence(events, segments, 'tagged-only')).toEqual([
      { kind: 'gap', from: 0, to: 2 },
      { kind: 'event', index: 2, name: 'c', segmentTag: 'A', isSegmentStart: true },
      { kind: 'event', index: 3, name: 'd', segmentTag: 'A', isSegmentStart: false },
      { kind: 'gap', from: 4, to: 5 },
    ])
  })

  it("mode 'tagged-only' with no tags at all collapses to a single gap covering everything", () => {
    expect(flattenSequence(events, segmentSequence(5, {}), 'tagged-only')).toEqual([
      { kind: 'gap', from: 0, to: 5 },
    ])
  })

  it("mode 'tagged-only' also collapses SUFFIX — it's structural filler after the interesting match, not a named tag worth showing", () => {
    // A(0-1), rest of the sequence is SUFFIX — the exact "long trailing tail" shape a
    // real match produces (e.g. `match A(fixture) >> * >> B(coverage)` where B is early).
    const suffixEvents = ['a', 'b', 'c', 'd', 'e']
    const suffixSegments = segmentSequence(5, { A: { from: 0, to: 1 }, SUFFIX: { from: 1, to: 5 } })
    expect(flattenSequence(suffixEvents, suffixSegments, 'tagged-only')).toEqual([
      { kind: 'event', index: 0, name: 'a', segmentTag: 'A', isSegmentStart: true },
      { kind: 'gap', from: 1, to: 5 },
    ])
  })

  it("mode 'tagged-only' keeps MATCHED visible — it's the interesting result when no named tags were bound", () => {
    const matchedEvents = ['a', 'b', 'c']
    const matchedSegments = segmentSequence(3, { MATCHED: { from: 0, to: 2 } })
    expect(flattenSequence(matchedEvents, matchedSegments, 'tagged-only')).toEqual([
      { kind: 'event', index: 0, name: 'a', segmentTag: 'MATCHED', isSegmentStart: true },
      { kind: 'event', index: 1, name: 'b', segmentTag: 'MATCHED', isSegmentStart: false },
      { kind: 'gap', from: 2, to: 3 },
    ])
  })
})
