import { describe, expect, it } from 'vitest'
import { buildTagColors, extractPatternTags, resolveTagOrder, SOL_NEUTRAL, SOL_TAG_PALETTE } from './sol-colors'

describe('extractPatternTags', () => {
  it('extracts bare event names in pattern order', () => {
    expect(extractPatternTags('match home_page >> * >> search_page >> * >> watch_start'))
      .toEqual(['home_page', 'search_page', 'watch_start'])
  })

  it('uses the tag name for tagged elements', () => {
    expect(extractPatternTags('match A(event_a) >> * >> B(event_b)'))
      .toEqual(['A', 'B'])
  })

  it('skips wildcards, untagged alternations, and start/end anchors', () => {
    expect(extractPatternTags('match start >> (a | b) >> * >> end')).toEqual([])
  })

  it('only reads the first line of the match clause', () => {
    expect(extractPatternTags('match home >> tail\nif duration(home, tail) < 5min'))
      .toEqual(['home', 'tail'])
  })

  it('returns empty for queries without a match clause', () => {
    expect(extractPatternTags('filter MATCHED')).toEqual([])
  })

  it('deduplicates repeated names', () => {
    expect(extractPatternTags('match a >> b >> a')).toEqual(['a', 'b'])
  })
})

describe('buildTagColors', () => {
  it('assigns palette colours in order and neutral to implicit tags', () => {
    const colors = buildTagColors(['home_page', 'search_page'])
    expect(colors['home_page']).toBe(SOL_TAG_PALETTE[0])
    expect(colors['search_page']).toBe(SOL_TAG_PALETTE[1])
    expect(colors['PREFIX']).toBe(SOL_NEUTRAL)
    expect(colors['SUFFIX']).toBe(SOL_NEUTRAL)
  })

  it('does not burn palette slots on implicit tags', () => {
    const colors = buildTagColors(['PREFIX', 'home_page'])
    expect(colors['PREFIX']).toBe(SOL_NEUTRAL)
    expect(colors['home_page']).toBe(SOL_TAG_PALETTE[0])
  })

  it('wraps the palette when there are many tags', () => {
    const tags = Array.from({ length: SOL_TAG_PALETTE.length + 1 }, (_, i) => `t${i}`)
    const colors = buildTagColors(tags)
    expect(colors[`t${SOL_TAG_PALETTE.length}`]).toBe(SOL_TAG_PALETTE[0])
  })
})

describe('resolveTagOrder', () => {
  it('uses the authoritative result tag names when a result exists', () => {
    expect(resolveTagOrder('match C(event_c) >> * >> D(event_d)', ['A', 'B']))
      .toEqual(['A', 'B'])
  })

  it('falls back to parsing the live query text when there is no result yet', () => {
    expect(resolveTagOrder('match A(event_a) >> * >> B(event_b)', undefined))
      .toEqual(['A', 'B'])
  })

  it('falls back to parsing the query when the result has no user tags', () => {
    expect(resolveTagOrder('match A(event_a) >> * >> B(event_b)', []))
      .toEqual(['A', 'B'])
  })

  it('keeps the result order even after the query text is edited without re-running', () => {
    // Simulates: ran with A/B, then the user renamed B to C in the box but hasn't re-run yet.
    // The displayed result still has A/B spans, so colors must stay pinned to A/B.
    expect(resolveTagOrder('match A(event_a) >> * >> C(event_c)', ['A', 'B']))
      .toEqual(['A', 'B'])
  })
})
