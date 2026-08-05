import { describe, it, expect } from 'vitest'
import { pushCapped, composeDescLiveView, MAX_LIVE_TAIL_BUFFER, LIVE_TAIL_TRIM_BATCH } from './streamBuffer'

describe('pushCapped', () => {
  it('appends in place without trimming while under the cap', () => {
    const buf: number[] = [1, 2, 3]
    pushCapped(buf, 4)
    expect(buf).toEqual([1, 2, 3, 4])
  })

  it('does not trim until the hysteresis threshold is exceeded', () => {
    const buf: number[] = Array.from({ length: MAX_LIVE_TAIL_BUFFER }, (_, i) => i)
    pushCapped(buf, MAX_LIVE_TAIL_BUFFER)
    expect(buf.length).toBe(MAX_LIVE_TAIL_BUFFER + 1)
  })

  it('trims the oldest entries in one batch once past cap + hysteresis, settling at the cap', () => {
    const initialLength = MAX_LIVE_TAIL_BUFFER + LIVE_TAIL_TRIM_BATCH
    const buf: number[] = Array.from({ length: initialLength }, (_, i) => i)
    pushCapped(buf, 999999)
    expect(buf.length).toBe(MAX_LIVE_TAIL_BUFFER)
    expect(buf[buf.length - 1]).toBe(999999)
    const dropped = initialLength + 1 - MAX_LIVE_TAIL_BUFFER
    expect(buf[0]).toBe(dropped)
  })

  it('never grows unbounded across many pushes', () => {
    const buf: number[] = []
    for (let i = 0; i < MAX_LIVE_TAIL_BUFFER * 5; i++) pushCapped(buf, i)
    expect(buf.length).toBeLessThanOrEqual(MAX_LIVE_TAIL_BUFFER + LIVE_TAIL_TRIM_BATCH)
    expect(buf[buf.length - 1]).toBe(MAX_LIVE_TAIL_BUFFER * 5 - 1)
  })
})

describe('composeDescLiveView', () => {
  it('returns the history buffer unchanged when there are no live records yet', () => {
    const history = ['h2', 'h1', 'h0']
    expect(composeDescLiveView(history, [])).toBe(history)
  })

  it('reverses the live buffer (arrival order) and prepends it to history (already newest-first)', () => {
    const history = ['h2', 'h1', 'h0'] // already newest-first from desc drain
    const live = ['l0', 'l1', 'l2']    // arrival order: l0 arrived first, l2 most recently
    expect(composeDescLiveView(history, live)).toEqual(['l2', 'l1', 'l0', 'h2', 'h1', 'h0'])
  })
})
