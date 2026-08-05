// @vitest-environment jsdom
//
// Regression test for a bug caught in review on Task 3 (commit 1e21163):
// composeDescLiveView(historyBuffer, liveBuffer) returns historyBuffer BY
// REFERENCE whenever liveBuffer is empty — true for the entire duration of
// any order=asc stream, any follow=false replay, and the historical-drain
// portion of an order=desc && follow=true stream. Meanwhile the history
// buffer in $topic.tsx (streamBufferRef.current) is one long-lived array
// mutated in place via push()/pushCapped() — its identity never changes on
// its own. $topic.tsx's composeStreamedRecords() previously did:
//
//   return composeDescLiveView(streamBufferRef.current, liveBufferRef.current)
//
// and every call site (the 250ms flush interval, onDone, onError,
// stopStream) fed that straight into setStreamedRecords(). Because React
// 19's setState bails out via Object.is when the new value is
// reference-equal to current state, the second and all subsequent flushes
// with that same mutated-in-place array were silently dropped — the
// component stopped re-rendering after the first successful flush, even
// though new records kept arriving into the ref.
//
// The fix lives in streamBuffer.ts's composeStreamView(), which wraps
// composeDescLiveView's result in a fresh top-level array unconditionally.
// This test exercises the ACTUAL production functions (pushCapped,
// composeDescLiveView, composeStreamView) through a real React component and
// DOM assertions — not another isolated pure-function test, since
// streamBuffer.test.ts already covers composeDescLiveView/pushCapped in
// isolation and passed even with this bug present (the bug was entirely in
// how the caller consumed the return value, not in the pure functions
// themselves).
import { describe, it, expect, afterEach } from 'vitest'
import { useRef, useState } from 'react'
import { render, screen, cleanup, act } from '@testing-library/react'
import { pushCapped, composeDescLiveView, composeStreamView } from '../../lib/streamBuffer'

afterEach(() => cleanup())

/** Mirrors $topic.tsx's streaming state shape exactly: a ref-backed history
 *  buffer mutated in place via pushCapped, an empty live buffer (reproducing
 *  the order=asc / follow=false case, where composeDescLiveView always takes
 *  its reference-returning branch), and a flush() analogous to the 4 real
 *  setStreamedRecords(composeStreamedRecords()) call sites. `compose` is
 *  injected so the "old, buggy call site" and "current, fixed call site" can
 *  be exercised against the identical harness. */
function StreamHarness({ compose }: { compose: (history: string[], live: string[]) => string[] }) {
  const historyRef = useRef<string[]>([])
  const liveRef = useRef<string[]>([])
  const [records, setRecords] = useState<string[]>([])

  function flush() {
    setRecords(compose(historyRef.current, liveRef.current))
  }

  return (
    <div>
      <button onClick={() => { pushCapped(historyRef.current, `r${historyRef.current.length}`); flush() }}>
        push-and-flush
      </button>
      <div data-testid="count">{records.length}</div>
      <div data-testid="last">{records[records.length - 1] ?? ''}</div>
    </div>
  )
}

describe('streamed-records flush pattern (regression for Task 3 review bug)', () => {
  it('reproduces the bug: the old call site (raw composeDescLiveView) freezes the render after the first flush', () => {
    // This is exactly what composeStreamedRecords() did before the fix:
    // `return composeDescLiveView(historyBuffer, liveBuffer)`.
    render(<StreamHarness compose={composeDescLiveView} />)
    const button = screen.getByRole('button', { name: 'push-and-flush' })

    act(() => button.click())
    expect(screen.getByTestId('count').textContent).toBe('1')
    expect(screen.getByTestId('last').textContent).toBe('r0')

    // Second and third flushes mutate the same underlying array in place
    // (pushCapped never reallocates) and hand React the exact same
    // reference back — React 19 bails out and drops the update.
    act(() => button.click())
    act(() => button.click())

    // This is the bug: the DOM is stuck at the first flush's snapshot even
    // though the underlying buffer actually grew to 3 records.
    expect(screen.getByTestId('count').textContent).toBe('1')
    expect(screen.getByTestId('last').textContent).toBe('r0')
  })

  it('proves the fix: composeStreamView (the real production function) re-renders on every flush', () => {
    render(<StreamHarness compose={composeStreamView} />)
    const button = screen.getByRole('button', { name: 'push-and-flush' })

    act(() => button.click())
    expect(screen.getByTestId('count').textContent).toBe('1')
    expect(screen.getByTestId('last').textContent).toBe('r0')

    act(() => button.click())
    expect(screen.getByTestId('count').textContent).toBe('2')
    expect(screen.getByTestId('last').textContent).toBe('r1')

    act(() => button.click())
    expect(screen.getByTestId('count').textContent).toBe('3')
    expect(screen.getByTestId('last').textContent).toBe('r2')
  })
})
