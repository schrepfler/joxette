// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook } from '@testing-library/react'
import { useVisibilityAwareInterval } from './useVisibilityAwareInterval'

function setVisibility(state: 'visible' | 'hidden') {
  Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => state })
  document.dispatchEvent(new Event('visibilitychange'))
}

describe('useVisibilityAwareInterval', () => {
  beforeEach(() => { vi.useFakeTimers(); setVisibility('visible') })
  afterEach(() => { vi.useRealTimers() })

  it('fires on the normal cadence while visible', () => {
    const calls: number[] = []
    renderHook(() => useVisibilityAwareInterval(() => calls.push(1), 1000))
    vi.advanceTimersByTime(2500)
    expect(calls.length).toBe(2)
  })

  it('stops firing while the tab is hidden', () => {
    const calls: number[] = []
    renderHook(() => useVisibilityAwareInterval(() => calls.push(1), 1000))
    vi.advanceTimersByTime(2000)
    expect(calls.length).toBe(2)
    setVisibility('hidden')
    vi.advanceTimersByTime(5000)
    expect(calls.length).toBe(2) // unchanged while hidden
  })

  it('fires an immediate catch-up call on resume, then resumes the normal cadence', () => {
    const calls: number[] = []
    renderHook(() => useVisibilityAwareInterval(() => calls.push(1), 1000))
    vi.advanceTimersByTime(1000)
    expect(calls.length).toBe(1)
    setVisibility('hidden')
    vi.advanceTimersByTime(5000)
    setVisibility('visible')
    expect(calls.length).toBe(2) // immediate catch-up call on resume
    vi.advanceTimersByTime(1000)
    expect(calls.length).toBe(3)
  })
})
