import { useEffect, useRef } from 'react'

/**
 * Like setInterval, but pauses while the document is hidden (backgrounded
 * tab) instead of continuing to fire, and — on becoming visible again —
 * fires one immediate catch-up call before resuming the normal cadence.
 */
export function useVisibilityAwareInterval(callback: () => void, intervalMs: number): void {
  const callbackRef = useRef(callback)
  useEffect(() => { callbackRef.current = callback }, [callback])

  useEffect(() => {
    let timer: ReturnType<typeof setInterval> | null = null

    function start() {
      if (timer == null) timer = setInterval(() => callbackRef.current(), intervalMs)
    }
    function stop() {
      if (timer != null) { clearInterval(timer); timer = null }
    }
    function handleVisibilityChange() {
      if (document.visibilityState === 'hidden') {
        stop()
      } else {
        callbackRef.current() // catch-up fetch on resume
        start()
      }
    }

    if (document.visibilityState !== 'hidden') start()
    document.addEventListener('visibilitychange', handleVisibilityChange)
    return () => {
      stop()
      document.removeEventListener('visibilitychange', handleVisibilityChange)
    }
  }, [intervalMs])
}
