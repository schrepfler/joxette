import { useEffect, useState } from 'react'
import { Debouncer } from '@tanstack/pacer'

/**
 * Returns a debounced copy of `value` that only updates after `delay` ms of inactivity.
 * Uses @tanstack/pacer's Debouncer under the hood.
 */
export function useDebounce<T>(value: T, delay = 300): T {
  const [debounced, setDebounced] = useState<T>(value)

  useEffect(() => {
    const debouncer = new Debouncer((v: T) => setDebounced(v), { wait: delay })
    debouncer.maybeExecute(value)
    return () => debouncer.cancel()
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [value, delay])

  return debounced
}
