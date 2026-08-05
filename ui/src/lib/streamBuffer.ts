// Pure helpers for the live-tail record buffer used by the topic replay
// stream (src/routes/topics/$topic.tsx). Kept dependency-free so they can
// be unit tested without touching the DOM or mocking EventSource/fetch.

/** Hard cap on how many records the live-tail buffer holds once follow=true
 *  tailing is active. Historical drain (before the `follow` preamble) is
 *  not capped here — that's already bounded by the query's own
 *  from/to/limit params. */
export const MAX_LIVE_TAIL_BUFFER = 5000

/** How many records to trim in one batch once the cap is exceeded, so the
 *  O(n) splice only runs once every N pushes instead of on every push —
 *  amortized O(1) per record. */
export const LIVE_TAIL_TRIM_BATCH = 500

/** Push `record` onto `buffer` in place, then, if the buffer has grown
 *  past MAX_LIVE_TAIL_BUFFER + LIVE_TAIL_TRIM_BATCH, drop the oldest
 *  excess in a single splice so the buffer settles back to
 *  MAX_LIVE_TAIL_BUFFER. */
export function pushCapped<T>(buffer: T[], record: T): void {
  buffer.push(record)
  if (buffer.length > MAX_LIVE_TAIL_BUFFER + LIVE_TAIL_TRIM_BATCH) {
    buffer.splice(0, buffer.length - MAX_LIVE_TAIL_BUFFER)
  }
}

/** Compose the buffer to render for order=desc + follow=true tailing,
 *  where live records must visually prepend (newest-first) without ever
 *  doing an O(n) unshift per incoming record. `liveBuffer` holds live
 *  records in arrival order (oldest-of-the-live-batch first);
 *  `historyBuffer` holds the already-drained history, already in the
 *  correct newest-first order. */
export function composeDescLiveView<T>(historyBuffer: T[], liveBuffer: T[]): T[] {
  if (liveBuffer.length === 0) return historyBuffer
  return [...liveBuffer].reverse().concat(historyBuffer)
}
