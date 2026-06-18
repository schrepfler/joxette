/**
 * Decodes a base64url or standard base64 string to a UTF-8 string.
 * Returns null if the input is not valid base64.
 */
export function tryDecodeBase64(s: string): string | null {
  try {
    if (!/^[A-Za-z0-9+/\-_]+=*$/.test(s)) return null
    return atob(s.replace(/-/g, '+').replace(/_/g, '/'))
  } catch {
    return null
  }
}

/**
 * Decodes a base64url or standard base64 string, returning the original string
 * unchanged if it is not valid base64.
 */
export function decodeB64(s: string | null): string | null {
  if (!s) return null
  const decoded = tryDecodeBase64(s)
  return decoded ?? s
}

/**
 * Attempts to parse a message value as JSON, trying both the raw string and
 * a base64-decoded form. Returns the parsed object and the decoded raw string,
 * or null if parsing fails.
 */
export function tryParseValue(s: string | null): { parsed: unknown; raw: string } | null {
  if (!s) return null
  try { return { parsed: JSON.parse(s) as unknown, raw: s } } catch { /* continue */ }
  const decoded = tryDecodeBase64(s)
  if (decoded) {
    try { return { parsed: JSON.parse(decoded) as unknown, raw: decoded } } catch { /* continue */ }
  }
  return null
}

/**
 * Decodes a base64url event value and extracts a numeric field by dot-path
 * (e.g. "amount" or "order.total").
 */
export function extractNumeric(valueB64: string | null, path: string): number | null {
  if (!valueB64) return null
  const result = tryParseValue(valueB64)
  if (!result) return null
  const parts = path.split('.')
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let cur: any = result.parsed
  for (const p of parts) {
    if (cur == null || typeof cur !== 'object') return null
    cur = (cur as Record<string, unknown>)[p]
  }
  return typeof cur === 'number' ? cur : null
}
