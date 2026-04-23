import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { streamLines, __testables } from './client'

const { createSseParser, createNdjsonHandler } = __testables

function chunk(s: string): Uint8Array {
  return new TextEncoder().encode(s)
}

function respondWith(body: string): Response {
  const stream = new ReadableStream<Uint8Array>({
    start(ctrl) {
      ctrl.enqueue(chunk(body))
      ctrl.close()
    },
  })
  return new Response(stream, {
    status: 200,
    headers: { 'Content-Type': 'text/event-stream' },
  })
}

describe('streamLines — SSE comment filter', () => {
  const originalFetch = globalThis.fetch
  beforeEach(() => {
    vi.restoreAllMocks()
  })
  afterEach(() => {
    globalThis.fetch = originalFetch
  })

  it('drops SSE comment lines and delivers only non-comment lines', async () => {
    const body = [
      ':heartbeat',
      '',
      'data: {"topic":"t","partition":0,"offset":1}',
      '',
      ':another-comment',
      '',
      'event: follow',
      'data: {}',
      '',
      ':keepalive',
      '',
      'data: {"topic":"t","partition":0,"offset":2}',
      '',
    ].join('\n')

    globalThis.fetch = vi.fn().mockResolvedValue(respondWith(body))

    const seen: string[] = []
    const ctrl = new AbortController()
    await streamLines(
      'http://fake/cassettes/topics/x?follow=true',
      'text/event-stream',
      (line) => seen.push(line),
      () => {},
      (e) => { throw e },
      ctrl.signal,
    )

    // Every `:`-prefixed line must have been dropped.
    expect(seen.some(l => l.startsWith(':'))).toBe(false)
    // The record and event lines must survive.
    expect(seen).toContain('data: {"topic":"t","partition":0,"offset":1}')
    expect(seen).toContain('event: follow')
    expect(seen).toContain('data: {}')
    expect(seen).toContain('data: {"topic":"t","partition":0,"offset":2}')
  })

  it('handles CRLF line endings without leaking \\r', async () => {
    const body = ':heartbeat\r\ndata: {"topic":"t"}\r\n\r\n:ping\r\n'
    globalThis.fetch = vi.fn().mockResolvedValue(respondWith(body))

    const seen: string[] = []
    const ctrl = new AbortController()
    await streamLines(
      'http://fake/',
      'text/event-stream',
      (line) => seen.push(line),
      () => {},
      () => {},
      ctrl.signal,
    )

    expect(seen.every(l => !l.endsWith('\r'))).toBe(true)
    expect(seen.some(l => l.startsWith(':'))).toBe(false)
    expect(seen).toContain('data: {"topic":"t"}')
  })
})

describe('createSseParser — event dispatching', () => {
  it('routes default (unnamed) blocks to onRecord and named events to onEvent', () => {
    const records: string[] = []
    const events: Array<[string, string]> = []
    const feed = createSseParser(
      (data) => records.push(data),
      (name, data) => events.push([name, data]),
    )
    // default event (onRecord)
    feed('data: {"a":1}')
    feed('')
    // named event
    feed('event: follow')
    feed('data: {}')
    feed('')
    // named terminal event
    feed('event: overflow')
    feed('data: {"reason":"buffer overflow"}')
    feed('')

    expect(records).toEqual(['{"a":1}'])
    expect(events).toEqual([
      ['follow', '{}'],
      ['overflow', '{"reason":"buffer overflow"}'],
    ])
  })
})

describe('createNdjsonHandler — status vs record split', () => {
  it('directs `{event:...}` envelopes to onStatus and records to onRecord', () => {
    const records: unknown[] = []
    const statuses: string[] = []
    const handle = createNdjsonHandler<unknown>(
      (r) => records.push(r),
      (e) => statuses.push(e),
    )
    handle('{"event":"follow"}')
    handle('{"topic":"t","partition":0,"offset":1}')
    handle('{"event":"heartbeat","ts":"2026-04-23T00:00:00Z"}')
    handle('{"topic":"t","partition":0,"offset":2}')
    handle('{"event":"overflow","reason":"buffer overflow"}')
    // blank line between records in a stream — must be ignored
    handle('')

    expect(statuses).toEqual(['follow', 'overflow']) // heartbeat silently dropped
    expect(records).toEqual([
      { topic: 't', partition: 0, offset: 1 },
      { topic: 't', partition: 0, offset: 2 },
    ])
  })
})
