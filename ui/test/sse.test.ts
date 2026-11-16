import { describe, expect, test } from 'bun:test'
import { type SseHandlers, sseStream } from '../src/sse.ts'
import { scripted, streamed } from './fixtures.ts'

function recorder() {
  const log: string[] = []
  const handlers: SseHandlers = {
    onMessage: (m) => log.push(`message ${JSON.stringify(m)}`),
    onEnd: () => log.push('end'),
    onError: (e) => log.push(`error ${e.code} ${e.error}${e.http ? ` ${e.http}` : ''}`),
    onKeepalive: () => log.push('keepalive'),
    onReconnect: (n) => log.push(`reconnect ${n}`),
  }
  return { log, handlers }
}

const run = (fetch: ReturnType<typeof scripted>['fetch'], signal = new AbortController().signal) => {
  const { log, handlers } = recorder()
  const done = sseStream({ method: 'QueryStream', request: {}, signal, fetch, retryDelayMs: 1 }, handlers)
  return { log, done }
}

describe('server-sent events', () => {
  test('frames split anywhere parse, and keep-alive comments are counted, not delivered', async () => {
    const { fetch } = scripted([
      () =>
        streamed([': keep', 'alive\n\nevent: mes', 'sage\ndata: {"a":', '1}\n\n: keepalive\n', '\nevent: end\ndata: {}\n\n']),
    ])
    const { log, done } = run(fetch)
    await done
    expect(log).toEqual(['keepalive', 'message {"a":1}', 'keepalive', 'end'])
  })

  test('an error event is final and keeps its code', async () => {
    const { fetch, calls } = scripted([
      () => streamed(['event: error\ndata: {"code":"invalid_argument","message":"bad limit"}\n\n']),
    ])
    const { log, done } = run(fetch)
    await done
    expect(log).toEqual(['error INVALID_ARGUMENT bad limit'])
    expect(calls).toHaveLength(1)
  })

  test('a connection dropped before the end reconnects, then succeeds', async () => {
    const { fetch, calls } = scripted([
      () => streamed([': keepalive\n\n', 'event: message\ndata: 1\n\n']),
      () => streamed(['event: message\ndata: 1\n\nevent: end\ndata: {}\n\n']),
    ])
    const { log, done } = run(fetch)
    await done
    expect(log).toEqual(['keepalive', 'message 1', 'reconnect 1', 'message 1', 'end'])
    expect(calls).toHaveLength(2)
  })

  test('reconnects are bounded and end in an error', async () => {
    const dropped = () => streamed([': keepalive\n\n'])
    const { fetch, calls } = scripted([dropped, dropped, dropped])
    const { log, done } = run(fetch)
    await done
    expect(calls).toHaveLength(3)
    expect(log.at(-1)).toBe('error UNAVAILABLE the event stream dropped and did not come back')
  })

  test('a refusal is final and carries the HTTP status', async () => {
    const refused = () =>
      new Response(JSON.stringify({ code: 'permission_denied', message: 'Origin "x" is not this console\'s' }), {
        status: 403,
        headers: { 'content-type': 'application/json' },
      })
    const { fetch, calls } = scripted([refused])
    const { log, done } = run(fetch)
    await done
    expect(log).toEqual([`error PERMISSION_DENIED Origin "x" is not this console's 403`])
    expect(calls).toHaveLength(1)
  })

  test('an abort ends the stream quietly', async () => {
    const controller = new AbortController()
    const { fetch } = scripted([
      () => {
        controller.abort()
        throw new DOMException('aborted', 'AbortError')
      },
    ])
    const { log, done } = run(fetch, controller.signal)
    await done
    expect(log).toEqual([])
  })
})
