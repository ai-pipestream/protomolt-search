import { describe, expect, test } from 'bun:test'
import type { JsonValue } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { createBff } from '../src/facade.ts'
import { ProgressiveSearch, type StreamHandlers } from '../src/progressive.ts'
import { completion, revision, routed, scripted, streamed, until } from './fixtures.ts'

function recorder() {
  const log: string[] = []
  const handlers: StreamHandlers = {
    onMessage: (m: JsonValue) => log.push(`message ${Object.keys(m as object)[0]}`),
    onEnd: () => log.push('end'),
    onError: (e) => log.push(`error ${e.code}`),
    onFallback: (why) => log.push(`fallback ${why}`),
    onKeepalive: () => log.push('keepalive'),
  }
  return { log, handlers }
}

describe('progressive search over Connect', () => {
  test('revisions then the completion arrive in order, then the end', async () => {
    const bff = createBff(
      routed({
        async *queryStream() {
          yield revision(1n)
          yield revision(2n)
          yield completion(2n)
        },
      }),
    )
    const { log, handlers } = recorder()
    new ProgressiveSearch(bff, { transport: 'connect' }).start('QueryStream', { query: {} }, handlers)
    await until(() => log.includes('end'))
    expect(log).toEqual(['message revision', 'message revision', 'message completion', 'end'])
  })

  test('a new stream cancels the one before it, server side too', async () => {
    let firstAborted = false
    let calls = 0
    const bff = createBff(
      routed({
        async *queryStream(_request, context) {
          calls += 1
          if (calls === 1) {
            yield revision(1n)
            await new Promise<void>((resolve) => context.signal.addEventListener('abort', () => resolve()))
            firstAborted = true
            return
          }
          yield completion(5n)
        },
      }),
    )
    const search = new ProgressiveSearch(bff, { transport: 'connect' })
    const first = recorder()
    search.start('QueryStream', {}, first.handlers)
    await until(() => first.log.length === 1)
    const second = recorder()
    search.start('QueryStream', {}, second.handlers)
    await until(() => second.log.includes('end') && firstAborted)
    expect(first.log).toEqual(['message revision'])
    expect(second.log).toEqual(['message completion', 'end'])
  })

  test('stop silences the live stream', async () => {
    const bff = createBff(
      routed({
        async *queryStream(_request, context) {
          yield revision(1n)
          await new Promise((r) => context.signal.addEventListener('abort', r))
        },
      }),
    )
    const search = new ProgressiveSearch(bff, { transport: 'connect' })
    const { log, handlers } = recorder()
    search.start('QueryStream', {}, handlers)
    await until(() => log.length === 1)
    search.stop()
    await new Promise((r) => setTimeout(r, 20))
    expect(log).toEqual(['message revision'])
  })

  test('a request that does not decode is refused before any transport', async () => {
    const { fetch, calls } = scripted([])
    const bff = createBff(routed({}))
    const { log, handlers } = recorder()
    new ProgressiveSearch(bff, { fetch }).start('QueryStream', { query: { nope: 1 } }, handlers)
    await until(() => log.length > 0)
    expect(log).toEqual(['error INVALID_ARGUMENT'])
    expect(calls).toHaveLength(0)
  })

  test('an error the console sends is final, not a fallback', async () => {
    const bff = createBff(
      routed({
        async *queryStream() {
          throw new ConnectError('collection "x" is unknown', Code.NotFound)
        },
      }),
    )
    const { log, handlers } = recorder()
    new ProgressiveSearch(bff).start('QueryStream', {}, handlers)
    await until(() => log.length > 0)
    expect(log).toEqual(['error NOT_FOUND'])
  })
})

describe('the SSE fallback', () => {
  const sse = () =>
    streamed([': keepalive\n\n', 'event: message\ndata: {"completion":{"completed":true}}\n\n', 'event: end\ndata: {}\n\n'])

  test('a transport failure before the first message replays the request over SSE', async () => {
    const bff = createBff(
      routed({
        async *queryStream() {
          throw new ConnectError('upstream connect error', Code.Unavailable)
        },
      }),
    )
    const { fetch, calls } = scripted([sse])
    const { log, handlers } = recorder()
    new ProgressiveSearch(bff, { fetch }).start('QueryStream', { query: { k: 2 } }, handlers)
    await until(() => log.includes('end'))
    expect(log).toEqual(['fallback upstream connect error', 'keepalive', 'message completion', 'end'])
    expect(calls[0]?.url).toBe('/bff/v1/sse/QueryStream')
    expect(JSON.parse(String(calls[0]?.init?.body))).toEqual({ query: { k: 2 } })
  })

  test('a Connect stream with no first message in time falls back', async () => {
    const bff = createBff(
      routed({
        async *queryStream(_request, context) {
          await new Promise((r) => context.signal.addEventListener('abort', r))
        },
      }),
    )
    const { fetch } = scripted([sse])
    const { log, handlers } = recorder()
    new ProgressiveSearch(bff, { fetch, firstMessageMs: 10 }).start('QueryStream', {}, handlers)
    await until(() => log.includes('end'))
    expect(log[0]).toBe('fallback no first message over Connect')
  })
})
