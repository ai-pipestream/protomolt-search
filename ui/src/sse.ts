/**
 * The server-sent-event fallback for a BFF stream: `POST
 * /bff/v1/sse/<Method>` read with eventsource-parser. The console sends
 * `: keepalive` comments while a stream waits, `message` events with one
 * response each, then one `end` or `error` event.
 */
import { createParser, type EventSourceMessage } from 'eventsource-parser'
import { type BffError, isAbort } from './errors.ts'
import { BFF_BASE, type FetchLike } from './transport.ts'

export interface SseHandlers {
  onMessage(json: unknown): void
  onEnd(): void
  onError(error: BffError): void
  /** Called for every `: keepalive` comment. */
  onKeepalive?(): void
  /** Called before each reconnect, with the attempt number from 1. */
  onReconnect?(attempt: number): void
}

export interface SseOptions {
  method: string
  request: unknown
  signal: AbortSignal
  fetch?: FetchLike
  /** Reconnects after a dropped connection; 0 disables them. */
  retries?: number
  retryDelayMs?: number
}

/** How one attempt finished. */
type Outcome = 'done' | 'dropped'

/** A non-2xx answer as the console's error shape. */
async function httpError(response: Response): Promise<BffError> {
  let body: { code?: string; message?: string } = {}
  try {
    body = (await response.json()) as typeof body
  } catch {
    body = { message: response.statusText }
  }
  const code = (body.code ?? 'unknown').toUpperCase()
  return { error: body.message ?? `HTTP ${response.status}`, code, http: response.status }
}

/** Routes parsed events to the handlers; returns whether a terminal event arrived. */
function dispatcher(handlers: SseHandlers): { feed(chunk: string): void; finished(): boolean } {
  let finished = false
  const parser = createParser({
    onComment(comment: string) {
      if (comment.trim() === 'keepalive') handlers.onKeepalive?.()
    },
    onEvent(event: EventSourceMessage) {
      if (finished) return
      if (event.event === 'end') {
        finished = true
        handlers.onEnd()
      } else if (event.event === 'error') {
        finished = true
        const body = JSON.parse(event.data) as { code: string; message: string }
        handlers.onError({ error: body.message, code: body.code.toUpperCase() })
      } else {
        handlers.onMessage(JSON.parse(event.data))
      }
    },
  })
  return { feed: (chunk) => parser.feed(chunk), finished: () => finished }
}

async function attempt(options: SseOptions, handlers: SseHandlers): Promise<Outcome> {
  const fetchImpl: FetchLike = options.fetch ?? ((input, init) => globalThis.fetch(input, init))
  const response = await fetchImpl(`${BFF_BASE}/sse/${options.method}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', accept: 'text/event-stream' },
    body: JSON.stringify(options.request ?? {}),
    signal: options.signal,
    credentials: 'same-origin',
  })
  if (!response.ok || !response.body) {
    const error = await httpError(response)
    // A refusal is final; an unavailable console may come back.
    if (response.status >= 500) return 'dropped'
    handlers.onError(error)
    return 'done'
  }
  const events = dispatcher(handlers)
  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader()
  for (;;) {
    const { value, done } = await reader.read()
    if (done) break
    events.feed(value)
    if (events.finished()) {
      await reader.cancel()
      return 'done'
    }
  }
  return events.finished() ? 'done' : 'dropped'
}

const sleep = (ms: number, signal: AbortSignal) =>
  new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, ms)
    signal.addEventListener('abort', () => (clearTimeout(timer), resolve()), { once: true })
  })

/**
 * Runs the stream to its end, an error, or `signal`. A connection that
 * drops before the terminal event is reopened with the same request, up
 * to `retries` times; the stream restarts from its first message, so a
 * reconnect is reported through `onReconnect` for the page to reset.
 */
export async function sseStream(options: SseOptions, handlers: SseHandlers): Promise<void> {
  const retries = options.retries ?? 2
  for (let tries = 0; ; tries++) {
    let outcome: Outcome
    try {
      outcome = await attempt(options, handlers)
    } catch (e) {
      if (isAbort(e, options.signal)) return
      outcome = 'dropped'
    }
    if (outcome === 'done' || options.signal.aborted) return
    if (tries >= retries) {
      handlers.onError({ error: 'the event stream dropped and did not come back', code: 'UNAVAILABLE' })
      return
    }
    handlers.onReconnect?.(tries + 1)
    await sleep((options.retryDelayMs ?? 500) * 2 ** tries, options.signal)
    if (options.signal.aborted) return
  }
}
