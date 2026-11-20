/**
 * Progressive search: one live stream at a time. Starting a stream aborts
 * the one before it (a new keystroke, a new page), so a slow stream never
 * paints over a newer one. Streams go over Connect; when the first message
 * does not arrive in time, or the transport fails before it, the same
 * request is replayed over server-sent events, which a buffering HTTP/1.1
 * proxy passes through because the console keeps them alive.
 */
import { Code, ConnectError } from '@connectrpc/connect'
import type { JsonValue } from '@bufbuild/protobuf'
import { type BffError, isAbort, toBffError } from './errors.ts'
import type { Bff } from './facade.ts'
import { sseStream } from './sse.ts'
import type { FetchLike } from './transport.ts'

export interface StreamHandlers {
  onMessage(message: JsonValue): void
  onEnd(): void
  onError(error: BffError): void
  /** The stream moved to server-sent events, and why. */
  onFallback?(reason: string): void
  onKeepalive?(): void
}

export interface ProgressiveOptions {
  /** `auto` tries Connect first and falls back to SSE. */
  transport?: 'auto' | 'connect' | 'sse'
  /** How long `auto` waits for the first Connect message. */
  firstMessageMs?: number
  fetch?: FetchLike
  sseRetries?: number
  sseRetryDelayMs?: number
}

/** A failure of the transport rather than an answer from the console. */
function transportFailure(e: unknown): boolean {
  if (e instanceof TypeError) return true
  if (!(e instanceof ConnectError)) return false
  return e.code === Code.Unknown || e.code === Code.Unavailable || e.code === Code.Internal
}

export class ProgressiveSearch {
  #controller: AbortController | undefined

  constructor(
    private readonly bff: Bff,
    private readonly options: ProgressiveOptions = {},
  ) {}

  /** Starts `method` with `request`, cancelling the stream before it. Returns its stop. */
  start(method: string, request: unknown, handlers: StreamHandlers): () => void {
    this.#controller?.abort()
    const controller = new AbortController()
    this.#controller = controller
    void this.#run(method, request, handlers, controller.signal)
    return () => controller.abort()
  }

  /** Cancels the live stream, if any; its handlers are not called again. */
  stop(): void {
    this.#controller?.abort()
    this.#controller = undefined
  }

  async #run(method: string, request: unknown, handlers: StreamHandlers, signal: AbortSignal) {
    const mode = this.options.transport ?? 'auto'
    if (mode === 'sse') return this.#sse(method, request, handlers, signal)
    const slow = new AbortController()
    const watchdog =
      mode === 'auto' ? setTimeout(() => slow.abort(), this.options.firstMessageMs ?? 10_000) : undefined
    let received = false
    try {
      const both = AbortSignal.any([signal, slow.signal])
      for await (const message of this.bff.messages(method, request, both)) {
        received = true
        clearTimeout(watchdog)
        if (signal.aborted) return
        handlers.onMessage(message)
      }
      if (!signal.aborted) handlers.onEnd()
    } catch (e) {
      clearTimeout(watchdog)
      if (signal.aborted) return
      const fallBack = mode === 'auto' && !received && (slow.signal.aborted || transportFailure(e))
      if (!fallBack) {
        if (!isAbort(e)) handlers.onError(toBffError(e))
        return
      }
      handlers.onFallback?.(slow.signal.aborted ? 'no first message over Connect' : toBffError(e).error)
      await this.#sse(method, request, handlers, signal)
    }
  }

  #sse(method: string, request: unknown, handlers: StreamHandlers, signal: AbortSignal) {
    const guarded = <A extends unknown[]>(f: (...a: A) => void) => (...a: A) => {
      if (!signal.aborted) f(...a)
    }
    return sseStream(
      {
        method,
        request,
        signal,
        fetch: this.options.fetch,
        retries: this.options.sseRetries,
        retryDelayMs: this.options.sseRetryDelayMs,
      },
      {
        onMessage: guarded((m: unknown) => handlers.onMessage(m as JsonValue)),
        onEnd: guarded(() => handlers.onEnd()),
        onError: guarded((e: BffError) => handlers.onError(e)),
        onKeepalive: guarded(() => handlers.onKeepalive?.()),
      },
    )
  }
}
