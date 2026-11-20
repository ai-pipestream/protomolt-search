/**
 * Errors as the console pages render them: `{ error, code, http? }` with
 * the code in the console's UPPER_SNAKE spelling.
 */
import { Code, ConnectError } from '@connectrpc/connect'

export interface BffError {
  error: string
  code: string
  http?: number
}

/** `InvalidArgument` -> `INVALID_ARGUMENT`. */
export function codeName(code: Code): string {
  if (code === Code.Canceled) return 'CANCELLED'
  return Code[code].replace(/([a-z])([A-Z])/g, '$1_$2').toUpperCase()
}

/** Any thrown value as a {@link BffError}. */
export function toBffError(e: unknown): BffError {
  if (e instanceof ConnectError) return { error: e.rawMessage, code: codeName(e.code) }
  if (e && typeof e === 'object' && 'error' in e && 'code' in e) return e as BffError
  const message = e instanceof Error ? e.message : String(e)
  return { error: message, code: 'CLIENT' }
}

/** Whether an error is the cancellation this client asked for. */
export function isAbort(e: unknown, signal?: AbortSignal): boolean {
  if (signal?.aborted) return true
  if (e instanceof ConnectError) return e.code === Code.Canceled
  return e instanceof Error && e.name === 'AbortError'
}
