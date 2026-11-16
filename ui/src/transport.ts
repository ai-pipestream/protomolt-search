/**
 * The console's wire: the Connect protocol with JSON messages to the
 * console's BFF service, same origin as the page. The console holds every
 * cluster credential; the page holds none.
 */
import type { Transport } from '@connectrpc/connect'
import { createConnectTransport } from '@connectrpc/connect-web'

/** The part of `fetch` the client uses. */
export type FetchLike = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>

/** Where the console serves Connect paths, relative to the page origin. */
export const BFF_BASE = '/bff/v1'

/** The browser transport. JSON keeps the wire readable in devtools. */
export function bffTransport(fetchImpl: FetchLike = (input, init) => globalThis.fetch(input, init)): Transport {
  return createConnectTransport({
    baseUrl: BFF_BASE,
    useBinaryFormat: false,
    fetch: ((input: RequestInfo | URL, init?: RequestInit) =>
      fetchImpl(input, { ...init, credentials: 'same-origin' })) as typeof globalThis.fetch,
  })
}
