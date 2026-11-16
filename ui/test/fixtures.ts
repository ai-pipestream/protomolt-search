/** Router transports and scripted fetches the client tests share. */
import { create } from '@bufbuild/protobuf'
import { createRouterTransport, type ServiceImpl } from '@connectrpc/connect'
import { ConsoleBffService } from '../src/gen/ai/protomolt/console/bff/v1/console_bff_pb.ts'
import {
  QueryHitSchema,
  QueryResponseSchema,
  QueryStreamResponseSchema,
  QueryStreamPhase,
} from '../src/gen/ai/protomolt/search/v1/search_pb.ts'
import type { FetchLike } from '../src/transport.ts'

export type Impl = Partial<ServiceImpl<typeof ConsoleBffService>>

export const routed = (impl: Impl) =>
  createRouterTransport(({ service }) => {
    service(ConsoleBffService, impl)
  })

export const hit = (docId: bigint, score: number) => create(QueryHitSchema, { docId, score, rank: 1 })

export const response = (...hits: ReturnType<typeof hit>[]) => create(QueryResponseSchema, { requestId: 'r1', hits })

export const revision = (n: bigint) =>
  create(QueryStreamResponseSchema, {
    payload: { case: 'revision', value: { revision: n, phase: QueryStreamPhase.UNSPECIFIED, hits: [] } },
  })

export const completion = (n: bigint) =>
  create(QueryStreamResponseSchema, {
    payload: { case: 'completion', value: { completed: true, finalRevision: n, response: response() } },
  })

/** A response whose body is `chunks`, delivered one read at a time. */
export function streamed(chunks: string[], status = 200): Response {
  const encoder = new TextEncoder()
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk))
      controller.close()
    },
  })
  return new Response(body, { status, headers: { 'content-type': 'text/event-stream' } })
}

/** A fetch answering each call with the next scripted response, recording the calls. */
export function scripted(responses: (() => Response | Promise<Response>)[]) {
  const calls: { url: string; init?: RequestInit }[] = []
  const fetch: FetchLike = async (input, init) => {
    calls.push({ url: String(input), init })
    const next = responses[calls.length - 1]
    if (!next) throw new TypeError('no scripted response left')
    return next()
  }
  return { fetch, calls }
}

/** Resolves when `predicate` holds, polling each macrotask. */
export async function until(predicate: () => boolean, ms = 2000): Promise<void> {
  const deadline = Date.now() + ms
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('condition not met in time')
    await new Promise((r) => setTimeout(r, 1))
  }
}
