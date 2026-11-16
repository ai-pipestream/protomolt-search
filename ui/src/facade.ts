/**
 * The pages' view of the BFF: proto3 JSON in, proto3 JSON out, over the
 * typed Connect client. The search page builds requests as JSON (its
 * state round-trips through the URL), so the facade converts at the edge
 * and renders every field, defaults included, as the explorer always has.
 */
import { type DescMethod, fromJson, type JsonValue, toJson } from '@bufbuild/protobuf'
import { createClient, type CallOptions, type Transport } from '@connectrpc/connect'
import { toBffError } from './errors.ts'
import { ConsoleBffService } from './gen/ai/protomolt/console/bff/v1/console_bff_pb.ts'

type Call = (request: unknown, options?: CallOptions) => unknown

export interface Bff {
  /** One unary method by its proto name, e.g. `Query`. */
  call(method: string, request?: unknown, signal?: AbortSignal): Promise<JsonValue>
  /** A server-streaming method's responses by its proto name. */
  messages(method: string, request: unknown, signal: AbortSignal): AsyncIterable<JsonValue>
}

/** The served method named `name`, of the kind the caller expects. */
export function bffMethod(name: string, kind: 'unary' | 'server_streaming'): DescMethod {
  const method = ConsoleBffService.methods.find((m) => m.name === name)
  if (!method) throw { error: `ConsoleBffService has no method ${name}`, code: 'NOT_FOUND' }
  if (method.methodKind !== kind) {
    throw { error: `${name} is ${method.methodKind}, not ${kind}`, code: 'INVALID_ARGUMENT' }
  }
  return method
}

const render = { alwaysEmitImplicit: true } as const

export function createBff(transport: Transport): Bff {
  const client = createClient(ConsoleBffService, transport) as unknown as Record<string, Call>
  const invoke = (method: DescMethod, request: unknown, signal?: AbortSignal) => {
    let input
    try {
      input = fromJson(method.input, (request ?? {}) as JsonValue)
    } catch (e) {
      throw { error: e instanceof Error ? e.message : String(e), code: 'INVALID_ARGUMENT' }
    }
    const call = client[method.localName]
    if (!call) throw { error: `no client method for ${method.name}`, code: 'INTERNAL' }
    return call(input, { signal })
  }
  return {
    async call(name, request, signal) {
      const method = bffMethod(name, 'unary')
      try {
        const response = await (invoke(method, request, signal) as Promise<never>)
        return toJson(method.output, response, render)
      } catch (e) {
        throw toBffError(e)
      }
    },
    async *messages(name, request, signal) {
      const method = bffMethod(name, 'server_streaming')
      for await (const response of invoke(method, request, signal) as AsyncIterable<never>) {
        yield toJson(method.output, response, render)
      }
    },
  }
}
