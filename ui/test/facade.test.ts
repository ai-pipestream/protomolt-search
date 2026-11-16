import { describe, expect, test } from 'bun:test'
import { Code, ConnectError } from '@connectrpc/connect'
import { createBff } from '../src/facade.ts'
import { codeName, toBffError } from '../src/errors.ts'
import { hit, response, routed } from './fixtures.ts'

describe('unary BFF calls', () => {
  test('a Query goes out as the typed request and comes back as proto3 JSON', async () => {
    let seen: unknown
    const bff = createBff(
      routed({
        query(request) {
          seen = request.selectionK
          return response(hit(42n, 1.5), hit(9007199254740993n, 0))
        },
      }),
    )
    const json = (await bff.call('Query', { selection_k: 7 })) as Record<string, unknown>
    expect(seen).toBe(7)
    const hits = json.hits as Record<string, unknown>[]
    expect(hits.map((h) => h.docId)).toEqual(['42', '9007199254740993'])
    // Defaults render, as the explorer always showed them.
    expect(hits[1]?.score).toBe(0)
    expect(json.nextCursor).toBe('')
  })

  test('request JSON in either field spelling decodes; an unknown field is refused', async () => {
    const ks: number[] = []
    const bff = createBff(routed({ query: (r) => (ks.push(r.k), response()) }))
    await bff.call('Query', { k: 3, selection_k: 1 })
    await bff.call('Query', { k: 4, selectionK: 1 })
    expect(ks).toEqual([3, 4])
    await expect(bff.call('Query', { nope: 1 })).rejects.toMatchObject({ code: 'INVALID_ARGUMENT' })
  })

  test('a Connect error maps to the console error shape', async () => {
    const bff = createBff(
      routed({
        query() {
          throw new ConnectError('field "x" is not indexed', Code.InvalidArgument)
        },
      }),
    )
    await expect(bff.call('Query', {})).rejects.toEqual({
      error: 'field "x" is not indexed',
      code: 'INVALID_ARGUMENT',
    })
  })

  test('an unserved method, or the wrong kind, never reaches the transport', async () => {
    const bff = createBff(routed({}))
    await expect(bff.call('IngestDocuments', {})).rejects.toMatchObject({ code: 'NOT_FOUND' })
    await expect(bff.call('QueryStream', {})).rejects.toMatchObject({ code: 'INVALID_ARGUMENT' })
  })

  test('code names follow the console spelling', () => {
    expect(codeName(Code.FailedPrecondition)).toBe('FAILED_PRECONDITION')
    expect(codeName(Code.Canceled)).toBe('CANCELLED')
    expect(toBffError(new TypeError('Failed to fetch'))).toEqual({ error: 'Failed to fetch', code: 'CLIENT' })
  })
})
