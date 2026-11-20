/**
 * The bundle the console pages import as `/bff.js`: one client of the
 * console's BFF service and one progressive-search lifecycle.
 */
import { createBff } from './facade.ts'
import { ProgressiveSearch } from './progressive.ts'
import { bffTransport } from './transport.ts'

export { BFF_BASE, bffTransport } from './transport.ts'
export { createBff, bffMethod, type Bff } from './facade.ts'
export { ProgressiveSearch, type StreamHandlers } from './progressive.ts'
export { sseStream } from './sse.ts'
export { codeName, toBffError, type BffError } from './errors.ts'

export const bff = createBff(bffTransport())
export const progressive = new ProgressiveSearch(bff)
