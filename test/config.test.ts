import type { Pool } from 'pg'
import { describe, expect, it } from 'vitest'
import { DEFAULTS, resolveConfig, type RekindleOptions } from '../src/config.js'
import { RekindleConfigError } from '../src/errors.js'
import type { MastraLike } from '../src/mastra.js'

// No database: resolving options never touches the pool or Mastra.
const pool = {} as Pool
const mastra: MastraLike = { getWorkflow: () => ({}) as ReturnType<MastraLike['getWorkflow']> }
const resolve = (overrides: Partial<RekindleOptions> = {}) =>
  resolveConfig({ mastra, pool, schema: 'rk_test', ...overrides })

describe('resolveConfig', () => {
  it('applies the defaults', () => {
    const config = resolve()
    expect(config).toMatchObject({
      heartbeatMs: DEFAULTS.heartbeatMs,
      staleMs: DEFAULTS.staleMs,
      guardWindowMs: DEFAULTS.staleMs + DEFAULTS.guardWindowPastStaleMs,
      maxAttempts: DEFAULTS.maxAttempts,
      workflows: undefined,
      setAsDefault: true,
    })
    expect(config.instanceId).toMatch(/^rekindle-/)
  })

  it.each([
    ['heartbeatMs', 0],
    ['heartbeatMs', -1],
    ['heartbeatMs', Number.NaN],
    ['staleMs', Number.POSITIVE_INFINITY],
    ['guardRetryMs', 0],
    ['guardWindowMs', -5],
    ['sweepIntervalMs', 0],
    ['pruneAfterMs', 0],
    ['bootDelayMs', -1],
    ['sweepJitterMs', -1],
    ['bootJitterMs', Number.NaN],
    ['maxAttempts', 0],
    ['maxAttempts', 1.5],
    ['maxRestartsPerSweep', 0],
    ['sweepPageSize', 0],
    ['sweepPageSize', 2.5],
  ])('rejects %s = %s', (name, value) => {
    expect(() => resolve({ [name]: value })).toThrow(RekindleConfigError)
    expect(() => resolve({ [name]: value })).toThrow(name)
  })

  it('accepts zero for delays and jitter', () => {
    expect(() => resolve({ bootDelayMs: 0, bootJitterMs: 0, sweepJitterMs: 0 })).not.toThrow()
  })

  it('needs staleMs above heartbeatMs', () => {
    expect(() => resolve({ heartbeatMs: 5_000, staleMs: 5_000 })).toThrow(/staleMs/)
  })

  it('needs Mastra', () => {
    expect(() => resolveConfig({ pool, schema: 'rk_test' } as RekindleOptions)).toThrow(/mastra/)
  })

  it('refuses a workflows list that is not names', () => {
    expect(() => resolve({ workflows: ['report', ''] })).toThrow(/workflows/)
  })
})
