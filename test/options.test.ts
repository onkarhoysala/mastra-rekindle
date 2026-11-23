import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { ageHeartbeat, dropSchema, fakeMastra, freshSchema, makePool, makeRekindle } from './helpers.js'

const pool = makePool()
let schema: string

beforeAll(async () => {
  schema = await freshSchema(pool)
  await makeRekindle(pool, schema).migrate()
})
afterAll(async () => {
  await dropSchema(pool, schema)
  await pool.end()
})

function spyLogger() {
  return { info: vi.fn(), warn: vi.fn(), error: vi.fn() }
}

describe('timings are in ms', () => {
  it('staleMs decides when an owner counts as dead', async () => {
    const owner = makeRekindle(pool, schema, { staleMs: 10_000 })
    const other = makeRekindle(pool, schema, { staleMs: 10_000 })
    await owner.heartbeatOnce()
    await other.heartbeatOnce()
    await owner.guard('wf', 'run-stale-ms')

    await ageHeartbeat(pool, schema, owner.instanceId, 5)
    expect(await other.claimAbandoned('wf', 'run-stale-ms', new Date(0))).toBe(false)

    await ageHeartbeat(pool, schema, owner.instanceId, 15)
    expect(await other.claimAbandoned('wf', 'run-stale-ms', new Date(0))).toBe(true)
  })

  it('pruneAfterMs decides when a dead instance row is deleted', async () => {
    const dead = makeRekindle(pool, schema)
    const pruner = makeRekindle(pool, schema, { pruneAfterMs: 5_000 })
    await dead.heartbeatOnce()
    await ageHeartbeat(pool, schema, dead.instanceId, 10)

    expect(await pruner.pruneDeadInstances()).toBeGreaterThanOrEqual(1)
    const { rows } = await pool.query(`SELECT 1 FROM ${schema}.rekindle_instances WHERE instance_id = $1`, [
      dead.instanceId,
    ])
    expect(rows).toHaveLength(0)
  })
})

describe('option validation', () => {
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
    expect(() => makeRekindle(pool, schema, { [name]: value })).toThrow(name)
  })

  it('accepts zero for delays and jitter', () => {
    expect(() =>
      makeRekindle(pool, schema, { bootDelayMs: 0, bootJitterMs: 0, sweepJitterMs: 0 })
    ).not.toThrow()
  })
})

describe('logger', () => {
  it("defaults to Mastra's logger", async () => {
    const mastraLogger = spyLogger()
    const { mastra } = fakeMastra({})
    const rekindle = makeRekindle(pool, schema, { workflows: [] })

    await rekindle.start({ ...mastra, getLogger: () => mastraLogger })
    await rekindle.stop()

    expect(mastraLogger.info).toHaveBeenCalledWith('started', expect.anything())
  })

  it('an explicit logger wins over Mastra\'s', async () => {
    const mastraLogger = spyLogger()
    const mine = spyLogger()
    const { mastra } = fakeMastra({})
    const rekindle = makeRekindle(pool, schema, { workflows: [], logger: mine })

    await rekindle.start({ ...mastra, getLogger: () => mastraLogger })
    await rekindle.stop()

    expect(mine.info).toHaveBeenCalledWith('started', expect.anything())
    expect(mastraLogger.info).not.toHaveBeenCalled()
  })
})
