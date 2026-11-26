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
    await owner.heartbeat()
    await other.heartbeat()
    await owner.guard('wf', 'run-stale-ms')

    await ageHeartbeat(pool, schema, owner.instanceId, 5)
    expect(await other.store.claim('wf', 'run-stale-ms', new Date(0))).toBe(false)

    await ageHeartbeat(pool, schema, owner.instanceId, 15)
    expect(await other.store.claim('wf', 'run-stale-ms', new Date(0))).toBe(true)
  })

  it('pruneAfterMs decides when a dead instance row is deleted', async () => {
    const dead = makeRekindle(pool, schema)
    const pruner = makeRekindle(pool, schema, { pruneAfterMs: 5_000 })
    await dead.heartbeat()
    await ageHeartbeat(pool, schema, dead.instanceId, 10)

    expect(await pruner.store.pruneExpired()).toBeGreaterThanOrEqual(1)
    const { rows } = await pool.query(`SELECT 1 FROM ${schema}.rekindle_instances WHERE instance_id = $1`, [
      dead.instanceId,
    ])
    expect(rows).toHaveLength(0)
  })
})

describe('logger', () => {
  it("defaults to Mastra's logger", async () => {
    const mastraLogger = spyLogger()
    const { mastra } = fakeMastra({})
    const rekindle = makeRekindle(pool, schema, { workflows: [], mastra: { ...mastra, getLogger: () => mastraLogger } })

    await rekindle.start()
    await rekindle.stop()

    expect(mastraLogger.info).toHaveBeenCalledWith('started', expect.anything())
  })

  it('an explicit logger wins over Mastra\'s', async () => {
    const mastraLogger = spyLogger()
    const mine = spyLogger()
    const { mastra } = fakeMastra({})
    const rekindle = makeRekindle(pool, schema, {
      workflows: [],
      logger: mine,
      mastra: { ...mastra, getLogger: () => mastraLogger },
    })

    await rekindle.start()
    await rekindle.stop()

    expect(mine.info).toHaveBeenCalledWith('started', expect.anything())
    expect(mastraLogger.info).not.toHaveBeenCalled()
  })
})
