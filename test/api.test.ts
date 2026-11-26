import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import type { Pool } from 'pg'
import {
  createRekindle,
  OwnershipLostError,
  RekindleConfigError,
  RekindleNotStartedError,
  rekindleWorkflow,
} from '../src/index.js'
import { resetRegistry } from '../src/registry.js'
import { ageHeartbeat, dropSchema, fakeMastra, freshSchema, makePool, makeRekindle, ownerOf, sleep } from './helpers.js'

const pool = makePool()
let schema: string

beforeAll(async () => {
  schema = await freshSchema(pool)
  await makeRekindle(pool, schema).migrate()
})
afterAll(async () => {
  resetRegistry()
  await dropSchema(pool, schema)
  await pool.end()
})

describe('errors', () => {
  it('OwnershipLostError takes the workflow first, like everything else', () => {
    const err = new OwnershipLostError('report', 'run-1', 'done')
    expect(err.workflowName).toBe('report')
    expect(err.runId).toBe('run-1')
    expect(err.message).toBe('report run run-1 was already marked done')
  })

  it('a guard refused by a live owner reports the right workflow and run', async () => {
    const owner = makeRekindle(pool, schema, { guardWindowMs: 300 })
    const other = makeRekindle(pool, schema, { guardWindowMs: 300 })
    await owner.heartbeat()
    await other.heartbeat()
    await owner.guard('report', 'e-live')

    const err = await other.guard('report', 'e-live').catch((e) => e)
    expect(err).toBeInstanceOf(OwnershipLostError)
    expect(err).toMatchObject({ workflowName: 'report', runId: 'e-live', reason: 'owned-elsewhere' })
  })

  it('RekindleNotStartedError keeps its reason', async () => {
    const ownership = rekindleWorkflow('noInstance')
    const err = await ownership.guard('r-1').catch((e) => e)
    expect(err).toBeInstanceOf(RekindleNotStartedError)
    expect(err.reason).toBe('no-instance')
  })

  it('config problems throw RekindleConfigError', () => {
    expect(() => makeRekindle(pool, schema, { heartbeatMs: 0 })).toThrow(RekindleConfigError)
    expect(() => createRekindle({ mastra: fakeMastra({}).mastra })).toThrow(RekindleConfigError)
  })
})

describe('public surface', () => {
  it('keeps the internals off the Rekindle object', () => {
    const rk = createRekindle({ mastra: fakeMastra({}).mastra, pool, schema, setAsDefault: false })
    for (const internal of ['heartbeatOnce', 'claimAbandoned', 'release', 'pruneDeadInstances']) {
      expect(rk).not.toHaveProperty(internal)
    }
  })

  it('start() after stop() refuses, instead of letting go of runs this process may still be running', async () => {
    const rk = makeRekindle(pool, schema, { workflows: [] })
    await rk.start()
    await rk.guard('report', 'restart-after-stop')
    await rk.stop()

    await expect(rk.start()).rejects.toThrow(/stop\(\)/)
    expect((await ownerOf(pool, schema, 'restart-after-stop'))?.owner_instance_id).toBe(rk.instanceId)
  })

  it('stop() during start() wins: no heartbeat loop is left running', async () => {
    const rk = makeRekindle(pool, schema, { workflows: [] })
    const starting = rk.start()
    await rk.stop()
    await starting

    const heartbeatAt = async () =>
      (await pool.query(`SELECT heartbeat_at FROM "${schema}".rekindle_instances WHERE instance_id = $1`, [rk.instanceId]))
        .rows[0]?.heartbeat_at as Date
    const beatAfterStart = await heartbeatAt()
    await sleep(600)
    expect((await heartbeatAt()).getTime()).toBe(beatAfterStart.getTime())
  })
})

describe('failures are logged, not swallowed', () => {
  it('logs a release that fails after a failed restart', async () => {
    rekindleWorkflow('report')
    const dead = makeRekindle(pool, schema)
    await dead.heartbeat()
    await dead.guard('report', 'rel-1')
    await ageHeartbeat(pool, schema, dead.instanceId, 60)

    // A pool whose release statement fails; everything else goes through.
    const failingRelease = {
      query: (text: string, params?: unknown[]) =>
        /SET owner_instance_id = NULL\s+WHERE workflow_name/.test(text)
          ? Promise.reject(new Error('release boom'))
          : pool.query(text, params),
    } as unknown as Pool
    const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() }
    const { mastra } = fakeMastra({ report: ['rel-1'] }, { failRestart: new Set(['rel-1']) })
    const survivor = makeRekindle(failingRelease, schema, { mastra, logger })
    await survivor.heartbeat()

    await survivor.sweepOnce()
    await sleep(100)

    expect(logger.error).toHaveBeenCalledWith('release failed', expect.objectContaining({ runId: 'rel-1', error: 'release boom' }))
  })
})
