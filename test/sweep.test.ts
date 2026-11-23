import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest'
import { rekindleWorkflow } from '../src/index.js'
import { resetRegistry } from '../src/registry.js'
import { ageHeartbeat, dropSchema, fakeMastra, freshSchema, makePool, makeRekindle, ownerOf, sleep } from './helpers.js'

const pool = makePool()
let schema: string

beforeAll(async () => {
  schema = await freshSchema(pool)
  await makeRekindle(pool, schema).migrate()
})
afterEach(() => resetRegistry())
afterAll(async () => {
  await dropSchema(pool, schema)
  await pool.end()
})

/** An instance that owned `runIds` and then died. */
async function deadOwnerOf(runIds: string[], workflow = 'report') {
  const dead = makeRekindle(pool, schema)
  await dead.heartbeatOnce()
  for (const id of runIds) await dead.guard(workflow, id)
  await ageHeartbeat(pool, schema, dead.instanceId, 60)
  return dead
}

describe('sweepOnce', () => {
  it('restarts abandoned runs of opted-in workflows', async () => {
    rekindleWorkflow('report')
    await deadOwnerOf(['s-1', 's-2'])
    const { mastra, restarted } = fakeMastra({ report: ['s-1', 's-2'] })
    const survivor = makeRekindle(pool, schema)
    await survivor.heartbeatOnce()

    const result = await survivor.sweepOnce(mastra)

    expect(result.restarted).toBe(2)
    expect(restarted.map((r) => r.runId).sort()).toEqual(['s-1', 's-2'])
    expect((await ownerOf(pool, schema, 's-1'))?.owner_instance_id).toBe(survivor.instanceId)
  })

  it('also restarts runs that were waiting (eg in a sleep step)', async () => {
    rekindleWorkflow('report')
    await deadOwnerOf(['w-1'])
    const { mastra, restarted } = fakeMastra({ report: [{ runId: 'w-1', status: 'waiting' }] })
    const survivor = makeRekindle(pool, schema)
    await survivor.heartbeatOnce()

    await survivor.sweepOnce(mastra)

    expect(restarted.map((r) => r.runId)).toEqual(['w-1'])
  })

  it('pages through the run list instead of loading it all at once', async () => {
    rekindleWorkflow('report')
    const ids = ['p-1', 'p-2', 'p-3', 'p-4', 'p-5']
    await deadOwnerOf(ids)
    const { mastra, restarted, listCalls } = fakeMastra({ report: ids })
    const survivor = makeRekindle(pool, schema, { sweepPageSize: 2 })
    await survivor.heartbeatOnce()

    await survivor.sweepOnce(mastra)

    expect(restarted.map((r) => r.runId).sort()).toEqual(ids)
    expect(listCalls.length).toBeGreaterThan(0)
    for (const call of listCalls) {
      expect(call.perPage).toBe(2)
      expect(typeof call.page).toBe('number')
    }
  })

  it('never touches workflows that did not opt in', async () => {
    rekindleWorkflow('report')
    const { mastra, restarted } = fakeMastra({ report: [], oneShot: ['o-1'] })
    const survivor = makeRekindle(pool, schema)
    await survivor.heartbeatOnce()

    await survivor.sweepOnce(mastra)

    expect(restarted).toEqual([])
    expect(await ownerOf(pool, schema, 'o-1')).toBeUndefined()
  })

  it('leaves runs owned by a live instance alone', async () => {
    rekindleWorkflow('report')
    const live = makeRekindle(pool, schema)
    await live.heartbeatOnce()
    await live.guard('report', 'l-1')
    const { mastra, restarted } = fakeMastra({ report: ['l-1'] })
    const other = makeRekindle(pool, schema)
    await other.heartbeatOnce()

    await other.sweepOnce(mastra)

    expect(restarted).toEqual([])
    expect((await ownerOf(pool, schema, 'l-1'))?.owner_instance_id).toBe(live.instanceId)
  })

  it('leaves an ownerless run alone while Mastra is still updating it', async () => {
    rekindleWorkflow('report')
    const { mastra, restarted } = fakeMastra({ report: [{ runId: 'n-1', updatedAt: new Date() }] })
    const survivor = makeRekindle(pool, schema)
    await survivor.heartbeatOnce()

    await survivor.sweepOnce(mastra)

    expect(restarted).toEqual([])
  })

  it('caps restarts per sweep across all workflows', async () => {
    rekindleWorkflow('report')
    rekindleWorkflow('import')
    const reportRuns = ['c-1', 'c-2', 'c-3']
    const importRuns = ['c-4', 'c-5', 'c-6']
    await deadOwnerOf(reportRuns, 'report')
    await deadOwnerOf(importRuns, 'import')
    const { mastra, restarted } = fakeMastra({ report: reportRuns, import: importRuns })
    const survivor = makeRekindle(pool, schema, { maxRestartsPerSweep: 4 })
    await survivor.heartbeatOnce()

    const first = await survivor.sweepOnce(mastra)
    expect(first.restarted).toBe(4)
    expect(restarted).toHaveLength(4)

    const second = await survivor.sweepOnce(mastra)
    expect(second.restarted).toBe(2)
    expect(restarted).toHaveLength(6)
  })

  it('releases a run whose restart fails, so the next sweep tries again', async () => {
    rekindleWorkflow('report')
    await deadOwnerOf(['f-1'])
    const { mastra } = fakeMastra({ report: ['f-1'] }, { failRestart: new Set(['f-1']) })
    const survivor = makeRekindle(pool, schema)
    await survivor.heartbeatOnce()

    await survivor.sweepOnce(mastra)
    await sleep(100) // restart is fire-and-forget; let its rejection land

    expect((await ownerOf(pool, schema, 'f-1'))?.owner_instance_id).toBeNull()
  })
})

describe('lifecycle', () => {
  it('start() heartbeats and sweeps; drain() stops sweeping but keeps the heartbeat', async () => {
    rekindleWorkflow('report')
    const runs: string[] = []
    const { mastra, restarted, listCalls } = fakeMastra({ report: runs })
    const rk = makeRekindle(pool, schema, { sweepIntervalMs: 300 })

    await rk.start(mastra)
    await sleep(100)
    const beat1 = await heartbeatAt(rk.instanceId)
    expect(beat1).toBeInstanceOf(Date)
    expect(listCalls.length).toBeGreaterThan(0) // the boot sweep ran

    rk.drain()
    expect(rk.isDraining).toBe(true)
    // A run abandoned after drain, and listed by Mastra, must not be picked up by this
    // (draining) instance, though its sweep interval passes twice.
    await deadOwnerOf(['d-1'])
    runs.push('d-1')
    await sleep(700)
    expect(restarted).toEqual([])
    const beat2 = await heartbeatAt(rk.instanceId)
    expect(beat2!.getTime()).toBeGreaterThan(beat1!.getTime())

    await rk.stop()
  })

  it('start() lets go of runs held under the same instanceId by a previous boot', async () => {
    const before = makeRekindle(pool, schema, { instanceId: 'pod-0' })
    await before.heartbeatOnce()
    await before.guard('report', 'reboot-1')

    const after = makeRekindle(pool, schema, { instanceId: 'pod-0', workflows: [] })
    await after.start(fakeMastra({}).mastra)
    await after.stop()

    expect((await ownerOf(pool, schema, 'reboot-1'))?.owner_instance_id).toBeNull()
  })

  it('start() twice does not leave a second heartbeat or sweep loop running after stop()', async () => {
    const { mastra, listCalls } = fakeMastra({ report: [] })
    const rk = makeRekindle(pool, schema, { workflows: ['report'], sweepIntervalMs: 100 })

    await rk.start(mastra)
    await rk.start(mastra)
    await sleep(250)
    await rk.stop()
    const beatAtStop = await heartbeatAt(rk.instanceId)
    const listsAtStop = listCalls.length

    await sleep(600)

    // A leaked loop from the second start() would keep beating and sweeping after stop().
    expect((await heartbeatAt(rk.instanceId))!.getTime()).toBe(beatAtStop!.getTime())
    expect(listCalls.length).toBe(listsAtStop)
  })

  it('warns once when a run has used up its attempts, and leaves it alone', async () => {
    rekindleWorkflow('report')
    await deadOwnerOf(['m-1'])
    await pool.query(`UPDATE "${schema}".rekindle_run_owners SET attempts = 3 WHERE run_id = 'm-1'`)
    const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() }
    const { mastra, restarted } = fakeMastra({ report: ['m-1'] })
    const survivor = makeRekindle(pool, schema, { logger })
    await survivor.heartbeatOnce()

    await survivor.sweepOnce(mastra)
    await survivor.sweepOnce(mastra)

    expect(restarted).toEqual([])
    const warnings = logger.warn.mock.calls.filter(([, meta]) => meta?.runId === 'm-1')
    expect(warnings).toHaveLength(1)
    expect(warnings[0][1]).toMatchObject({ workflow: 'report', runId: 'm-1', attempts: 3 })
  })

  it('installSignalHandlers() drains, then lets the signal exit the process', async () => {
    const kill = vi.spyOn(process, 'kill').mockImplementation(() => true)
    const rk = makeRekindle(pool, schema, { workflows: [] })
    await rk.start(fakeMastra({}).mastra)
    rk.installSignalHandlers({ signals: ['SIGUSR2'] })
    try {
      process.emit('SIGUSR2', 'SIGUSR2')
      await sleep(50)
      expect(rk.isDraining).toBe(true)
      expect(kill).toHaveBeenCalledWith(process.pid, 'SIGUSR2')
    } finally {
      kill.mockRestore()
      await rk.stop()
    }
  })

  it('start() can be retried after a failed first attempt', async () => {
    const unmigrated = `rk_unmigrated_${Date.now()}`
    const rk = makeRekindle(pool, unmigrated, { workflows: [] })
    try {
      await expect(rk.start(fakeMastra({}).mastra)).rejects.toThrow()

      await rk.migrate()
      await rk.start(fakeMastra({}).mastra)

      const { rows } = await pool.query(`SELECT 1 FROM "${unmigrated}".rekindle_instances WHERE instance_id = $1`, [
        rk.instanceId,
      ])
      expect(rows).toHaveLength(1)
    } finally {
      await rk.stop()
      await dropSchema(pool, unmigrated)
    }
  })

  it('installSignalHandlers() leaves the exit to your own handler and keeps heartbeating', async () => {
    const kill = vi.spyOn(process, 'kill').mockImplementation(() => true)
    let shutdownRan = false
    process.once('SIGUSR2', () => {
      shutdownRan = true
    })
    const rk = makeRekindle(pool, schema, { workflows: [] })
    await rk.start(fakeMastra({}).mastra)
    rk.installSignalHandlers({ signals: ['SIGUSR2'] })
    try {
      process.emit('SIGUSR2', 'SIGUSR2')
      const beat1 = await heartbeatAt(rk.instanceId)
      await sleep(600)
      const beat2 = await heartbeatAt(rk.instanceId)

      expect(shutdownRan).toBe(true)
      expect(rk.isDraining).toBe(true)
      expect(kill).not.toHaveBeenCalled()
      expect(beat2!.getTime()).toBeGreaterThan(beat1!.getTime())
    } finally {
      kill.mockRestore()
      await rk.stop()
    }
  })

  it('installSignalHandlers({ exit: false }) only drains', async () => {
    const kill = vi.spyOn(process, 'kill').mockImplementation(() => true)
    const rk = makeRekindle(pool, schema, { workflows: [] })
    await rk.start(fakeMastra({}).mastra)
    rk.installSignalHandlers({ signals: ['SIGUSR2'], exit: false })
    try {
      process.emit('SIGUSR2', 'SIGUSR2')
      await sleep(50)
      expect(rk.isDraining).toBe(true)
      expect(kill).not.toHaveBeenCalled()
    } finally {
      kill.mockRestore()
      await rk.stop()
    }
  })
})

async function heartbeatAt(instanceId: string): Promise<Date | undefined> {
  const { rows } = await pool.query(
    `SELECT heartbeat_at FROM "${schema}".rekindle_instances WHERE instance_id = $1`,
    [instanceId]
  )
  return rows[0]?.heartbeat_at
}
