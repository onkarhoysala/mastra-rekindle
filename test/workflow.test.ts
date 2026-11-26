import { randomUUID } from 'node:crypto'
import { Mastra } from '@mastra/core/mastra'
import { createStep, createWorkflow } from '@mastra/core/workflows'
import { PostgresStore } from '@mastra/pg'
import type pg from 'pg'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { z } from 'zod'
import { rekindleWorkflow, type Rekindle } from '../src/index.js'
import { assembleRekindle } from '../src/rekindle.js'
import { resetRegistry } from '../src/registry.js'
import { ageHeartbeat, dropSchema, freshSchema, makePool, ownerOf, sleep, type TestRekindle } from './helpers.js'

const pool = makePool()
let schema: string

beforeAll(async () => {
  schema = await freshSchema(pool)
})
afterAll(async () => {
  resetRegistry()
  await dropSchema(pool, schema)
  await pool.end()
})

const io = z.object({ topic: z.string() })

/** Mastra creates its tables in the background on construction; wait for that before using it. */
async function mastraWith(workflows: Record<string, ReturnType<typeof createWorkflow>>) {
  const storage = new PostgresStore({ id: `test-${randomUUID().slice(0, 6)}`, pool: pool as pg.Pool, schemaName: schema })
  await storage.init()
  return new Mastra({ workflows, storage, logger: false })
}

/** Pool and schema come from Mastra's PostgresStore, as they would in an app. */
function quickRekindle(
  mastra: Mastra,
  instanceId: string,
  extra: { workflows?: string[]; setAsDefault?: boolean } = {}
): TestRekindle {
  const { rekindle, store, heartbeat } = assembleRekindle({
    mastra,
    instanceId,
    heartbeatMs: 200,
    staleMs: 2_000,
    guardRetryMs: 100,
    setAsDefault: false,
    ...extra,
  })
  return Object.assign(rekindle, { store, heartbeat })
}

async function until(check: () => boolean | Promise<boolean>, ms = 15_000) {
  const deadline = Date.now() + ms
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error('timed out waiting')
    await sleep(50)
  }
}

describe('a real Mastra workflow', () => {
  it('guards with the runId Mastra passes each step, no runId in the input', async () => {
    const ownership = rekindleWorkflow('noRunIdInput')
    const step = createStep({
      id: 'only',
      inputSchema: io,
      outputSchema: io,
      execute: async ({ inputData, runId }) => {
        await ownership.guard(runId)
        return inputData
      },
    })
    const noRunIdInput = createWorkflow({ id: 'noRunIdInput', inputSchema: io, outputSchema: io }).then(step).commit()
    const mastra = await mastraWith({ noRunIdInput })
    const rekindle = quickRekindle(mastra, `inst-${randomUUID().slice(0, 8)}`, { setAsDefault: true })
    await rekindle.migrate()
    await rekindle.heartbeat()

    const run = await mastra.getWorkflow('noRunIdInput').createRun()
    const result = await run.start({ inputData: { topic: 'demo' } })

    expect(result.status).toBe('success')
    expect((await ownerOf(pool, schema, run.runId))?.owner_instance_id).toBe(rekindle.instanceId)
  })

  it('resumes a run whose instance died mid-step, on another instance, from the interrupted step', async () => {
    const log: string[] = []

    // The same workflow as two processes would each build it. On A, step two never finishes.
    function crashRun(instance: string, rk: () => Rekindle, hang: boolean) {
      const first = createStep({
        id: 'first',
        inputSchema: io,
        outputSchema: io,
        execute: async ({ inputData, runId }) => {
          await rk().guard('crashRun', runId)
          log.push(`first@${instance}`)
          return inputData
        },
      })
      const second = createStep({
        id: 'second',
        inputSchema: io,
        outputSchema: io,
        execute: async ({ inputData, runId }) => {
          await rk().guard('crashRun', runId)
          log.push(`second-start@${instance}`)
          if (hang) await new Promise(() => {})
          log.push(`second-end@${instance}`)
          return inputData
        },
      })
      const third = createStep({
        id: 'third',
        inputSchema: io,
        outputSchema: io,
        execute: async ({ inputData, runId }) => {
          await rk().guard('crashRun', runId)
          log.push(`third@${instance}`)
          await rk().clear('crashRun', runId)
          return inputData
        },
      })
      return createWorkflow({ id: 'crashRun', inputSchema: io, outputSchema: io })
        .then(first)
        .then(second)
        .then(third)
        .commit()
    }

    let rkA!: TestRekindle
    const mastraA = await mastraWith({ crashRun: crashRun('A', () => rkA, true) })
    rkA = quickRekindle(mastraA, 'A', { workflows: ['crashRun'] })
    await rkA.migrate()
    await rkA.heartbeat()

    const runA = await mastraA.getWorkflow('crashRun').createRun()
    void runA.start({ inputData: { topic: 'demo' } })
    await until(() => log.includes('second-start@A'))

    // A "dies": no more heartbeats.
    await ageHeartbeat(pool, schema, 'A', 60)

    let rkB!: TestRekindle
    const mastraB = await mastraWith({ crashRun: crashRun('B', () => rkB, false) })
    rkB = quickRekindle(mastraB, 'B', { workflows: ['crashRun'] })
    await rkB.heartbeat()

    const swept = await rkB.sweepOnce()
    expect(swept.restarted).toBe(1)
    await until(() => log.includes('third@B'))

    expect(log).toEqual(['first@A', 'second-start@A', 'second-start@B', 'second-end@B', 'third@B'])
    expect((await ownerOf(pool, schema, runA.runId))?.done_at).toBeInstanceOf(Date)
  })
})
