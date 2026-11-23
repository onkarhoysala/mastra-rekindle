import { createStep, createWorkflow } from '@mastra/core/workflows'
import type pg from 'pg'
import { z } from 'zod'
import { rekindleWorkflow } from '../../src/index.js'

/**
 * A three-step workflow that opts in to rekindle. The slow middle step is where the demo
 * kills the instance running it.
 *
 * Every step guards first with the runId Mastra passes it, and is safe to re-run (each step
 * only appends to a log, so a rerun shows up as a second "start" row, which is the point).
 */

export const DEMO_SCHEMA = 'rekindle_demo'

const ownership = rekindleWorkflow('demoRun')

const io = z.object({ topic: z.string() })

let logPool: pg.Pool | null = null
let instanceName = 'unknown'

export function configureDemoWorkflow(pool: pg.Pool, name: string) {
  logPool = pool
  instanceName = name
}

async function record(runId: string, step: string, event: 'start' | 'end') {
  console.log(`[${instanceName}] step ${step} ${event} (run ${runId.slice(0, 8)})`)
  await logPool!.query(
    `INSERT INTO ${DEMO_SCHEMA}.step_log (run_id, step, event, instance) VALUES ($1, $2, $3, $4)`,
    [runId, step, event, instanceName]
  )
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

const gather = createStep({
  id: 'gather',
  inputSchema: io,
  outputSchema: io,
  execute: async ({ inputData, runId }) => {
    await ownership.guard(runId)
    await record(runId, 'gather', 'start')
    await sleep(2_000)
    await record(runId, 'gather', 'end')
    return inputData
  },
})

const slow = createStep({
  id: 'slow',
  inputSchema: io,
  outputSchema: io,
  execute: async ({ inputData, runId }) => {
    await ownership.guard(runId)
    await record(runId, 'slow', 'start')
    for (let i = 1; i <= 10; i++) {
      await sleep(1_000)
      console.log(`[${instanceName}]   slow ... ${i}/10`)
    }
    await record(runId, 'slow', 'end')
    return inputData
  },
})

const publish = createStep({
  id: 'publish',
  inputSchema: io,
  outputSchema: io,
  execute: async ({ inputData, runId }) => {
    await ownership.guard(runId)
    await record(runId, 'publish', 'start')
    await sleep(1_000)
    await record(runId, 'publish', 'end')
    // Last step: the run is done, drop its owner row.
    await ownership.clear(runId)
    return inputData
  },
})

export const demoRun = createWorkflow({
  id: 'demoRun',
  inputSchema: io,
  outputSchema: io,
})
  .then(gather)
  .then(slow)
  .then(publish)
  .commit()
