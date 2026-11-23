/**
 * One Mastra instance with rekindle switched on.
 *
 *   node --import tsx examples/basic/server.ts --name A --start-run
 *   node --import tsx examples/basic/server.ts --name B
 *
 * Both talk to the same Postgres (DATABASE_URL). Timings are shortened so the demo takes
 * seconds: heartbeat 1s, dead after 4s, sweep every 3s.
 */
import { randomUUID } from 'node:crypto'
import { Mastra } from '@mastra/core/mastra'
import { PostgresStore } from '@mastra/pg'
import pg from 'pg'
import { consoleLogger, createRekindle } from '../../src/index.js'
import { configureDemoWorkflow, DEMO_SCHEMA, demoRun } from './workflow.js'

const args = process.argv.slice(2)
const name = args[args.indexOf('--name') + 1] ?? 'X'
const startRun = args.includes('--start-run')

const connectionString = process.env.DATABASE_URL
if (!connectionString) throw new Error('DATABASE_URL is required')

const pool = new pg.Pool({ connectionString, max: 8 })

const mastra = new Mastra({
  workflows: { demoRun },
  storage: new PostgresStore({ id: 'demo', pool, schemaName: DEMO_SCHEMA }),
  // Before 1.84, Mastra's server restarts EVERY active run on boot with no lock, so two
  // instances race for the same run. Off: rekindle decides who resumes what.
  recovery: { workflows: 'off' },
  logger: false,
})

configureDemoWorkflow(pool, name)

const rekindle = createRekindle({
  mastra, // pool and schema come from Mastra's PostgresStore
  instanceId: `${name}-${randomUUID().slice(0, 6)}`, // unique per process, readable in logs
  heartbeatMs: 1_000,
  staleMs: 4_000,
  guardRetryMs: 500,
  bootDelayMs: 2_000,
  bootJitterMs: 0,
  sweepIntervalMs: 3_000,
  sweepJitterMs: 0,
  logger: consoleLogger, // Mastra's own logger is off above to keep the demo output short
})

await pool.query(`CREATE SCHEMA IF NOT EXISTS ${DEMO_SCHEMA}`)
await pool.query(`CREATE TABLE IF NOT EXISTS ${DEMO_SCHEMA}.step_log (
  run_id text, step text, event text, instance text, at timestamptz DEFAULT now())`)
await rekindle.migrate()
await rekindle.start()
rekindle.installSignalHandlers()
console.log(`[${name}] up as ${rekindle.instanceId}`)

if (startRun) {
  const runId = randomUUID()
  const run = await mastra.getWorkflow('demoRun').createRun({ runId })
  console.log(`[${name}] RUN_STARTED ${runId}`)
  void run.start({ inputData: { topic: 'demo' } }).then((r) => console.log(`[${name}] run finished: ${r.status}`))
}

// Stay alive; the orchestrator kills us.
setInterval(() => {}, 60_000)
