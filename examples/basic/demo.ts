/**
 * The kill -9 demo.
 *
 *   1. Instance A starts a three-step run.
 *   2. While A is in the middle of the slow step, it is killed with SIGKILL (no cleanup at all).
 *   3. Instance B, sharing the same Postgres, notices A stopped heartbeating, claims the run and
 *      calls Mastra's restart(). The run resumes at the slow step: gather is not run again.
 *
 *   DATABASE_URL=postgres://... npx tsx examples/basic/demo.ts
 */
import { spawn, type ChildProcess } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import pg from 'pg'
import { DEMO_SCHEMA } from './workflow.js'

const connectionString = process.env.DATABASE_URL
if (!connectionString) throw new Error('DATABASE_URL is required')
const pool = new pg.Pool({ connectionString })
const server = fileURLToPath(new URL('./server.ts', import.meta.url))
const t0 = Date.now()
const at = () => `${((Date.now() - t0) / 1000).toFixed(1)}s`

function startInstance(name: string, extra: string[] = []): { child: ChildProcess; lines: string[] } {
  const lines: string[] = []
  const child = spawn(process.execPath, ['--import', 'tsx', server, '--name', name, ...extra], {
    env: process.env,
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  const onData = (buf: Buffer) => {
    for (const line of buf.toString().split('\n').filter(Boolean)) {
      lines.push(line)
      console.log(`${at().padStart(6)}  ${line}`)
    }
  }
  child.stdout!.on('data', onData)
  child.stderr!.on('data', onData)
  return { child, lines }
}

async function waitFor(check: () => boolean | Promise<boolean>, what: string, timeoutMs = 60_000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await check()) return
    await new Promise((r) => setTimeout(r, 200))
  }
  throw new Error(`timed out waiting for: ${what}`)
}

await pool.query(`DROP SCHEMA IF EXISTS ${DEMO_SCHEMA} CASCADE`)

console.log(`${at().padStart(6)}  --- starting instance A with a run`)
const a = startInstance('A', ['--start-run'])
await waitFor(() => a.lines.some((l) => l.includes('slow ... 3/10')), 'A to be 3s into the slow step')

console.log(`${at().padStart(6)}  --- kill -9 A (mid step "slow")`)
a.child.kill('SIGKILL')
const runId = a.lines.find((l) => l.includes('RUN_STARTED'))!.split('RUN_STARTED ')[1].trim()

console.log(`${at().padStart(6)}  --- starting instance B`)
const b = startInstance('B')

await waitFor(
  async () =>
    (await pool.query(
      `SELECT 1 FROM ${DEMO_SCHEMA}.step_log WHERE run_id = $1 AND step = 'publish' AND event = 'end'`,
      [runId]
    )).rowCount! > 0,
  'the run to finish on B',
  90_000
)
await new Promise((r) => setTimeout(r, 500))
b.child.kill('SIGTERM')

const { rows } = await pool.query(
  `SELECT step, event, instance FROM ${DEMO_SCHEMA}.step_log WHERE run_id = $1 ORDER BY at`,
  [runId]
)
console.log('\nstep log for run', runId)
for (const r of rows) console.log(`  ${r.step.padEnd(8)} ${r.event.padEnd(6)} on ${r.instance}`)

const count = (step: string, event: string) => rows.filter((r) => r.step === step && r.event === event).length
const ok =
  count('gather', 'start') === 1 && // completed before the kill: not re-run
  count('slow', 'start') === 2 && // interrupted: re-run from its start on B
  count('slow', 'end') === 1 &&
  count('publish', 'end') === 1 &&
  rows.filter((r) => r.step !== 'gather').at(-1)?.instance.startsWith('B')

console.log(ok ? '\nPASS: resumed on B at the interrupted step, gather not re-run' : '\nFAIL: see step log above')
await pool.end()
process.exit(ok ? 0 : 1)
