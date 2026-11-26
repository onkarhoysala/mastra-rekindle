import { randomUUID } from 'node:crypto'
import pg from 'pg'
import type { MastraLike, Rekindle, RekindleOptions } from '../src/index.js'
import { assembleRekindle } from '../src/rekindle.js'
import type { OwnerStore } from '../src/store.js'

const url = process.env.TEST_DATABASE_URL
if (!url) {
  throw new Error(
    'TEST_DATABASE_URL is not set. Run `npm run db:start` and export ' +
      'TEST_DATABASE_URL=postgres://postgres@localhost:54329/postgres'
  )
}

export function makePool(): pg.Pool {
  return new pg.Pool({ connectionString: url, max: 6 })
}

/** A schema of its own per test file, so files never see each other's rows. */
export async function freshSchema(pool: pg.Pool): Promise<string> {
  const schema = `rk_${randomUUID().replace(/-/g, '').slice(0, 12)}`
  await pool.query(`CREATE SCHEMA ${schema}`)
  return schema
}

export async function dropSchema(pool: pg.Pool, schema: string): Promise<void> {
  await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
}

/** A Mastra with no workflows, for instances that never sweep. */
export const noWorkflowsMastra: MastraLike = {
  getWorkflow(name) {
    throw new Error(`workflow ${name} not registered`)
  },
}

/** A Rekindle with its heartbeat and store in reach, so tests can look alive and claim without start(). */
export type TestRekindle = Rekindle & { store: OwnerStore; heartbeat: () => Promise<void> }

/** Short timings so staleness tests take seconds, not minutes. */
export function makeRekindle(pool: pg.Pool, schema: string, overrides: Partial<RekindleOptions> = {}): TestRekindle {
  const { rekindle, store, heartbeat } = assembleRekindle({
    mastra: noWorkflowsMastra,
    pool,
    schema,
    instanceId: `inst-${randomUUID().slice(0, 8)}`,
    heartbeatMs: 200,
    staleMs: 2_000,
    guardRetryMs: 100,
    bootDelayMs: 0,
    bootJitterMs: 0,
    sweepIntervalMs: 60_000,
    sweepJitterMs: 0,
    setAsDefault: false,
    ...overrides,
  })
  // Assigned onto the Rekindle object itself, so its isDraining getter keeps working.
  return Object.assign(rekindle, { store, heartbeat })
}

/** Make an instance look dead without waiting: push its heartbeat into the past. */
export async function ageHeartbeat(pool: pg.Pool, schema: string, instanceId: string, seconds: number) {
  await pool.query(
    `UPDATE "${schema}".rekindle_instances SET heartbeat_at = now() - make_interval(secs => $2) WHERE instance_id = $1`,
    [instanceId, seconds]
  )
}

export interface OwnerRow {
  owner_instance_id: string | null
  restarted_at: Date | null
  attempts: number
  done_at: Date | null
}

export async function ownerOf(pool: pg.Pool, schema: string, runId: string, workflow?: string) {
  const { rows } = await pool.query(
    `SELECT owner_instance_id, restarted_at, attempts, done_at FROM "${schema}".rekindle_run_owners
     WHERE run_id = $1 AND ($2::text IS NULL OR workflow_name = $2)`,
    [runId, workflow ?? null]
  )
  return rows[0] as OwnerRow | undefined
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

const longAgo = () => new Date(Date.now() - 3_600_000)

type FakeRun = string | { runId: string; status?: 'running' | 'waiting'; updatedAt?: Date }

/**
 * A stand-in for the slice of Mastra the sweep touches. Mirrors @mastra/pg: listWorkflowRuns
 * filters by one status and only paginates when both `perPage` and `page` are numbers.
 * Runs default to `running`, last updated an hour ago.
 */
export function fakeMastra(runsByWorkflow: Record<string, FakeRun[]>, opts: { failRestart?: Set<string> } = {}) {
  const restarted: Array<{ workflow: string; runId: string }> = []
  const listCalls: Array<{ workflow: string; status?: string; perPage?: number | false; page?: number }> = []
  const mastra = {
    getWorkflow(name: string) {
      if (!(name in runsByWorkflow)) throw new Error(`workflow ${name} not registered`)
      const all = runsByWorkflow[name].map((r) =>
        typeof r === 'string' ? { runId: r, status: 'running', updatedAt: longAgo() } : { status: 'running', updatedAt: longAgo(), ...r }
      )
      return {
        async listWorkflowRuns(args: { status?: string; perPage?: number | false; page?: number } = {}) {
          listCalls.push({ workflow: name, status: args.status, perPage: args.perPage, page: args.page })
          const matching = all.filter((r) => args.status == null || r.status === args.status)
          const paginate = typeof args.perPage === 'number' && typeof args.page === 'number'
          const page = paginate
            ? matching.slice(args.page! * (args.perPage as number), (args.page! + 1) * (args.perPage as number))
            : matching
          return { runs: page.map((r) => ({ ...r, workflowName: name })), total: matching.length }
        },
        async createRun({ runId }: { runId: string }) {
          return {
            async restart() {
              restarted.push({ workflow: name, runId })
              if (opts.failRestart?.has(runId)) throw new Error(`restart of ${runId} failed`)
              return { status: 'success' }
            },
          }
        },
      }
    },
  }
  return { mastra, restarted, listCalls }
}
