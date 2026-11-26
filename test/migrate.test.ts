import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { createRekindle, migrationSql } from '../src/index.js'
import { dropSchema, freshSchema, makePool, makeRekindle, noWorkflowsMastra } from './helpers.js'

const pool = makePool()
let schema: string

beforeAll(async () => {
  schema = await freshSchema(pool)
})
afterAll(async () => {
  await dropSchema(pool, schema)
  await pool.end()
})

describe('migrate', () => {
  it('creates the two rekindle tables in the given schema', async () => {
    await makeRekindle(pool, schema).migrate()
    const { rows } = await pool.query(
      `SELECT table_name FROM information_schema.tables WHERE table_schema = $1 ORDER BY 1`,
      [schema]
    )
    expect(rows.map((r) => r.table_name)).toEqual(['rekindle_instances', 'rekindle_run_owners'])
  })

  it('is safe when several instances migrate a fresh schema at the same time', async () => {
    const fresh = `rk_concurrent_${Date.now()}`
    const pools = Array.from({ length: 8 }, () => makePool())
    try {
      const results = await Promise.allSettled(pools.map((p) => makeRekindle(p, fresh).migrate()))
      expect(results.filter((r) => r.status === 'rejected')).toEqual([])
    } finally {
      await Promise.all(pools.map((p) => p.end()))
      await dropSchema(pool, fresh)
    }
  })

  it('is safe to run twice', async () => {
    const rk = makeRekindle(pool, schema)
    await rk.migrate()
    await expect(rk.migrate()).resolves.toBeUndefined()
  })

  it('keeps a mixed-case schema name as is, the way Mastra does', async () => {
    const mixed = `Rk_Mixed_${Date.now()}`
    try {
      await makeRekindle(pool, mixed).migrate()
      const { rows } = await pool.query(`SELECT 1 FROM information_schema.tables WHERE table_schema = $1`, [mixed])
      expect(rows.length).toBe(2)
    } finally {
      await dropSchema(pool, mixed)
    }
  })

  it('refuses a schema name that is not a plain identifier', () => {
    expect(() => createRekindle({ mastra: noWorkflowsMastra, pool, schema: 'mastra; DROP TABLE x' })).toThrow(/schema/i)
  })

  it('migrationSql() needs the schema spelled out', () => {
    expect(() => (migrationSql as (s?: string) => string)()).toThrow(/schema/i)
  })
})
