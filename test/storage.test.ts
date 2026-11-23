import { randomUUID } from 'node:crypto'
import { Mastra } from '@mastra/core/mastra'
import { PostgresStore } from '@mastra/pg'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { createRekindle } from '../src/index.js'
import { dropSchema, freshSchema, makePool } from './helpers.js'

const pool = makePool()
let schema: string
let mastra: Mastra

beforeAll(async () => {
  schema = await freshSchema(pool)
  // One store, initialised up front: Mastra creates its tables in the background on construction,
  // and two stores racing on one schema (or a drop mid-init) fails inside Mastra, not rekindle.
  const storage = new PostgresStore({ id: 'test', pool, schemaName: schema })
  await storage.init()
  mastra = new Mastra({ storage, logger: false })
})
afterAll(async () => {
  await dropSchema(pool, schema)
  await pool.end()
})

describe("uses Mastra's storage", () => {
  it("takes the pool and schema from Mastra's PostgresStore", async () => {
    const rekindle = createRekindle({ mastra, instanceId: `inst-${randomUUID().slice(0, 8)}`, setAsDefault: false })

    await rekindle.migrate()
    await rekindle.heartbeatOnce()

    const { rows } = await pool.query(`SELECT instance_id FROM ${schema}.rekindle_instances`)
    expect(rows.map((r) => r.instance_id)).toContain(rekindle.instanceId)
  })

  it('an explicit schema wins over the store', async () => {
    const other = await freshSchema(pool)
    try {
      const rekindle = createRekindle({ mastra, schema: other, setAsDefault: false })

      await rekindle.migrate()
      await rekindle.heartbeatOnce()

      const { rows } = await pool.query(`SELECT 1 FROM ${other}.rekindle_instances`)
      expect(rows).toHaveLength(1)
    } finally {
      await dropSchema(pool, other)
    }
  })

  it('says what is wrong when Mastra has no Postgres storage and no pool is given', () => {
    const noStorage = new Mastra({ logger: false })
    expect(() => createRekindle({ mastra: noStorage, setAsDefault: false })).toThrow(/PostgresStore/)
  })
})
