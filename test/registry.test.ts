import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import { RekindleNotStartedError, registeredWorkflows, rekindleWorkflow } from '../src/index.js'
import { resetRegistry } from '../src/registry.js'
import { dropSchema, freshSchema, makePool, makeRekindle, ownerOf } from './helpers.js'

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

describe('rekindleWorkflow (one line per workflow)', () => {
  it('registers the workflow at import time, before any instance exists', () => {
    rekindleWorkflow('reportRun')
    rekindleWorkflow('importRun')
    expect(registeredWorkflows()).toEqual(['reportRun', 'importRun'])
  })

  it('guard() fails clearly if no instance has been created', async () => {
    const ownership = rekindleWorkflow('reportRun')
    await expect(ownership.guard('r-1')).rejects.toBeInstanceOf(RekindleNotStartedError)
  })

  it('guard() and clear() go through the default instance once one exists', async () => {
    const ownership = rekindleWorkflow('reportRun')
    const rk = makeRekindle(pool, schema, { setAsDefault: true })
    await rk.heartbeatOnce()

    await ownership.guard('r-2')
    expect((await ownerOf(pool, schema, 'r-2'))?.owner_instance_id).toBe(rk.instanceId)

    await ownership.clear('r-2')
    const row = await ownerOf(pool, schema, 'r-2')
    expect(row?.done_at).toBeInstanceOf(Date)
    expect(row?.owner_instance_id).toBeNull()
  })
})
