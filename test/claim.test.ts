import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { ageHeartbeat, dropSchema, freshSchema, makePool, makeRekindle, ownerOf } from './helpers.js'

const pool = makePool()
let schema: string

beforeAll(async () => {
  schema = await freshSchema(pool)
  await makeRekindle(pool, schema).migrate()
})
afterAll(async () => {
  await dropSchema(pool, schema)
  await pool.end()
})

const longAgo = () => new Date(Date.now() - 3_600_000)

describe('claimAbandoned', () => {
  it('lets exactly one of several racing instances claim an unowned run', async () => {
    const instances = Array.from({ length: 5 }, () => makeRekindle(pool, schema))
    await Promise.all(instances.map((i) => i.heartbeatOnce()))

    const results = await Promise.all(instances.map((i) => i.claimAbandoned('wf', 'run-race', longAgo())))

    expect(results.filter(Boolean)).toHaveLength(1)
    const winner = instances[results.indexOf(true)]
    expect((await ownerOf(pool, schema, 'run-race'))?.owner_instance_id).toBe(winner.instanceId)
  })

  it('does not take a run whose owner is still heartbeating', async () => {
    const owner = makeRekindle(pool, schema)
    const other = makeRekindle(pool, schema)
    await owner.heartbeatOnce()
    await other.heartbeatOnce()
    await owner.guard('wf', 'run-live')

    expect(await other.claimAbandoned('wf', 'run-live', longAgo())).toBe(false)
    expect((await ownerOf(pool, schema, 'run-live'))?.owner_instance_id).toBe(owner.instanceId)
  })

  it('takes a run whose owner stopped heartbeating, and stamps restarted_at and attempts', async () => {
    const dead = makeRekindle(pool, schema)
    const alive = makeRekindle(pool, schema)
    await dead.heartbeatOnce()
    await alive.heartbeatOnce()
    await dead.guard('wf', 'run-dead')
    await ageHeartbeat(pool, schema, dead.instanceId, 60)

    expect(await alive.claimAbandoned('wf', 'run-dead', longAgo())).toBe(true)
    const row = await ownerOf(pool, schema, 'run-dead')
    expect(row?.owner_instance_id).toBe(alive.instanceId)
    expect(row?.restarted_at).toBeInstanceOf(Date)
    expect(row?.attempts).toBe(1)
  })

  it('does not take an ownerless run that Mastra updated within the stale window', async () => {
    const a = makeRekindle(pool, schema)
    await a.heartbeatOnce()

    expect(await a.claimAbandoned('wf', 'run-fresh', new Date())).toBe(false)
    expect(await ownerOf(pool, schema, 'run-fresh')).toBeUndefined()

    expect(await a.claimAbandoned('wf', 'run-fresh', longAgo())).toBe(true)
  })

  it('does not claim when its own heartbeat is stale', async () => {
    const dead = makeRekindle(pool, schema)
    const stale = makeRekindle(pool, schema)
    await dead.heartbeatOnce()
    await stale.heartbeatOnce()
    await dead.guard('wf', 'run-self-stale')
    await ageHeartbeat(pool, schema, dead.instanceId, 60)
    await ageHeartbeat(pool, schema, stale.instanceId, 60)

    expect(await stale.claimAbandoned('wf', 'run-self-stale', longAgo())).toBe(false)
    expect(await stale.claimAbandoned('wf', 'run-self-stale-new', longAgo())).toBe(false)
  })

  it('never claims a run that was cleared as done', async () => {
    const a = makeRekindle(pool, schema)
    const b = makeRekindle(pool, schema)
    await a.heartbeatOnce()
    await b.heartbeatOnce()
    await a.guard('wf', 'run-done')
    await a.clear('wf', 'run-done')
    await ageHeartbeat(pool, schema, a.instanceId, 60)

    expect(await b.claimAbandoned('wf', 'run-done', longAgo())).toBe(false)
    expect((await ownerOf(pool, schema, 'run-done'))?.done_at).toBeInstanceOf(Date)
  })

  it('stops claiming a run after maxAttempts', async () => {
    const a = makeRekindle(pool, schema, { maxAttempts: 2 })
    await a.heartbeatOnce()

    expect(await a.claimAbandoned('wf', 'run-cap', longAgo())).toBe(true)
    await a.release('wf', 'run-cap')
    expect(await a.claimAbandoned('wf', 'run-cap', longAgo())).toBe(true)
    await a.release('wf', 'run-cap')
    expect(await a.claimAbandoned('wf', 'run-cap', longAgo())).toBe(false)
  })

  it('keeps the same runId in two workflows apart', async () => {
    const a = makeRekindle(pool, schema)
    const b = makeRekindle(pool, schema)
    await a.heartbeatOnce()
    await b.heartbeatOnce()
    await a.guard('wfA', 'run-shared')

    expect(await b.claimAbandoned('wfB', 'run-shared', longAgo())).toBe(true)
    expect((await ownerOf(pool, schema, 'run-shared', 'wfA'))?.owner_instance_id).toBe(a.instanceId)
    expect((await ownerOf(pool, schema, 'run-shared', 'wfB'))?.owner_instance_id).toBe(b.instanceId)
  })

  it('release() makes a claimed run claimable again', async () => {
    const a = makeRekindle(pool, schema)
    const b = makeRekindle(pool, schema)
    await a.heartbeatOnce()
    await b.heartbeatOnce()
    expect(await a.claimAbandoned('wf', 'run-release', longAgo())).toBe(true)
    expect(await b.claimAbandoned('wf', 'run-release', longAgo())).toBe(false)

    await a.release('wf', 'run-release')

    expect(await b.claimAbandoned('wf', 'run-release', longAgo())).toBe(true)
  })
})
