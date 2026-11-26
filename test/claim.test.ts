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
    await Promise.all(instances.map((i) => i.heartbeat()))

    const results = await Promise.all(instances.map((i) => i.store.claim('wf', 'run-race', longAgo())))

    expect(results.filter(Boolean)).toHaveLength(1)
    const winner = instances[results.indexOf(true)]
    expect((await ownerOf(pool, schema, 'run-race'))?.owner_instance_id).toBe(winner.instanceId)
  })

  it('does not take a run whose owner is still heartbeating', async () => {
    const owner = makeRekindle(pool, schema)
    const other = makeRekindle(pool, schema)
    await owner.heartbeat()
    await other.heartbeat()
    await owner.guard('wf', 'run-live')

    expect(await other.store.claim('wf', 'run-live', longAgo())).toBe(false)
    expect((await ownerOf(pool, schema, 'run-live'))?.owner_instance_id).toBe(owner.instanceId)
  })

  it('takes a run whose owner stopped heartbeating, and stamps restarted_at and attempts', async () => {
    const dead = makeRekindle(pool, schema)
    const alive = makeRekindle(pool, schema)
    await dead.heartbeat()
    await alive.heartbeat()
    await dead.guard('wf', 'run-dead')
    await ageHeartbeat(pool, schema, dead.instanceId, 60)

    expect(await alive.store.claim('wf', 'run-dead', longAgo())).toBe(true)
    const row = await ownerOf(pool, schema, 'run-dead')
    expect(row?.owner_instance_id).toBe(alive.instanceId)
    expect(row?.restarted_at).toBeInstanceOf(Date)
    expect(row?.attempts).toBe(1)
  })

  it('does not take an ownerless run that Mastra updated within the stale window', async () => {
    const a = makeRekindle(pool, schema)
    await a.heartbeat()

    expect(await a.store.claim('wf', 'run-fresh', new Date())).toBe(false)
    expect(await ownerOf(pool, schema, 'run-fresh')).toBeUndefined()

    expect(await a.store.claim('wf', 'run-fresh', longAgo())).toBe(true)
  })

  it('does not claim when its own heartbeat is stale', async () => {
    const dead = makeRekindle(pool, schema)
    const stale = makeRekindle(pool, schema)
    await dead.heartbeat()
    await stale.heartbeat()
    await dead.guard('wf', 'run-self-stale')
    await ageHeartbeat(pool, schema, dead.instanceId, 60)
    await ageHeartbeat(pool, schema, stale.instanceId, 60)

    expect(await stale.store.claim('wf', 'run-self-stale', longAgo())).toBe(false)
    expect(await stale.store.claim('wf', 'run-self-stale-new', longAgo())).toBe(false)
  })

  it('never claims a run that was cleared as done', async () => {
    const a = makeRekindle(pool, schema)
    const b = makeRekindle(pool, schema)
    await a.heartbeat()
    await b.heartbeat()
    await a.guard('wf', 'run-done')
    await a.clear('wf', 'run-done')
    await ageHeartbeat(pool, schema, a.instanceId, 60)

    expect(await b.store.claim('wf', 'run-done', longAgo())).toBe(false)
    expect((await ownerOf(pool, schema, 'run-done'))?.done_at).toBeInstanceOf(Date)
  })

  it('stops claiming a run after maxAttempts', async () => {
    const a = makeRekindle(pool, schema, { maxAttempts: 2 })
    await a.heartbeat()

    expect(await a.store.claim('wf', 'run-cap', longAgo())).toBe(true)
    await a.store.release('wf', 'run-cap')
    expect(await a.store.claim('wf', 'run-cap', longAgo())).toBe(true)
    await a.store.release('wf', 'run-cap')
    expect(await a.store.claim('wf', 'run-cap', longAgo())).toBe(false)
  })

  it('keeps the same runId in two workflows apart', async () => {
    const a = makeRekindle(pool, schema)
    const b = makeRekindle(pool, schema)
    await a.heartbeat()
    await b.heartbeat()
    await a.guard('wfA', 'run-shared')

    expect(await b.store.claim('wfB', 'run-shared', longAgo())).toBe(true)
    expect((await ownerOf(pool, schema, 'run-shared', 'wfA'))?.owner_instance_id).toBe(a.instanceId)
    expect((await ownerOf(pool, schema, 'run-shared', 'wfB'))?.owner_instance_id).toBe(b.instanceId)
  })

  it('release() makes a claimed run claimable again', async () => {
    const a = makeRekindle(pool, schema)
    const b = makeRekindle(pool, schema)
    await a.heartbeat()
    await b.heartbeat()
    expect(await a.store.claim('wf', 'run-release', longAgo())).toBe(true)
    expect(await b.store.claim('wf', 'run-release', longAgo())).toBe(false)

    await a.store.release('wf', 'run-release')

    expect(await b.store.claim('wf', 'run-release', longAgo())).toBe(true)
  })
})
