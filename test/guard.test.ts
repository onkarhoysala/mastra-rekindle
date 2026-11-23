import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { OwnershipLostError, RekindleNotStartedError } from '../src/index.js'
import { ageHeartbeat, dropSchema, freshSchema, makePool, makeRekindle, ownerOf, sleep } from './helpers.js'

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

describe('guard (the per-step fence)', () => {
  it('takes an unowned run', async () => {
    const a = makeRekindle(pool, schema)
    await a.heartbeatOnce()
    await a.guard('wf', 'g-unowned')
    expect((await ownerOf(pool, schema, 'g-unowned'))?.owner_instance_id).toBe(a.instanceId)
  })

  it('passes on every step of a run it already owns', async () => {
    const a = makeRekindle(pool, schema)
    await a.heartbeatOnce()
    await a.guard('wf', 'g-mine')
    await expect(a.guard('wf', 'g-mine')).resolves.toBeUndefined()
    await expect(a.guard('wf', 'g-mine')).resolves.toBeUndefined()
  })

  it('aborts with OwnershipLostError when a live instance owns the run (the zombie case)', async () => {
    const rescuer = makeRekindle(pool, schema)
    const zombie = makeRekindle(pool, schema)
    await rescuer.heartbeatOnce()
    await zombie.heartbeatOnce()
    // The rescuer has taken over; the stalled original wakes up and reaches its next step.
    await rescuer.guard('wf', 'g-zombie')
    // Keep both visibly alive for the whole retry window.
    const beat = setInterval(() => void Promise.all([rescuer.heartbeatOnce(), zombie.heartbeatOnce()]), 200)
    try {
      await expect(zombie.guard('wf', 'g-zombie')).rejects.toBeInstanceOf(OwnershipLostError)
    } finally {
      clearInterval(beat)
    }
    expect((await ownerOf(pool, schema, 'g-zombie'))?.owner_instance_id).toBe(rescuer.instanceId)
  })

  it('takes over from a dead owner', async () => {
    const dead = makeRekindle(pool, schema)
    const next = makeRekindle(pool, schema)
    await dead.heartbeatOnce()
    await next.heartbeatOnce()
    await dead.guard('wf', 'g-dead')
    await ageHeartbeat(pool, schema, dead.instanceId, 60)

    await next.guard('wf', 'g-dead')

    expect((await ownerOf(pool, schema, 'g-dead'))?.owner_instance_id).toBe(next.instanceId)
  })

  it('waits out the stale window when the owner dies just before the guard runs', async () => {
    const dying = makeRekindle(pool, schema)
    const next = makeRekindle(pool, schema)
    await dying.heartbeatOnce()
    await next.heartbeatOnce()
    await dying.guard('wf', 'g-dying')
    // Fresh heartbeat, then silence: it goes stale ~2s from now, inside the guard's retry window.
    const beat = setInterval(() => void next.heartbeatOnce(), 200)
    const started = Date.now()
    try {
      await next.guard('wf', 'g-dying')
    } finally {
      clearInterval(beat)
    }
    expect(Date.now() - started).toBeGreaterThanOrEqual(1500)
    expect((await ownerOf(pool, schema, 'g-dying'))?.owner_instance_id).toBe(next.instanceId)
  })

  it('fails clearly when this instance never heartbeats within the guard window', async () => {
    const a = makeRekindle(pool, schema, { guardWindowMs: 300 })
    const started = Date.now()
    await expect(a.guard('wf', 'g-no-beat')).rejects.toBeInstanceOf(RekindleNotStartedError)
    expect(Date.now() - started).toBeGreaterThanOrEqual(250)
  })

  it('waits for start() when a run reaches its guard before rekindle has started', async () => {
    const a = makeRekindle(pool, schema, { workflows: [] })
    const guarded = a.guard('wf', 'g-early')
    await sleep(300) // eg the app boots rekindle without awaiting it, and a run lands first
    await a.start({ getWorkflow: () => { throw new Error('unused') } })
    try {
      await expect(guarded).resolves.toBeUndefined()
      expect((await ownerOf(pool, schema, 'g-early'))?.owner_instance_id).toBe(a.instanceId)
    } finally {
      await a.stop()
    }
  })

  it('fails at once, saying so, on a run already marked done', async () => {
    const a = makeRekindle(pool, schema)
    await a.heartbeatOnce()
    await a.guard('wf', 'g-done')
    await a.clear('wf', 'g-done')

    const started = Date.now()
    const err = await a.guard('wf', 'g-done').catch((e) => e)
    expect(err).toBeInstanceOf(OwnershipLostError)
    expect(err.reason).toBe('done')
    expect(Date.now() - started).toBeLessThan(1_000)
  })

  it('fails at once, saying so, when this instance is stopped and its heartbeat is stale', async () => {
    const a = makeRekindle(pool, schema)
    await a.heartbeatOnce()
    await a.guard('wf', 'g-self-stale')
    await ageHeartbeat(pool, schema, a.instanceId, 60)

    const started = Date.now()
    const err = await a.guard('wf', 'g-self-stale').catch((e) => e)
    expect(err).toBeInstanceOf(OwnershipLostError)
    expect(err.reason).toBe('not-alive')
    expect(Date.now() - started).toBeLessThan(1_000)
  })

  it('heartbeats and carries on when a running instance finds its own heartbeat stale and nobody took over', async () => {
    const a = makeRekindle(pool, schema, { workflows: [] })
    await a.start({ getWorkflow: () => { throw new Error('unused') } })
    try {
      await a.guard('wf', 'g-stall')
      await ageHeartbeat(pool, schema, a.instanceId, 60) // eg the event loop stalled

      const started = Date.now()
      await expect(a.guard('wf', 'g-stall')).resolves.toBeUndefined()
      expect(Date.now() - started).toBeLessThan(1_000)
      expect((await ownerOf(pool, schema, 'g-stall'))?.owner_instance_id).toBe(a.instanceId)
    } finally {
      await a.stop()
    }
  })
})
