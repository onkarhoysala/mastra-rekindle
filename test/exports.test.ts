import { describe, expect, it } from 'vitest'
import * as rekindle from '../src/index.js'

describe('public API', () => {
  it('exports exactly these runtime values', () => {
    expect(Object.keys(rekindle).sort()).toEqual([
      'OwnershipLostError',
      'RekindleNotStartedError',
      'consoleLogger',
      'createRekindle',
      'migrationSql',
      'registeredWorkflows',
      'rekindleWorkflow',
      'setDefaultProvider',
    ])
  })
})
